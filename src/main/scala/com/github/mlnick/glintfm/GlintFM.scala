package com.github.mlnick.glintfm

import java.io.File
import java.util.Date

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.stats.distributions.Rand
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.LazyLogging
import glint.Client
import glint.models.client.granular.{GranularBigMatrix, GranularBigVector}

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, Vectors}
import org.apache.spark.mllib.regression._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.collection.glintfm.OHashMap


case class FMResults(model: FMModel, auc: Double, time: Double)
case class GlintFMResults(auc: Double, time: Double)
case class MLResults(iter: Int, model: org.apache.spark.ml.classification.LogisticRegressionModel, auc: Double, time: Double)

case class PushStats(time: Double, size: Long)
case class PullStats(time: Double, size: Long)
case class ComputeStats(time: Double, size: Long)
case class IterStats(push: PushStats, pull: PullStats, grad: ComputeStats, time: Double)

object GlintFM extends LazyLogging {

  def runTest(
    inputPath: String,
    configPath: String,
    format: String = "parquet",
    parts: Int = -1,
    models: Int = -1,
    msgSize: Int = 100000,
    fitIntercept: Boolean = true, fitLinear: Boolean = true, k: Int = 2,
    interceptRegParam: Double = 0.0, wRegParam: Double = 0.1, vRegParam: Double = 0.1, initStd: Double = 0.1,
    mlRegParam: Double = 0.1,
    numIterations: Int = 10,
    mlStepSize: Double = 1.0,
    glintStepSize: Double = 1.0,
    treeDepth: Int = 2,
    timeout: Int = 30,
    runML: Boolean = true,
    runMLLIB: Boolean = true,
    runGlint: Boolean = true) = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("glint-fm")
      .getOrCreate()

    val raw = spark.read.format(format).load(inputPath).select(col("label").cast(DoubleType), col("features"))
    val df = if (parts < 0) {
      raw
    } else {
      raw.repartition(parts)
    }
    df.cache()
    val splits = df.randomSplit(Array(0.8, 0.2), seed = 42)
    val (train, test) = (splits(0), splits(1))
    // map to -1/1 because spark-libFM uses this form
    val mllibTrain = train.rdd.map { case Row(l: Double, v: org.apache.spark.ml.linalg.Vector) =>
      LabeledPoint(if (l <= 0) -1.0 else 1.0, Vectors.fromML(v))
    }
    mllibTrain.cache()
    val mllibTest = test.rdd.map { case Row(l: Double, v: org.apache.spark.ml.linalg.Vector) =>
      LabeledPoint(if (l <= 0) -1.0 else 1.0, Vectors.fromML(v))
    }
    mllibTest.cache()
    val n = df.count()
    val dim = mllibTrain.first().features.size

    val mlResults = if (runML) {
      logger.warn(s"Starting ML LoR test run at ${new Date().toString}.")
      // ==== Spark ML LR
      val start = System.currentTimeMillis()
      val lr = new LogisticRegression()
        .setRegParam(mlRegParam)
        .setMaxIter(numIterations)
        .setFitIntercept(fitIntercept)
        .setStandardization(false)
      val model = lr.fit(train.select(when(train("label") <= 0, 0.0).otherwise(1.0).alias("label"), train("features")))
      val elapsed = (System.currentTimeMillis() - start) / 1000.0
      val eval = new BinaryClassificationEvaluator()
      val auc = eval.evaluate(model.transform(test.select(when(test("label") <= 0, 0.0).otherwise(1.0).alias("label"), test("features"))))

      logger.warn(s"Completed ML LoR test run at ${new Date().toString}.")
      Some(MLResults(numIterations, model, auc, elapsed))
    } else {
      None
    }

    val mllibResults = if (runMLLIB) {
      logger.warn(s"Starting MLlib FM test run at ${new Date().toString}.")
      // ==== Spark MLlib GradientDescent FM
      val mstart = System.currentTimeMillis()
      val model = FMWithSGD.train(mllibTrain, task = 1, numIterations = numIterations,
        stepSize = mlStepSize, miniBatchFraction = 1.0,
        dim = (fitIntercept, fitLinear, k), regParam = (interceptRegParam, wRegParam, vRegParam), initStd = initStd, treeDepth = treeDepth)
      val elapsed = (System.currentTimeMillis() - mstart) / 1000.0
      val scores = model.predict(mllibTest.map(_.features)).zip(mllibTest.map(_.label))
      val auc = new BinaryClassificationMetrics(scores).areaUnderROC()
      logger.warn(s"MLlib FM predictions: ${scores.take(20).mkString(",")}")
      logger.warn(s"Completed MLlib FM test run at ${new Date().toString}.")
      Some(FMResults(model, auc, elapsed))
    } else {
      None
    }

    val glintResults = if (runGlint) {
      logger.warn(s"Starting Glint FM test run at ${new Date().toString}.")
      // ==== Glint FM
      val config = ConfigFactory.parseFile(new File(configPath))
      @transient val client = Client(config)
      @transient implicit val ec = ExecutionContext.Implicits.global
      val numParts = mllibTrain.getNumPartitions
      val min = Double.MinValue
      val max = Double.MaxValue

      // set up coefficients
      val wDim = if (fitLinear) {
        if (fitIntercept) dim + 1 else dim
      } else {
        if (fitIntercept) 1 else 0
      }
      val distW = if (wDim > 0) {
        Some(new GranularBigVector(client.vector[Double](wDim), msgSize))
      } else {
        None
      }
      val distV = if (k > 0) {
        Some(new GranularBigMatrix(client.matrix[Double](dim, k), msgSize))
      } else {
        None
      }

      val gstart = System.currentTimeMillis()
      val glintIterStats = mllibTrain.mapPartitions { iter =>
        implicit val ec = ExecutionContext.Implicits.global
        val partitionData = iter.toIterable
        // TODO shuffle data per partition?
        // TODO mini-batch SGD per partition?
        // pre-compute the local feature indices for this partition
        val localKeys = collection.mutable.HashSet[Long]()
        // add intercept to keyset if used
        if (fitIntercept) localKeys.add(dim)
        partitionData.foreach { case LabeledPoint(_, features) =>
          features.foreachActive { case (idx, _) => localKeys.add(idx.toLong) }
        }
        // feature indices to pull/push from servers
        val keys = localKeys.toArray.sorted
        // int keys for mapping local to global feature index in arrays
        val idx = keys.map(_.toInt)
        val localWDim = if (fitLinear) {
          // keys is already correct for whatever value of k0
          keys.length
        } else if (fitIntercept) {
          1
        } else {
          0
        }
        val wKeys = if (localWDim == 1) Array(0L) else keys
        // we need to ignore intercept to get rows of V
        val localVDim = if (fitIntercept) keys.length - 1 else keys.length
        logger.info(s"Local unique keys: ${keys.length}")
        // stat holders
        val iterStats = new Array[IterStats](numIterations)
        for (iter <- 1 to numIterations) {
          logger.info(s"Starting iteration $iter")
          val iterStart = System.currentTimeMillis()
          // pull coefficients from param server
          val (result, pullStats) = if (iter == 1) {
            // if 1st iteration we don't pull coefficients
            // init w to zeros
            val zeroW = BDV.zeros[Double](localWDim)
            // init V to N(0, initStd)
            val randV = BDM.rand(localVDim, k, Rand.gaussian(0.0, initStd))
            ((zeroW, randV), PullStats(0, 0))
          } else {
            val pullStart = System.currentTimeMillis()
            // pull relevant keys of w
            val pullW = distW.map { w =>
              w.pull(wKeys).map(values => BDV[Double](values))
            }.getOrElse {
              Future { BDV.zeros[Double](0) }
            }
            // pull relevant rows of V
            val rows = if (fitIntercept) keys.init else keys
            val pullV = distV.map(_.pull(rows).map { vectors =>
              // stack vectors to form the local V matrix
              BDV.horzcat[Double](vectors.map(_.toDenseVector): _*).t
            }).getOrElse { Future { BDM.zeros[Double](0, 0) } }
            val pulls = for {
              wr <- pullW
              vr <- pullV
            } yield (wr, vr)

            val result = Await.result(pulls, timeout seconds)
            val pullElapsed = (System.currentTimeMillis() - pullStart) / 1000.0
            logger.info(f"Iteration $iter - pull time $pullElapsed%2.4f sec; w size=$localWDim, V size=($localVDim,$k)")
            (result, PullStats(pullElapsed, (localWDim + localVDim * k) * 8))
          }
          val w = result._1
          val V = result._2

          // gradient computation
          val gradStart = System.currentTimeMillis()
          val agg = partitionData.foldLeft(new FMAggregator(1, localWDim, idx, fitIntercept, fitLinear, k, min, max)) {
            case (a, LabeledPoint(label, data)) =>
              a.add(data, label, w, V)
          }
          val count = agg.getNumExamples
          val loss = agg.getLossSum
          val gradElapsed = (System.currentTimeMillis() - gradStart) / 1000.0
          logger.info(f"Iteration $iter - gradient computation stats: elapsed=$gradElapsed%2.4f sec; pred=${agg._pelapsed}%2.4f sec, grad=${agg._gelapsed}%2.4f sec; loss=$loss; examples=$count")

          val scale = count.toDouble * numParts
          val gradW = agg.getGradW
          val gradV = agg.getGradV
          val step = glintStepSize / math.sqrt(iter)

          // compute updates
          val updateStart = System.currentTimeMillis()
          val updateW = new Array[Double](gradW.length)
          // update w
          if (fitIntercept) {
            updateW(localWDim - 1) = -step * (gradW(localWDim - 1) / scale + interceptRegParam * w(localWDim - 1))
          }
          if (fitLinear) {
            for (i <- 0 until localWDim - 2) {
              updateW(i) = -step * (gradW(i) / scale + wRegParam * w(i))
            }
          }
          // update V
          val rows = new Array[Long](localVDim * k)
          val cols = new Array[Int](localVDim * k)
          val values = new Array[Double](localVDim * k)

          var uk = 0
          var i = 0
          while (i < localVDim) {
            val idx = keys(i)
            var j = 0
            while (j <  k) {
              values(uk) = -step * (gradV(i, j) / scale + vRegParam * V(i, j))
              rows(uk) = idx
              cols(uk) = j
              uk += 1
              j += 1
            }
            i += 1
          }
          val updateElapsed = (System.currentTimeMillis() - updateStart) / 1000.0
          logger.info(f"Iteration $iter - compute update time $updateElapsed%2.4f sec")
          val gradStats = ComputeStats(gradElapsed + updateElapsed, count)

          val pushStart = System.currentTimeMillis()
          val pushes = for {
            pushW <- distW.map(_.push(wKeys, updateW)).getOrElse(Future { true } )
            pushV <- distV.map(_.push(rows, cols, values)).getOrElse(Future { true })
          } yield (pushW, pushV)
          Await.result(pushes, timeout seconds)
          val pushElapsed = (System.currentTimeMillis() - pushStart) / 1000.0
          logger.info(f"Iteration $iter - push time $pushElapsed%2.4f sec; w size=$localWDim, V size=($localVDim,$k)")
          val pushStats = PushStats(pushElapsed, (localWDim + localVDim * k) * 8)
          val iterElapsed = (System.currentTimeMillis() - iterStart) / 1000.0
          logger.info(f"Iteration $iter - total time $iterElapsed%2.4f sec")

          iterStats(iter - 1) = IterStats(pushStats, pullStats, gradStats, iterElapsed)
        }
        Iterator.single(iterStats)
      }.collect()

      val elapsed = (System.currentTimeMillis() - gstart) / 1000.0

      logger.warn(s"Glint FM elapsed training time: $elapsed")

      import spark.implicits._
      val stats = glintIterStats.flatMap { partStats =>
        partStats.map { s =>
          (s.push.size, s.pull.time, s.push.time, s.grad.time, s.time)
        }
      }.toSeq.toDF("size", "pull", "push", "comp", "total")
      stats.groupBy().avg().show()
      stats.columns.foreach { c =>
        val m = stats.stat.approxQuantile(c, Array(0.5, 0.75, 1.0), 0.001)
        logger.warn(f"Stats $c median=${m(0)}%4.2f; 75th=${m(1)}%4.2f; max=${m(1)}%4.2f")
      }

      // predict distributed
      val scores = mllibTest.mapPartitions { iter =>
        implicit val ec = ExecutionContext.Implicits.global
        val partitionData = iter.toIterable
        // pre-compute the local feature indices for this partition
        val localKeys = collection.mutable.HashSet[Long]()
        // add intercept to keyset if used
        if (fitIntercept) localKeys.add(dim)
        partitionData.foreach { case LabeledPoint(_, features) =>
          features.foreachActive { case (idx, _) => localKeys.add(idx.toLong) }
        }
        // feature indices to pull/push from servers
        val keys = localKeys.toArray.sorted
        // int keys for mapping local to global feature index in arrays
        val idx = keys.map(_.toInt)
        val localWDim = if (fitLinear) {
          // keys is already correct for whatever value of k0
          keys.length
        } else if (fitIntercept) {
          1
        } else {
          0
        }
        val wKeys = if (localWDim == 1) Array(0L) else keys
        val localVDim = if (fitIntercept) keys.length - 1 else keys.length

        val pullStart = System.currentTimeMillis()
        // pull relevant keys of w
        val pullW = distW.map { w =>
          w.pull(wKeys).map(values => BDV[Double](values))
        }.getOrElse {
          Future { BDV.zeros[Double](0) }
        }
        // pull relevant rows of V
        val rows = if (fitIntercept) keys.init else keys
        val pullV = distV.map(_.pull(rows).map { vectors =>
          // stack vectors to form the local V matrix
          BDV.horzcat[Double](vectors.map(_.toDenseVector): _*).t
        }).getOrElse { Future { BDM.zeros[Double](0, 0) } }
        val pulls = for {
          wr <- pullW
          vr <- pullV
        } yield (wr, vr)

        val result = Await.result(pulls, timeout seconds)
        val pullElapsed = (System.currentTimeMillis() - pullStart) / 1000.0
        logger.info(f"Model evaluation - pull time $pullElapsed%2.4f sec; w size=$localWDim, V size=($localVDim,$k)")
        val w = result._1
        val V = result._2
        val model = new FMAggregator(1, localWDim, idx, fitIntercept, fitLinear, k, min, max)
        partitionData.map { case (LabeledPoint(label, features)) =>
          (model.testPredict(features, w, V), label)
        }.toIterator
      }
      val auc = new BinaryClassificationMetrics(scores).areaUnderROC()
      logger.warn(s"Glint FM predictions: ${scores.take(20).mkString(",")}")
      logger.warn(s"Completed Glint FM test run at ${new Date().toString}.")

      // clean up distributed coefficients
      distV.foreach(_.destroy())
      distW.foreach(_.destroy())
      client.stop()

      Some(GlintFMResults(auc, elapsed))
    } else {
      None
    }

    // print results
    logger.warn(s"Completed test run at ${new Date().toString}.")
    logger.warn(s"Rows: $n, Cols: $dim")
    logger.warn(s"Num iterations=$numIterations, parts=$parts, dim=${(fitIntercept, fitLinear, k)}, regParam=${(interceptRegParam, wRegParam, vRegParam)}")
    mlResults.foreach { case MLResults(_, model, auc, elapsed) =>
      logger.warn(s"ML LoR results -- regParam=$mlRegParam")
      logger.warn(s"ML LoR model: w0 = ${model.intercept}; w = ${model.coefficients.toArray.take(10).mkString(",")}")
      logger.warn(s"ML LoR test AUC: $auc")
      logger.warn(s"ML LoR elapsed training time: $elapsed")
    }
    mllibResults.foreach { case FMResults(model, auc, elapsed) =>
      logger.warn(s"MLlib FM results -- stepSize=$mlStepSize")
      val w = model.weightVector.getOrElse(Vectors.zeros(0)).toArray.take(10).mkString(",")
      logger.warn(s"MLlib FM model: w0 = ${model.intercept}; w = $w; V = ${if (k > 0) model.factorMatrix else ""}")
      logger.warn(s"MLlib FM test AUC: $auc")
      logger.warn(s"MLlib FM elapsed training time: $elapsed")
    }
    glintResults.foreach { case GlintFMResults(auc, elapsed) =>
      logger.warn(s"Glint FM results -- stepSize=$glintStepSize, models=$models, msgSize=$msgSize, timeout=$timeout")
      logger.warn(s"Glint FM test AUC: $auc")
      logger.warn(s"Glint FM elapsed training time: $elapsed")
    }

    mllibTrain.unpersist()
    mllibTest.unpersist()
    df.unpersist()
  }

}

// Logic taken mostly from spark-libFM
private class FMAggregator(
  task: Int,  // 0 = regression, 1 = classification
  wDim: Int,
  idx: Array[Int],
  fitIntercept: Boolean,
  fitLinear: Boolean,
  k: Int,
  min: Double,
  max: Double) {

  private var examples = 0
  private var lossSum = 0.0

  private val activeDim = idx.length
  private val wGrad = new Array[Double](wDim)
  private val vGrad = BDM.zeros[Double](activeDim, k)

  private val idxMapping = {
    val m = new OHashMap[Int, Int](activeDim)
    var i = 0
    while (i < activeDim) {
      m.update(idx(i), i)
      i += 1
    }
    m
  }

  def getNumExamples = examples
  def getLossSum = lossSum
  def getGradW = wGrad
  def getGradV = vGrad

  def testPredict(data: Vector, weights: BDV[Double], V: BDM[Double]): Double = {

    var pred = if (fitIntercept) weights(weights.length - 1) else 0.0

    if (fitLinear) {
      data.foreachActive {
        case (i, v) =>
          pred += weights(idxMapping(i)) * v
      }
    }

    for (f <- 0 until k) {
      var sum = 0.0
      var sumSqr = 0.0
      data.foreachActive {
        case (i, v) =>
          val d = V(idxMapping(i), f) * v
          sum += d
          sumSqr += d * d
      }
      pred += (sum * sum - sumSqr) / 2
    }

    task match {
      case 0 =>
        Math.min(Math.max(pred, min), max)
      case 1 =>
        1.0 / (1.0 + Math.exp(-pred))
    }
  }

  private def predict(data: Vector, weights: BDV[Double], V: BDM[Double]): (Double, Array[Double]) = {

    var pred = if (fitIntercept) weights(weights.length - 1) else 0.0

    if (fitLinear) {
      data.foreachActive {
        case (i, v) =>
          pred += weights(idxMapping(i)) * v
      }
    }

    val sum = Array.fill(k)(0.0)
    for (f <- 0 until k) {
      var sumSqr = 0.0
      data.foreachActive {
        case (i, v) =>
          val d = V(idxMapping(i), f) * v
          sum(f) += d
          sumSqr += d * d
      }
      pred += (sum(f) * sum(f) - sumSqr) / 2
    }

    if (task == 0) {
      pred = Math.min(Math.max(pred, min), max)
    }
    (pred, sum)
  }

  private def updateGradientInPlace(
    data: Vector,
    label: Double,
    pred: Double,
    sum: Array[Double],
    V: BDM[Double]): Unit = {

    val mult = task match {
      case 0 =>
        pred - label
      case 1 =>
        -label * (1.0 - 1.0 / (1.0 + Math.exp(-label * pred)))
    }

    if (fitIntercept) {
      wGrad(wGrad.length - 1) += mult
    }
    if (fitLinear) {
      data.foreachActive { case (i, v) =>
        wGrad(idxMapping(i)) += v * mult
      }
    }

    data.foreachActive { case (i, v) =>
      val idx = idxMapping(i)
      for (f <- 0 until k) {
        val g = (sum(f) * v - V(idx, f) * v * v) * mult
        vGrad(idx, f) += g
      }
    }

  }

  var _pelapsed = 0.0
  var _gelapsed = 0.0

  def add(data: Vector, label: Double, w: BDV[Double], V: BDM[Double]): this.type = {

    val ps = System.currentTimeMillis()
    val (pred, sum) = predict(data, w, V)
    _pelapsed += (System.currentTimeMillis() - ps) / 1000.0

    val gs = System.currentTimeMillis()
    updateGradientInPlace(data, label, pred, sum, V)
    _gelapsed += (System.currentTimeMillis() - gs) / 1000.0

    val loss = task match {
      case 0 =>
        (pred - label) * (pred - label)
      case 1 =>
        1 - Math.signum(pred * label)
    }
    lossSum += loss
    examples += 1
    this
  }

}
