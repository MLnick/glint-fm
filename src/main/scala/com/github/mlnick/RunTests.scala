package com.github.mlnick

import com.github.mlnick.glintfm.GlintFM

object RunTests extends App {

  val inputPath = "rcv1_train.binary"
  val format = "libsvm"
  val configPath = "src/main/resources/glintfm.conf"
  GlintFM.runTest(inputPath, configPath, format, parts = -1, models = 2, msgSize = 50000, timeout = 300,
    fitIntercept = true, fitLinear = true, k = 4, numIterations = 10, runML = false, runMLLIB = true, runGlint = true)
}
