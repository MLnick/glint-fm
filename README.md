# Glint FM

## Factorization Machines on Spark and Glint

An implementation of distributed factorization machines on Spark using the [Glint parameter server](https://github.com/rjagerman/glint).

To build, run `sbt package`. You will need to have Glint and spark-libFM installed locally, since they are not available on Maven central.

You will need to clone the [glint repository](https://github.com/rjagerman/glint) and run `sbt publish-local` (master branch should be fine).

For [spark-libFM](https://github.com/zhengruifeng/spark-libFM) you will need to build it with Spark 2.0 support - check out my branch [here](https://github.com/MLnick/spark-libFM/tree/spark20) which was used for performance comparisons. 
Again, run `sbt publish-local` to install locally before running glint-fm.
