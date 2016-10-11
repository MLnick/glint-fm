name := "glint-fm"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies += "com.mininglamp.ml" % "FM" % "1.0-SNAPSHOT"

libraryDependencies += "ch.ethz.inf.da" %% "glint" % "0.1-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided"

fork in run := false
javaOptions in run ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)
    