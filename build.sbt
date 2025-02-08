ThisBuild / scalaVersion := "2.12.20"

ThisBuild / organization := "me.kosik.interwalled"
ThisBuild / version := "0.1.0-SNAPSHOT"

// Libraries' versions
val SparkVersion            = "3.5.3"
val SparkTestingBaseVersion = "2.0.1"

lazy val domain = (project in file("domain"))
  .settings(name := "domain")

lazy val ailist = (project in file("ailist"))
  .settings(name := "ailist")
  .dependsOn(domain)

lazy val spark = (project in file("spark"))
  .settings(name := "spark")
  .dependsOn(ailist, domain)

lazy val benchmark = (project in file("benchmark"))
  .settings(
    name := "benchmark",
    assembly / assemblyJarName := "interwalled-benchmark.jar",
    assembly / mainClass := Some("me.kosik.interwalled.benchmark.Main")
  )
  .dependsOn(ailist, domain, spark)

lazy val testDataGenerator = (project in file("test-data-generator"))
  .settings(name := "test-data-generator")
  .dependsOn(domain)

lazy val root = (project in file("."))
  .aggregate(domain, ailist, spark, benchmark, testDataGenerator)
  .settings(name := "interwalled")


ailist / libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

benchmark / mainClass in (Compile, run) := Some("me.kosik.interwalled.benchmark.Main")
benchmark / parallelExecution in Test := false

benchmark / libraryDependencies += "org.apache.hadoop" % "hadoop-client"  % "3.3.4"
benchmark / libraryDependencies += "org.apache.spark"  %% "spark-core"    % SparkVersion
benchmark / libraryDependencies += "org.apache.spark"  %% "spark-sql"     % SparkVersion
benchmark / libraryDependencies += "org.apache.hadoop" %  "hadoop-client"       % "3.3.4"                                       % "test"
benchmark / libraryDependencies += "com.holdenkarau"   %% "spark-testing-base"  % f"${SparkVersion}_${SparkTestingBaseVersion}" % "test"

spark / parallelExecution in Test := false
spark / libraryDependencies += "org.apache.spark"  %% "spark-core"    % SparkVersion % "provided"
spark / libraryDependencies += "org.apache.spark"  %% "spark-sql"     % SparkVersion % "provided"
spark / libraryDependencies += "org.apache.hadoop" %  "hadoop-client"       % "3.3.4"                                       % "test"
spark / libraryDependencies += "com.holdenkarau"   %% "spark-testing-base"  % f"${SparkVersion}_${SparkTestingBaseVersion}" % "test"

testDataGenerator / mainClass in (Compile, run) := Some("me.kosik.interwalled.test.data.generator.Main")
testDataGenerator / libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4"
testDataGenerator / libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
testDataGenerator / libraryDependencies += "org.apache.spark" %% "spark-sql"  % SparkVersion
