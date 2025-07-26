ThisBuild / scalaVersion := "2.12.20"

ThisBuild / organization := "me.kosik.interwalled"
ThisBuild / version := "0.1.0-SNAPSHOT"


// Deduplication (assemblyMergeStrategy) for sbt-assembly
val sparkJobAssemblyMergeStrategy: String => sbtassembly.MergeStrategy = {
  // Do not erase log4j files
  case "plugin.properties" | "log4j.properties" =>
    MergeStrategy.concat

  // Otherwise it will fail with "Failed to find the data source: parquet."
  case PathList("META-INF", "services",  _*) =>
    MergeStrategy.concat

  case PathList("META-INF", xs @ _*) =>
    MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

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
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions"),
    assembly / assemblyJarName := "interwalled-benchmark.jar",
    assembly / mainClass := Some("me.kosik.interwalled.benchmark.Main"),
    assembly / assemblyMergeStrategy := sparkJobAssemblyMergeStrategy
  )
  .dependsOn(ailist, domain, spark)

lazy val testDataGenerator = (project in file("test-data-generator"))
  .settings(
    name := "test-data-generator",
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions"),
    assembly / assemblyJarName := "interwalled-test-data-generator.jar",
    assembly / mainClass := Some("me.kosik.interwalled.test.data.generator.Main"),
    assembly / assemblyMergeStrategy := sparkJobAssemblyMergeStrategy
  )
  .dependsOn(domain, spark)

lazy val root = (project in file("."))
  .aggregate(domain, ailist, spark, benchmark, testDataGenerator)
  .settings(name := "interwalled")


ailist / libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

benchmark / Compile / run / mainClass := Some("me.kosik.interwalled.benchmark.Main")
benchmark / parallelExecution in Test := false
benchmark / libraryDependencies += "org.apache.spark"  %% "spark-core"          % SparkVersion
benchmark / libraryDependencies += "org.apache.spark"  %% "spark-sql"           % SparkVersion
benchmark / libraryDependencies += "com.holdenkarau"   %% "spark-testing-base"  % f"${SparkVersion}_${SparkTestingBaseVersion}" % "test"

spark / Test / parallelExecution := false
spark / libraryDependencies += "org.apache.spark"  %% "spark-core"          % SparkVersion                                  % "provided"
spark / libraryDependencies += "org.apache.spark"  %% "spark-sql"           % SparkVersion                                  % "provided"
spark / libraryDependencies += "com.holdenkarau"   %% "spark-testing-base"  % f"${SparkVersion}_${SparkTestingBaseVersion}" % "test"

testDataGenerator / Compile / run / mainClass := Some("me.kosik.interwalled.test.data.generator.Main")
testDataGenerator / libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
testDataGenerator / libraryDependencies += "org.apache.spark" %% "spark-sql"  % SparkVersion
