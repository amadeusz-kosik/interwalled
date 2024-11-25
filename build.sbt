ThisBuild / scalaVersion := "2.13.13"

ThisBuild / organization := "me.kosik.interwalled"
ThisBuild / version := "0.1.0-SNAPSHOT"

// Libraries' versions
val SparkVersion            = "3.5.3"
val SparkTestingBaseVersion = "2.0.1"


lazy val ailist = (project in file("ailist"))
  .settings(
    name := "ailist"
  )

lazy val testDataGenerator = (project in file("test-data-generator"))
  .settings(
    name := "test-data-generator"
  )

lazy val root = (project in file("."))
  .aggregate(ailist, testDataGenerator)
  .dependsOn(ailist)
  .settings(
    name := "interwalled"
  )

ailist / libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

testDataGenerator / mainClass in (Compile, run) := Some("me.kosik.interwalled.test.data.generator.Main")
testDataGenerator / libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4"
testDataGenerator / libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
testDataGenerator / libraryDependencies += "org.apache.spark" %% "spark-sql"  % SparkVersion

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % SparkVersion % "provided"

libraryDependencies += "com.holdenkarau"  %% "spark-testing-base" % f"${SparkVersion}_${SparkTestingBaseVersion}" % "test"