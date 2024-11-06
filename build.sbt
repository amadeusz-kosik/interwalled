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

lazy val root = (project in file("."))
  .aggregate(ailist)
  .dependsOn(ailist)
  .settings(
    name := "interwalled"
  )

ailist / libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % SparkVersion % "provided"

libraryDependencies += "com.holdenkarau"  %% "spark-testing-base" % f"${SparkVersion}_${SparkTestingBaseVersion}" % "test"