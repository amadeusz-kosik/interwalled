ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

// Libraries' versions
val SparkVersion = "3.5.1"



lazy val root = (project in file("."))
  .settings(
    name := "interwalled"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % SparkVersion % "provided"

libraryDependencies += "com.holdenkarau"  %% "spark-testing-base" % f"${SparkVersion}_1.5.2" % "test"