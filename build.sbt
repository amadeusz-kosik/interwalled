ThisBuild / scalaVersion := "2.12.20"

ThisBuild / organization := "me.kosik.interwalled"
ThisBuild / version := "0.1.0-SNAPSHOT"


val DefaultScalacOptions = Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions")

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
val SparkTestingBaseVersion = f"${SparkVersion}_2.0.1"

// Sequila has support for Spark up to 3.4.3
val SequilaSparkVersion = "3.4.1"
val SequilaSparkTestingBaseVersion = f"${SequilaSparkVersion}_1.4.4"


lazy val ailist = (project in file("ailist"))
  .settings(
    name := "ailist",
    javacOptions ++= Seq("-source", "17", "-target", "17")
  )

lazy val spark = (project in file("spark")) // FIXME: rename to interwalled
  .settings(name := "spark")
  .dependsOn(ailist)

lazy val benchmarkCommon = (project in file("benchmarks/common"))
  .settings(
    name := "benchmark-common",
    scalacOptions ++= DefaultScalacOptions
  )

lazy val benchmarkSequila = (project in file("benchmarks/sequila"))
  .settings(
    name := "benchmark-sequila",
    scalacOptions ++= DefaultScalacOptions,
    assembly / assemblyJarName := "benchmark-sequila.jar",
    assembly / mainClass := Some("me.kosik.interwalled.benchmark.sequila.Main"),
    assembly / assemblyMergeStrategy := sparkJobAssemblyMergeStrategy
  )
  .dependsOn(benchmarkCommon)

lazy val benchmarkInterwalled = (project in file("benchmarks/interwalled"))
  .settings(
    name := "benchmark-interwalled",
    scalacOptions ++= DefaultScalacOptions,
    assembly / assemblyJarName := "benchmark-interwalled.jar",
    assembly / mainClass := Some("me.kosik.interwalled.benchmark.interwalled.Main"),
    assembly / assemblyMergeStrategy := sparkJobAssemblyMergeStrategy
  )
  .dependsOn(benchmarkCommon, spark)

lazy val root = (project in file("."))
  .aggregate(ailist, spark, benchmarkSequila)
  .settings(name := "interwalled")

// Necessary for sequila

ailist / libraryDependencies += "com.github.sbt.junit" %  "jupiter-interface"   % "0.16.0"                  % Test

spark / Test / parallelExecution := false
spark / libraryDependencies += "org.apache.spark"  %% "spark-core"              % SparkVersion              % Provided
spark / libraryDependencies += "org.apache.spark"  %% "spark-sql"               % SparkVersion              % Provided
spark / libraryDependencies += "com.holdenkarau"   %% "spark-testing-base"      % SparkTestingBaseVersion   % Test

benchmarkInterwalled / Test / parallelExecution := false
benchmarkInterwalled / libraryDependencies += "org.apache.spark"      %% "spark-core"           % SparkVersion                    % Provided
benchmarkInterwalled / libraryDependencies += "org.apache.spark"      %% "spark-sql"            % SparkVersion                    % Provided
benchmarkInterwalled / libraryDependencies += "com.holdenkarau"       %% "spark-testing-base"   % SparkTestingBaseVersion         % Test

benchmarkSequila / Test / parallelExecution := false
benchmarkSequila / libraryDependencies += "org.apache.spark"          %% "spark-core"           % SequilaSparkVersion             % Provided
benchmarkSequila / libraryDependencies += "org.apache.spark"          %% "spark-sql"            % SequilaSparkVersion             % Provided
benchmarkSequila / libraryDependencies += "com.holdenkarau"           %% "spark-testing-base"   % SequilaSparkTestingBaseVersion  % Test
benchmarkSequila / libraryDependencies += "org.biodatageeks"          %% "sequila"              % "local"
