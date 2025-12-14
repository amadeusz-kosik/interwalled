package me.kosik.interwalled.benchmark.generator

import me.kosik.interwalled.benchmark.app.{ApplicationEnv, BenchmarkApp}
import me.kosik.interwalled.benchmark.test.data.datasets.{TestCase, TestCases}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory


class TestDataGenerator(args: Array[String], env: ApplicationEnv) extends BenchmarkApp {
  import TestDataGenerator.{generate, generateResults}

  override def run(): Unit = args.headOption match {
    case Some("benchmark-data") =>
      generate("benchmark-data", TestCases.benchmarkData, 16, env)

    case Some("unit-test-data") =>
      generate("unit-test-data", TestCases.unitTestData, 1, env)
      generateResults("unit-test-data", env)

    case Some(anyOther) =>
      System.err.println(s"Unknown type of data to generate: $anyOther.")
      System.exit(1)

    case None =>
      System.err.println("Type of data to generate not provided.")
      System.exit(1)
  }
}

object TestDataGenerator {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def generate(path: String, testCases: Iterable[TestCase], partitions: Int, env: ApplicationEnv): Unit = {
    testCases foreach { testCaseCallback =>
      implicit val spark: SparkSession = env.sparkSession

      logger.info(s"Generating ${testCaseCallback.testCaseName} data.")

      val generatedData = testCaseCallback.generate()
      val writePath = s"${env.dataDirectory}/$path/${testCaseCallback.testCaseName}.parquet"

      generatedData
        .repartition(partitions)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }

  def generateResults(path: String, env: ApplicationEnv): Unit = {
    TestCases.unitResults foreach { testCaseResultsCallback =>
      implicit val spark: SparkSession = env.sparkSession

      logger.info(s"Generating ${testCaseResultsCallback.testCaseName} data results.")

      val generatedData = testCaseResultsCallback.generate()
      val writePath = s"${env.dataDirectory}/$path-results/${testCaseResultsCallback.testCaseName}.parquet"

      generatedData
        .write
        .mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }
}