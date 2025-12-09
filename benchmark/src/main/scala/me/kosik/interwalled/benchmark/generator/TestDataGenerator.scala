package me.kosik.interwalled.benchmark.generator

import me.kosik.interwalled.benchmark.app.{ApplicationEnv, BenchmarkApp}
import me.kosik.interwalled.benchmark.generator.TestDataGenerator.generate
import me.kosik.interwalled.benchmark.test.data.datasets.TestCases
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory


class TestDataGenerator(args: Array[String], env: ApplicationEnv) extends BenchmarkApp {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private val PARTITIONS_PER_DATASET: Int      = 16

  override def run(): Unit = {
    generate(PARTITIONS_PER_DATASET, env)
  }
}

object TestDataGenerator {
  private lazy val logger = LoggerFactory.getLogger(getClass)


  def generate(partitions: Int, env: ApplicationEnv): Unit = {
    TestCases.values.values foreach { testCaseCallback =>
      import env.sparkSession.implicits._
      implicit val spark: SparkSession = env.sparkSession

      logger.info(s"Generating ${testCaseCallback.testCaseName} data.")

      val testData = testCaseCallback.generate()
      val writePath = s"${env.dataDirectory}/test-data/${testCaseCallback.testCaseName}.parquet"

      testData
        .repartition(partitions)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }
}