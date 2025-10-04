package me.kosik.interwalled.benchmark.app

import me.kosik.interwalled.benchmark.test.data.datasets.TestCases
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory


class TestDataGenerator(env: MainEnv) extends BenchmarkApp {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private val PARTITIONS_PER_DATASET: Int      = 16

  override def run(): Unit = {
    implicit val spark: SparkSession = env.sparkSession

    TestCases.values.values foreach { testCaseCallback =>
      logger.info(s"Generating ${testCaseCallback.testCaseName} data.")
      val testData = testCaseCallback.generate()
      val writePath = s"${env.dataDirectory}/test-data/${testCaseCallback.testCaseName}.parquet"

      testData
        .repartition(PARTITIONS_PER_DATASET)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(writePath)
    }
  }
}
