package me.kosik.interwalled.test.data.generator

import me.kosik.interwalled.test.data.generator.test.cases.{TestOneToAll, TestOneToOne}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

object Main extends App {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("TestDataGenerator")
    .master("local[4]")
    .getOrCreate()

  val testDataSize = 100_000_000

  val testCase = args.head match {
    case "one-to-one" =>
      TestOneToOne(testDataSize)

    case "one-to-all" =>
      TestOneToAll(testDataSize)

    case unknown =>
      logger.error(s"Unknown test case: $unknown.")
      sys.exit(1)
  }

  write(testCase.generateLHS, "in-lhs")
  write(testCase.generateRHS, "in-rhs")
  write(testCase.generateResult, "out-result")


  private def write[T](dataset: Dataset[T], datasetName: String): Unit = {
    val writePath = f"data/${testCase.testCaseName}/$datasetName.parquet"

    logger.info(s"Writing $datasetName dataset to ${writePath}.")

    dataset.write
      .mode("overwrite")
      .parquet(writePath)

    logger.info(s"Dataset $datasetName written successfully.")
  }
}
