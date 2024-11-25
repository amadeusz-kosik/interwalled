package me.kosik.interwalled.test.data.generator

import me.kosik.interwalled.test.data.generator.test.cases.{TestOneToAll, TestOneToMany, TestOneToOne}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

object Main extends App {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("TestDataGenerator")
    .master("local[4]")
    .getOrCreate()

  val testDataSize = args(0).toLong

  val testCase = args(1) match {
    case "one-to-one" =>
      TestOneToOne(testDataSize)

    case "one-to-many" =>
      TestOneToMany(testDataSize, 4)

    case "one-to-all" =>
      TestOneToAll(testDataSize)

    case unknown =>
      logger.error(s"Unknown test case: $unknown.")
      sys.exit(1)
  }

  write(testCase.generateLHS,     s"$testDataSize/in-lhs")
  write(testCase.generateRHS,     s"$testDataSize/in-rhs")
  write(testCase.generateResult,  s"$testDataSize/out-result")


  private def write[T](dataset: Dataset[T], datasetName: String): Unit = {
    val writePath = f"data/${testCase.testCaseName}/$datasetName.parquet"

    logger.info(s"Writing $datasetName dataset to ${writePath}.")

    dataset.write
      .mode("overwrite")
      .parquet(writePath)

    logger.info(s"Dataset $datasetName written successfully.")
  }
}
