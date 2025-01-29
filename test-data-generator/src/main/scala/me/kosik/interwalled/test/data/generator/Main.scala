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

  val testDataSizes: Array[Long] = {
    if(sys.env.getOrElse("INTERWALLED_RUN_100K", "FALSE") != "FALSE")
      Array(100L, 1 * 1000L, 10 * 1000L, 100 * 1000L)
    else if(sys.env.getOrElse("INTERWALLED_RUN_10K", "FALSE") != "FALSE")
      Array(100L, 1 * 1000L, 10 * 1000L)
    else
      Array(100L, 1 * 1000L)
  }
  val testCaseNames = Array("one-to-one", "one-to-many", "one-to-all")

  testDataSizes foreach { testDataSize => testCaseNames foreach { testCaseName =>

    val testCase = testCaseName match {
      case "one-to-one"   => TestOneToOne(testDataSize)
      case "one-to-many"  => TestOneToMany(testDataSize, 4)
      case "one-to-all"   => TestOneToAll(testDataSize)

      case unknown => sys.exit(1)
    }

    write(testCaseName, testCase.generateLHS,     s"$testDataSize/in-lhs")
    write(testCaseName, testCase.generateRHS,     s"$testDataSize/in-rhs")
    write(testCaseName, testCase.generateResult,  s"$testDataSize/out-result")
  }}


  private def write[T](testCaseName: String, dataset: Dataset[T], datasetName: String): Unit = {
    val writePath = f"data/${testCaseName}/$datasetName.parquet"

    logger.info(s"Writing $datasetName dataset to ${writePath}.")

    dataset.write
      .mode("overwrite")
      .parquet(writePath)

    logger.info(s"Dataset $datasetName written successfully.")
  }
}
