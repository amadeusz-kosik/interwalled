package me.kosik.interwalled.test.data.generator

import me.kosik.interwalled.test.data.generator.test.cases.{TestOneToAll, TestOneToMany, TestOneToOne}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory


object Main extends App {
  val logger = LoggerFactory.getLogger(getClass)
  val env = MainEnv.build()
  val Array(generateLargeDataset) = args.take(1)

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("InterwalledTestDataGenerator")
    .config("spark.driver.memory",    env.driverMemory)
    .config("spark.executor.memory",  env.executorMemory)
    .master(env.sparkMaster)
    .getOrCreate()

  val testDataSizes: Array[Long] = {
    val baseline = Array(100L, 1000L, 10 * 1000L)
    val extended = Array(100 * 1000L, 1000 * 1000L)

    if(generateLargeDataset.toLowerCase == "true")
      baseline ++ extended
    else
      baseline
  }

  val testCases = Array(
    (testDataSize: Long) => TestOneToOne(testDataSize),
    (testDataSize: Long) => TestOneToMany(testDataSize,   4),
    (testDataSize: Long) => TestOneToMany(testDataSize,  64),
    (testDataSize: Long) => TestOneToMany(testDataSize, 256),
    (testDataSize: Long) => TestOneToAll(testDataSize)
  )

  testDataSizes foreach { testDataSize => testCases foreach { testCaseCallback =>
    val testCase = testCaseCallback(testDataSize)
    val testCaseName = testCase.testCaseName

    write(testCaseName, testCase.generateLHS,     s"$testDataSize/database")
    write(testCaseName, testCase.generateRHS,     s"$testDataSize/query")
    write(testCaseName, testCase.generateResult,  s"$testDataSize/result")
  }}


  private def write[T](testCaseName: String, dataset: Dataset[T], datasetName: String): Unit = {
    val writePath = f"${env.dataDirectory}/${testCaseName}/$datasetName.parquet"

    logger.info(s"Writing $datasetName dataset to ${writePath}.")

    dataset.write
      .mode("overwrite")
      .parquet(writePath)

    logger.info(s"Dataset $datasetName written successfully.")
  }
}
