package me.kosik.interwalled.test.data.generator

import me.kosik.interwalled.domain.benchmark.ActiveBenchmarks
import me.kosik.interwalled.test.data.generator.test.cases.{TestOneToAll, TestOneToOne, TestSparse}
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
    if(generateLargeDataset.toLowerCase == "true")
      ActiveBenchmarks.TestDataSizes.extended
    else
      ActiveBenchmarks.TestDataSizes.baseline
  }

  val testCases = Array(
    (testDataSize: Long) => TestOneToOne(env.clustersCount, testDataSize),
    (testDataSize: Long) => TestSparse(env.clustersCount, testDataSize, 16),
    (testDataSize: Long) => TestOneToAll(env.clustersCount, testDataSize)
  )

  testDataSizes foreach { testDataSize => testCases foreach { testCaseCallback =>
    val testCase = testCaseCallback(testDataSize)
    val testCaseName = testCase.testCaseName

    write(testCaseName, testCase.generateLHS,     s"$testDataSize/${env.clustersCount}/database")
    write(testCaseName, testCase.generateRHS,     s"$testDataSize/${env.clustersCount}/query")
    write(testCaseName, testCase.generateResult,  s"$testDataSize/${env.clustersCount}/result")
  }}


  private def write[T](testCaseName: String, dataset: Dataset[T], datasetName: String): Unit = {
    val writePath = f"${env.dataDirectory}/${testCaseName}/$datasetName.parquet"

    logger.info(s"Writing $datasetName dataset to ${writePath}.")

    dataset.write
      .mode("overwrite")
      .parquet(writePath)

    logger.info(s"\tDataset $datasetName written successfully.")
  }
}
