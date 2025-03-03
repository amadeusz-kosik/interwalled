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

  val testDataSizes: Array[(Int, Long)] = {
    if(generateLargeDataset.toLowerCase == "true")
      ActiveBenchmarks.TestDataSizes.extended
    else
      ActiveBenchmarks.TestDataSizes.baseline
  }

  val testCases = Array(
    (clustersCount: Int, rowsPerCluster: Long) => TestOneToOne(clustersCount, rowsPerCluster),
    (clustersCount: Int, rowsPerCluster: Long) => TestSparse(clustersCount, rowsPerCluster, 16),
    (clustersCount: Int, rowsPerCluster: Long) => TestOneToAll(clustersCount, rowsPerCluster)
  )

  testDataSizes foreach { case (clustersCount, testDataSize) => testCases foreach { testCaseCallback =>
    val testCase = testCaseCallback(clustersCount, testDataSize)
    val testCaseName = testCase.testCaseName

    write(testCaseName, testCase.generateLHS,     s"$testDataSize/${clustersCount}/database")
    write(testCaseName, testCase.generateRHS,     s"$testDataSize/${clustersCount}/query")
    write(testCaseName, testCase.generateResult,  s"$testDataSize/${clustersCount}/result")
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
