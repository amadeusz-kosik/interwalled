package me.kosik.interwalled.test.data.generator

import me.kosik.interwalled.domain.benchmark.ActiveBenchmarks
import me.kosik.interwalled.test.data.generator.test.cases.{TestAllToAll, TestCase, TestOneToAll, TestOneToOne, TestSpanning, TestSparse}
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
    (clustersCount: Int, rowsPerCluster: Long) => TestOneToAll(clustersCount, rowsPerCluster),
    (clustersCount: Int, rowsPerCluster: Long) => TestSpanning(clustersCount, rowsPerCluster, 4),
    (clustersCount: Int, rowsPerCluster: Long) => TestSpanning(clustersCount, rowsPerCluster, 16),
    (clustersCount: Int, rowsPerCluster: Long) => TestAllToAll(clustersCount, rowsPerCluster)
  )

  testCases foreach { testCaseCallback => testDataSizes foreach { case (clustersCount, testDataSize) =>
    val testCase = testCaseCallback(clustersCount, testDataSize)
    val testCaseName = testCase.testCaseName
    val outputFilesCount = 1 + (testDataSize / 1000000).toInt // 1M

    write(testCaseName, testCase.generateLHS.coalesce(outputFilesCount),     s"$testDataSize/${clustersCount}/database")
    write(testCaseName, testCase.generateRHS.coalesce(outputFilesCount),     s"$testDataSize/${clustersCount}/query")

    if(testCase.isInstanceOf[TestCase])
      writeIfPresent(
        testCaseName,
        testCase.generateResult.map(_.coalesce(outputFilesCount)),
        s"$testDataSize/${clustersCount}/result"
      )
  }}


  private def writeIfPresent[T](testCaseName: String, datasetOption: Option[Dataset[T]], datasetName: String): Unit = datasetOption match {
    case Some(dataset) =>
      write(testCaseName, dataset, datasetName)
    case None =>
      logger.info(s"Skipping writing results for $testCaseName on $datasetName - implementation not available.")
  }

  private def write[T](testCaseName: String, dataset: Dataset[T], datasetName: String): Unit = {
    val writePath = f"${env.dataDirectory}/${testCaseName}/$datasetName.parquet"

    logger.info(s"Writing $datasetName dataset to ${writePath}.")

    dataset
      .write
      .mode("overwrite")
      .parquet(writePath)

    logger.info(s"\tDataset $datasetName written successfully.")
  }
}
