package me.kosik.interwalled.test.data.generator

import me.kosik.interwalled.domain.benchmark.ActiveBenchmarks
import me.kosik.interwalled.main.MainEnv
import me.kosik.interwalled.test.data.generator.data.types.{IntervalLength, IntervalMargin, TestDataFilter}
import me.kosik.interwalled.test.data.generator.test.cases.TestCase
import me.kosik.interwalled.test.data.generator.test.cases.deterministic.TestUniform
import me.kosik.interwalled.test.data.generator.test.cases.random.{TestRandomNormal, TestRandomPoisson, TestRandomUniform}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory


object Main extends App {
  import SaveMode._

  val logger = LoggerFactory.getLogger(getClass)
  val env = MainEnv.build()

  val PARTITIONS_PER_DATASET      = 16
  val MAX_TEST_DATASET_SIZE       = 100L * 1000L * 1000L

  implicit val spark: SparkSession =
    env.buildSparkSession("InterwalledTestDataGenerator")

  val testCases = Array(
    // Point by point: (0, 0), (1, 1), (2, 2)...
    new TestUniform(IntervalLength(0), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Point by point, only even numbers: (0, 0), (2, 2), (4, 4)...
    new TestUniform(IntervalLength(0), IntervalMargin(1), MAX_TEST_DATASET_SIZE, TestDataFilter(row => row.from % 2 == 0)),

    // Point by point, only odd numbers: (1, 1), (3, 3), (5, 5)...
    new TestUniform(IntervalLength(0), IntervalMargin(1), MAX_TEST_DATASET_SIZE, TestDataFilter(row => row.from % 2 == 1)),

    // Short spans, overlap of 2: (0, 10), (5, 15), (10, 20)...
    new TestUniform(IntervalLength(10), IntervalMargin(-5), MAX_TEST_DATASET_SIZE),

    // Short spans, no overlap: (0, 9), (10, 19), (20, 29)...
    new TestUniform(IntervalLength(9), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Short spans, short margins: (0, 9), (20, 29), (40, 49)...
    new TestUniform(IntervalLength(9), IntervalMargin(11), MAX_TEST_DATASET_SIZE),

    // Short spans, long margins: (0, 9), (100, 109), (200, 209)...
    new TestUniform(IntervalLength(9), IntervalMargin(91), MAX_TEST_DATASET_SIZE),

    // Long spans, overlap of 2: (0, 10 000), (5 000, 15 000), (10 000, 20 000)...
    new TestUniform(IntervalLength(10000), IntervalMargin(-5000), MAX_TEST_DATASET_SIZE),

    // Long spans, short margins: (0, 9 999), (10 000, 19 999), (20 000, 29 999)...
    new TestUniform(IntervalLength(9999), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Long spans, long margins: (0, 10 000), (20 000, 30 000), (40 000, 50 000)...
    new TestUniform(IntervalLength(10000), IntervalMargin(10000), MAX_TEST_DATASET_SIZE),

    // 1/10 - 1 of domain length
    new TestUniform(IntervalLength(MAX_TEST_DATASET_SIZE.toInt / 10 - 1), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Full domain length
    new TestUniform(IntervalLength(MAX_TEST_DATASET_SIZE.toInt), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Random - normal distribution, single points
    new TestRandomNormal(IntervalLength(0), MAX_TEST_DATASET_SIZE),

    // Random - normal distribution, short spans
    new TestRandomNormal(IntervalLength(10), MAX_TEST_DATASET_SIZE),

    // Poisson is too slow ATM, so it is set to 1/10 of others

    // Random - poisson distribution, single points
    new TestRandomPoisson(IntervalLength(0), MAX_TEST_DATASET_SIZE / 10),

    // Random - poisson distribution, short spans
    new TestRandomPoisson(IntervalLength(10), MAX_TEST_DATASET_SIZE / 10),

    // Random - unform distribution, single points
    new TestRandomUniform(IntervalLength(0), MAX_TEST_DATASET_SIZE),

    // Random - unform distribution, short spans
    new TestRandomUniform(IntervalLength(10), MAX_TEST_DATASET_SIZE),
  )

  testCases foreach { testCaseCallback =>
    logger.info(s"Generating ${testCaseCallback.testCaseName} data:")
    val testData = testCaseCallback.generate()
    val writePath = s"${env.dataDirectory}/test-data/${testCaseCallback.testCaseName}.parquet"

    testData
      .repartition(PARTITIONS_PER_DATASET)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(writePath)
  }
}
