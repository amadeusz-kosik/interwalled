package me.kosik.interwalled.benchmark.test.data.datasets

import me.kosik.interwalled.benchmark.test.data.datasets.deterministic.{TestUniform, TestUniformResult}
import me.kosik.interwalled.benchmark.test.data.datasets.deterministic.TestUniform.{TestDataFilter, TestDataMapping}
import me.kosik.interwalled.benchmark.test.data.datasets.random.{TestRandomNormal, TestRandomPoisson, TestRandomUniform}
import me.kosik.interwalled.benchmark.test.data.model.{IntervalLength, IntervalMargin}


object TestCases {
  private val MAX_BENCHMARK_DATASET_SIZE: Long = 100L * 1000L * 1000L
  private val MAX_UNIT_TEST_DATASET_SIZE: Long =          10L * 1000L

  private def uniformTestCases(datasetSizeLimit: Long, domainRightBorder: Long): Array[TestUniform] = Array(
    // Point by point: (0, 0), (1, 1), (2, 2)...
    new TestUniform("single-point", IntervalLength(0), IntervalMargin(1), datasetSizeLimit),

    new TestUniform("single-point-negative", IntervalLength(0), IntervalMargin(1), datasetSizeLimit,
      new TestDataMapping(row => row.copy(from = row.to * -1, to = row.from * -1)), TestDataFilter.default
    ),

    // Point by point, only even numbers: (0, 0), (2, 2), (4, 4)...
    new TestUniform("single-point-even", IntervalLength(0), IntervalMargin(1), datasetSizeLimit,
      TestDataMapping.default, TestDataFilter(row => row.from % 2 == 0)
    ),

    // Point by point, only odd numbers: (1, 1), (3, 3), (5, 5)...
    new TestUniform("single-point-odd", IntervalLength(0), IntervalMargin(1), datasetSizeLimit,
      TestDataMapping.default, TestDataFilter(row => row.from % 2 == 1)
    ),

    // Short spans, overlap of 2: (0, 10), (5, 15), (10, 20)...
    new TestUniform("short-overlap", IntervalLength(10), IntervalMargin(-5), datasetSizeLimit),

    // Short spans, no overlap: (0, 9), (10, 19), (20, 29)...
    new TestUniform("short-continuous", IntervalLength(9), IntervalMargin(1), datasetSizeLimit),

    // Short spans, short margins: (0, 9), (20, 29), (40, 49)...
    new TestUniform("short-separated", IntervalLength(9), IntervalMargin(11), datasetSizeLimit),

    // Short spans, long margins: (0, 9), (100, 109), (200, 209)...
    new TestUniform("short-sparse", IntervalLength(9), IntervalMargin(91), datasetSizeLimit),

    // Long spans, overlap of 2: (0, 10 000), (5 000, 15 000), (10 000, 20 000)...
    new TestUniform("long-overlap", IntervalLength(10000), IntervalMargin(-5000), datasetSizeLimit),

    // Long spans, short margins: (0, 9 999), (10 000, 19 999), (20 000, 29 999)...
    new TestUniform("long-continuous", IntervalLength(9999), IntervalMargin(1), datasetSizeLimit),

    // Long spans, long margins: (0, 10 000), (20 000, 30 000), (40 000, 50 000)...
    new TestUniform("long-sparse", IntervalLength(10000), IntervalMargin(10000), datasetSizeLimit),

    // 1/10 - 1 of domain length
    new TestUniform("sparse-10", IntervalLength(domainRightBorder.toInt / 10 - 1), IntervalMargin(1), domainRightBorder),

    // Full domain length
    new TestUniform("sparse-01", IntervalLength(domainRightBorder.toInt), IntervalMargin(1), domainRightBorder)
  )

  private def randomTestCases(datasetSizeLimit: Long): Array[TestCase] = Array(
    // Random - normal distribution, single points
    new TestRandomNormal("random-normal-single", IntervalLength(0), datasetSizeLimit),

    // Random - normal distribution, short spans
    new TestRandomNormal("random-normal-short", IntervalLength(10), datasetSizeLimit),

    // Poisson is too slow ATM, so it is set to 1/10 of others

    // Random - poisson distribution, single points
    new TestRandomPoisson("random-poisson-single", IntervalLength(0), datasetSizeLimit / 10),

    // Random - poisson distribution, short spans
    new TestRandomPoisson("random-poisson-short", IntervalLength(10), datasetSizeLimit / 10),

    // Random - unform distribution, single points
    new TestRandomUniform("random-uniform-single", IntervalLength(0), datasetSizeLimit),

    // Random - unform distribution, short spans
    new TestRandomUniform("random-uniform-short", IntervalLength(10), datasetSizeLimit)
  )

  private def uniformTestCasesResults(datasetSizeLimit: Long, domainRightBorder: Long): Array[TestUniformResult] = {
    val testCasesLookup = uniformTestCases(datasetSizeLimit, domainRightBorder)
      .map(testCase => testCase.testCaseName -> testCase)
      .toMap

    Array(
      new TestUniformResult(testCasesLookup("single-point"),        testCasesLookup("single-point")),
      new TestUniformResult(testCasesLookup("single-point"),        testCasesLookup("short-continuous")),
      new TestUniformResult(testCasesLookup("single-point"),        testCasesLookup("long-continuous")),
      new TestUniformResult(testCasesLookup("short-continuous"),    testCasesLookup("sparse-10")),
      new TestUniformResult(testCasesLookup("single-point-even"),   testCasesLookup("single-point-odd")),
    )
  }

  val benchmarkData: Seq[TestCase] =
    uniformTestCases(MAX_BENCHMARK_DATASET_SIZE, MAX_BENCHMARK_DATASET_SIZE) ++
    randomTestCases(MAX_BENCHMARK_DATASET_SIZE)

  val unitTestData: Seq[TestCase] =
    uniformTestCases(MAX_UNIT_TEST_DATASET_SIZE, MAX_UNIT_TEST_DATASET_SIZE)

  val unitResults: Seq[TestUniformResult] =
    uniformTestCasesResults(MAX_UNIT_TEST_DATASET_SIZE, MAX_UNIT_TEST_DATASET_SIZE)
}
