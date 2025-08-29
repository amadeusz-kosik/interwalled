package me.kosik.interwalled.benchmark.test.data.datasets

import me.kosik.interwalled.benchmark.test.data.datasets.deterministic.TestUniform
import me.kosik.interwalled.benchmark.test.data.datasets.random.{TestRandomNormal, TestRandomPoisson, TestRandomUniform}
import me.kosik.interwalled.benchmark.test.data.model.{IntervalLength, IntervalMargin, TestDataFilter}


object TestCases {
  private val MAX_TEST_DATASET_SIZE: Long      = 100L * 1000L * 1000L

  val values: Map[String, TestCase] = Array(
    // Point by point: (0, 0), (1, 1), (2, 2)...
    new TestUniform("single-point-continuous", IntervalLength(0), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Point by point, only even numbers: (0, 0), (2, 2), (4, 4)...
    new TestUniform("single-point-even", IntervalLength(0), IntervalMargin(1), MAX_TEST_DATASET_SIZE, TestDataFilter(row => row.from % 2 == 0)),

    // Point by point, only odd numbers: (1, 1), (3, 3), (5, 5)...
    new TestUniform("single-point-odd", IntervalLength(0), IntervalMargin(1), MAX_TEST_DATASET_SIZE, TestDataFilter(row => row.from % 2 == 1)),

    // Short spans, overlap of 2: (0, 10), (5, 15), (10, 20)...
    new TestUniform("short-overlap", IntervalLength(10), IntervalMargin(-5), MAX_TEST_DATASET_SIZE),

    // Short spans, no overlap: (0, 9), (10, 19), (20, 29)...
    new TestUniform("short-continuous", IntervalLength(9), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Short spans, short margins: (0, 9), (20, 29), (40, 49)...
    new TestUniform("short-separated", IntervalLength(9), IntervalMargin(11), MAX_TEST_DATASET_SIZE),

    // Short spans, long margins: (0, 9), (100, 109), (200, 209)...
    new TestUniform("short-sparse", IntervalLength(9), IntervalMargin(91), MAX_TEST_DATASET_SIZE),

    // Long spans, overlap of 2: (0, 10 000), (5 000, 15 000), (10 000, 20 000)...
    new TestUniform("long-overlap", IntervalLength(10000), IntervalMargin(-5000), MAX_TEST_DATASET_SIZE),

    // Long spans, short margins: (0, 9 999), (10 000, 19 999), (20 000, 29 999)...
    new TestUniform("long-continuous", IntervalLength(9999), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Long spans, long margins: (0, 10 000), (20 000, 30 000), (40 000, 50 000)...
    new TestUniform("long-sparse", IntervalLength(10000), IntervalMargin(10000), MAX_TEST_DATASET_SIZE),

    // 1/10 - 1 of domain length
    new TestUniform("sparse-10", IntervalLength(MAX_TEST_DATASET_SIZE.toInt / 10 - 1), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Full domain length
    new TestUniform("sparse-01", IntervalLength(MAX_TEST_DATASET_SIZE.toInt), IntervalMargin(1), MAX_TEST_DATASET_SIZE),

    // Random - normal distribution, single points
    new TestRandomNormal("random-normal-single", IntervalLength(0), MAX_TEST_DATASET_SIZE),

    // Random - normal distribution, short spans
    new TestRandomNormal("random-normal-short", IntervalLength(10), MAX_TEST_DATASET_SIZE),

    // Poisson is too slow ATM, so it is set to 1/10 of others

    // Random - poisson distribution, single points
    new TestRandomPoisson("random-poisson-single", IntervalLength(0), MAX_TEST_DATASET_SIZE / 10),

    // Random - poisson distribution, short spans
    new TestRandomPoisson("random-poisson-short", IntervalLength(10), MAX_TEST_DATASET_SIZE / 10),

    // Random - unform distribution, single points
    new TestRandomUniform("random-uniform-single", IntervalLength(0), MAX_TEST_DATASET_SIZE),

    // Random - unform distribution, short spans
    new TestRandomUniform("random-uniform-short", IntervalLength(10), MAX_TEST_DATASET_SIZE),
  ).map(testCase => testCase.testCaseName -> testCase).toMap
}
