package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.test.suite.TestDataSuite
import me.kosik.interwalled.spark.join.api.IntervalJoin


case class BenchmarkRequest(
  dataSuite: TestDataSuite,
  join: IntervalJoin
)
