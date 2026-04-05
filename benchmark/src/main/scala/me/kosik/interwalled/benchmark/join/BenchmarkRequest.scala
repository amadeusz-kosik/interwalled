package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuite
import me.kosik.interwalled.spark.join.api.IntervalJoin


case class BenchmarkRequest(
  dataSuite: TestDataSuite,
  join: IntervalJoin
)
