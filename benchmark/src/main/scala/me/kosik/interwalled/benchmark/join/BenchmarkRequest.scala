package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.spark.join.api.IntervalJoin


case class BenchmarkRequest(
  dataSuite: TestDataSuiteMetadata,
  join: IntervalJoin
)
