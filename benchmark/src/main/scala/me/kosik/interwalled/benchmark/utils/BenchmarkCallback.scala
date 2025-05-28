package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TestData
import me.kosik.interwalled.domain.test.TestResultRow
import org.apache.spark.sql.Dataset


case class BenchmarkCallback(
  description: String,
  fn: TestData => Dataset[TestResultRow]
)
