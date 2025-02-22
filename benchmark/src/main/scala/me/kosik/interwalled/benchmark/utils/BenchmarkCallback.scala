package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TestData


case class BenchmarkCallback(
  description: String,
  fn: TestData => BenchmarkResult
)
