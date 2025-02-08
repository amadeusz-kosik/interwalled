package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.TestData

import scala.util.Try


case class BenchmarkCallback(
  description: String,
  fn: TestData => Try[BenchmarkResult]
)
