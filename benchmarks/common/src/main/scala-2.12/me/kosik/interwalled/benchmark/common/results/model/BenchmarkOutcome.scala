package me.kosik.interwalled.benchmark.common.results.model

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata


case class BenchmarkOutcome[T](
  benchmarkName:        String,
  dataSuiteMetadata:    TestDataSuiteMetadata,
  result:               BenchmarkResult[T]
)
