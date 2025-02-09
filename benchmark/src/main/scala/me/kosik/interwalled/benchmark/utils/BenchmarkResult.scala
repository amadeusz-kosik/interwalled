package me.kosik.interwalled.benchmark.utils


case class BenchmarkResult(
  dataSuite: String,
  dataSize: Long,
  joinName: String,
  elapsedTime: Long
)