package me.kosik.interwalled.benchmark.preprocessing

case class PreprocessingResult(
  dataSuite: String,
  preprocessor: String,
  lhsRowsCount: Long,
  lhsRowsPerPartition: Array[Int],
  rhsRowsCount: Long,
  rhsRowsPerPartition: Array[Int]
)
