package me.kosik.interwalled.spark.join.api.model

import me.kosik.interwalled.spark.join.api.model.IntervalStatistics.{InputStats, ResultStats}


case class IntervalStatistics(
  database: InputStats,
  query: InputStats,
  result: ResultStats
)

object IntervalStatistics {
  case class InputStats(
    rawRowsCount: Long,
    finalRowsCount: Long
  )

  case class ResultStats(
    rawRowsCount: Long,
    finalRowsCount: Long
  )
}