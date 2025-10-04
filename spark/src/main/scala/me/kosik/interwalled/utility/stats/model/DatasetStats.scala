package me.kosik.interwalled.utility.stats.model

case class DatasetStats(
  rawRowsCount:       Long,
  finalRowsCount:     Long,
  finalPartitioning:  PartitioningStats
)

object DatasetStats {
  def empty: DatasetStats = DatasetStats(
    rawRowsCount      = 0,
    finalRowsCount    = 0,
    finalPartitioning = PartitioningStats.empty
  )
}
