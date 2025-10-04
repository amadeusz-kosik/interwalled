package me.kosik.interwalled.utility.stats.model

case class PartitioningStats(
  partitionsCount:     Long,
  rowsPerPartition50:  Long,
  rowsPerPartition90:  Long,
  rowsPerPartition00:  Long
)

object PartitioningStats {
  def empty: PartitioningStats = PartitioningStats(
    partitionsCount      = 0,
    rowsPerPartition50   = 0,
    rowsPerPartition90   = 0,
    rowsPerPartition00   = 0
  )
}
