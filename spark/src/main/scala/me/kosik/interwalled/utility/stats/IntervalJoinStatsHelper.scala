package me.kosik.interwalled.utility.stats

import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{Input, PreparedInput}
import me.kosik.interwalled.utility.stats.model.{DatasetStats, IntervalJoinRunStats, PartitioningStats}
import org.apache.spark.sql.Dataset


object IntervalJoinStatsHelper {

  def gatherAllStatistics(input: Input, preparedInput: PreparedInput, rawResult: Dataset[_], finalResult: Dataset[_]): IntervalJoinRunStats =
    ???

  def gatherInputStatistics(input: Input, preparedInput: PreparedInput): IntervalJoinRunStats =
    ???

  private def gatherInputStatistics(rawInput: Dataset[_], preparedInput: Dataset[_]): DatasetStats =
    DatasetStats(
      rawRowsCount      = rawInput.count(),
      finalRowsCount    = preparedInput.count(),
      finalPartitioning = computePartitioningStats(preparedInput)
    )

  private def gatherResultStatistics(rawResult: Dataset[_], finalResult: Dataset[_]): DatasetStats =
    DatasetStats(
      rawRowsCount      = rawResult.count(),
      finalRowsCount    = finalResult.count(),
      finalPartitioning = computePartitioningStats(finalResult)
    )

  private def computePartitioningStats(dataset: Dataset[_]): PartitioningStats = {
    val partitionsCount = dataset
      .rdd
      .glom()
      .map(_.length)
      .collect()
      .sorted

    val index50 = (partitionsCount.length * 0.5).toInt
    val index90 = Math.min((partitionsCount.length * 0.9).toInt, partitionsCount.length - 1)
    val index00 = partitionsCount.length - 1

    PartitioningStats(
      partitionsCount     = partitionsCount.length.toLong,
      rowsPerPartition50  = partitionsCount(index50),
      rowsPerPartition90  = partitionsCount(index90),
      rowsPerPartition00  = partitionsCount(index00)
    )
  }
}
