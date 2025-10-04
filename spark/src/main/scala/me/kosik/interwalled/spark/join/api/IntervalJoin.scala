package me.kosik.interwalled.spark.join.api

import me.kosik.interwalled.domain.IntervalsPair
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{Input, PreparedInput, Result}
import me.kosik.interwalled.utility.stats.IntervalJoinRunStatsHelper
import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats
import org.apache.spark.internal.Logging
import org.apache.spark.sql._


trait IntervalJoin extends Logging with Serializable {

  override def toString: String =
    throw new NotImplementedError("Classes inheriting from IntervalJoin trait are expected to" +
      " implement custom version of toString method.")

  def join(input: Input): Result =
    join(input, gatherStatistics = false)

  def join(input: Input, gatherStatistics: Boolean): Result = {
    val prepared    = prepareInput(input.toPreparedInput)
    val joined      = doJoin(prepared)
    val finalized   = finalizeResult(joined)

    val statistics: Option[IntervalJoinRunStats] = {
      if(gatherStatistics)
        Some(IntervalJoinRunStatsHelper.gatherStats(finalized))
      else
        None
    }

    Result(finalized, statistics)
  }

  protected def prepareInput(input: PreparedInput): PreparedInput

  protected def doJoin(input: PreparedInput): Dataset[IntervalsPair]

  protected def finalizeResult(joinedResultRaw: Dataset[IntervalsPair]): Dataset[IntervalsPair]
}
