package me.kosik.interwalled.spark.join.api

import me.kosik.interwalled.ailist.model.IntervalsPair
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{Input, PreparedInput}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._


trait IntervalJoin extends Logging with Serializable {

  override def toString: String =
    throw new NotImplementedError("Classes inheriting from IntervalJoin trait are expected to" +
      " implement custom version of toString method.")

  def join(input: Input): Dataset[IntervalsPair] = {
    val prepared    = prepareInput(input.toPreparedInput)
    val joined      = doJoin(prepared)
    val finalized   = finalizeResult(joined)

    finalized
  }

  protected def prepareInput(input: PreparedInput): PreparedInput

  protected def doJoin(input: PreparedInput): Dataset[IntervalsPair]

  protected def finalizeResult(result: Dataset[IntervalsPair]): Dataset[IntervalsPair]
}
