package me.kosik.interwalled.spark.join.api

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{Input, Result}
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics.{InputStats, ResultStats}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, functions => F}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


trait IntervalJoin extends Logging with Serializable {

  protected type BucketedIntervals[T] = Dataset[BucketedInterval[T]]
  protected type PreparedInput[T] = (BucketedIntervals[T], BucketedIntervals[T])

  def join[T : TypeTag](input: Input[T]): Result[T] =
    join(input, gatherStatistics = false)

  def join[T : TypeTag](input: Input[T], gatherStatistics: Boolean): Result[T] = {
    implicit val spark: SparkSession = input.lhsData.sparkSession

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

//    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
//    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val (lhsInputRaw,       rhsInputRaw)      = (input.lhsData, input.rhsData)
    val (lhsInputPrepared,  rhsInputPrepared) = prepareInput(input)

    val joinedResultRaw   = doJoin[T](lhsInputPrepared,  rhsInputPrepared)
    val joinedResultFinal = finalizeResult[T](joinedResultRaw)

    val statistics = {
      if(gatherStatistics) Some(IntervalStatistics(
        database  = InputStats(lhsInputRaw.count(),       rhsInputRaw.count()),
        query     = InputStats(lhsInputPrepared.count(),  rhsInputPrepared.count()),
        result    = ResultStats(joinedResultRaw.count(),  joinedResultFinal.count())
      )) else None
    }

    Result(joinedResultFinal, statistics)
  }

  protected def prepareInput[T : TypeTag](input: Input[T]): PreparedInput[T]
  protected def doJoin[T : TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared: BucketedIntervals[T]): DataFrame
  protected def finalizeResult[T : TypeTag](joinedResultRaw: DataFrame): Dataset[IntervalsPair[T]]

}
