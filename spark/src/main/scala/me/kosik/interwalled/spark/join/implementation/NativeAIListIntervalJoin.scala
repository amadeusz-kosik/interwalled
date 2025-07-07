package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import me.kosik.interwalled.spark.join.api.{BucketizedJoin, IntervalJoin}
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.DriverAIListIntervalJoin.bucketizer
import me.kosik.interwalled.utility.bucketizer.{BucketingConfig, Bucketizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}
import org.apache.spark.sql.expressions.Window

import scala.annotation.tailrec
import scala.reflect.runtime.universe._



class NativeAIListIntervalJoin(config: AIListConfig, bucketingConfig: Option[BucketingConfig]) extends IntervalJoin {
  private val bucketizer = new Bucketizer(bucketingConfig)

  override protected def prepareInput[T : TypeTag](input: Input[T]): PreparedInput[T] =
    (bucketizer.bucketize(input.lhsData), bucketizer.bucketize(input.rhsData))

  override protected def doJoin[T: TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared: BucketedIntervals[T]): DataFrame = {
    import lhsInputPrepared.sparkSession.implicits._
    import IntervalColumns._

    lhsInputPrepared.cache()

    val joinDatabase = {
      val emptyDF = lhsInputPrepared.sparkSession
        .emptyDataset[Interval[T]]
        .toDF()
        .withColumn(BUCKET, F.lit(0))
        .withColumn(_COMPONENT, F.lit(0))
        .withColumn("_ailist_max_end", F.lit(0))

      val inputDF = lhsInputPrepared
        .toDF()
        .sort(KEY, BUCKET, FROM, TO)

      iterate(inputDF, emptyDF, config.maximumComponentsCount)
        .repartition(F.col(IntervalColumns.KEY), F.col(BUCKET), F.col(_COMPONENT))  // FIXME why here error?
        .rdd
        .map(row => (row.getAs[String](KEY), row.getAs[Long](BUCKET)) -> row)
    }

    val joinQuery = {
      rhsInputPrepared
        .toDF()
        .rdd
        .map(row => (row.getAs[String](KEY), row.getAs[Long](BUCKET)) -> row)
    }

    val joinResultRDD: RDD[IntervalsPair[T]] = (joinDatabase cogroup joinQuery).flatMap { case ((key, _), (aiList, queries)) =>
      queries flatMap { query =>
        val queryFrom = query.getAs[Long](FROM)
        val queryTo = query.getAs[Long](TO)

        val matching = aiList
          .dropWhile(row => row.getAs[Long](_MAX_E) < queryFrom)
          .takeWhile(row => row.getAs[Long](FROM) <= queryTo)
          .filter(row => row.getAs[Long](TO) >= queryFrom)

        matching map { row =>
          val lhsInterval = Interval(
            key,
            row.getAs[Long](FROM),
            row.getAs[Long](TO),
            row.getAs[T](VALUE),
          )

          val rhsInterval = Interval(
            key,
            query.getAs[Long](FROM),
            query.getAs[Long](TO),
            query.getAs[T](VALUE),
          )

          IntervalsPair(key, lhsInterval, rhsInterval)
        }
      }
    }

    val joinResult = lhsInputPrepared.sparkSession
      .createDataset(joinResultRDD)
      .toDF

    joinResult
  }

  override protected def finalizeResult[T : TypeTag](joinedResultRaw: DataFrame): Dataset[IntervalsPair[T]] = {
    import joinedResultRaw.sparkSession.implicits._

    bucketizer
      .deduplicate(joinedResultRaw)
      .as[IntervalsPair[T]]
  }

  @tailrec
  private def iterate(sourceDF: DataFrame, accumulatorDF: DataFrame, maxIterations: Int): DataFrame = {
    import IntervalColumns._

    if(maxIterations == 0) {
      sourceDF
    } else {
      val sourceInputLookaheadWindow = Window
        .partitionBy(KEY, BUCKET)
        .orderBy(FROM, TO)
        .rowsBetween(1, config.intervalsCountToCheckLookahead)

      val preparedDF = sourceDF
        .withColumn("_ailist_lookahead", F.collect_list(TO).over(sourceInputLookaheadWindow))
        .withColumn("_ailist_lookahead_overlapping", F.filter(F.col("_ailist_lookahead"), _ <= F.col(TO)))
        .withColumn("_ailist_lookahead_overlapping_count", F.size(F.col("_ailist_lookahead_overlapping")))
        .withColumn("_ailist_lookahead_overlapping_keeper", F.col("_ailist_lookahead_overlapping_count") < F.lit(config.intervalsCountToTriggerExtraction))

      val keepersDF   = preparedDF
        .filter(F.col("_ailist_lookahead_overlapping_keeper") === true)
        .withColumn(_COMPONENT, F.lit(maxIterations))
        .drop(
          "_ailist_lookahead",
          "_ailist_lookahead_overlapping",
          "_ailist_lookahead_overlapping_count",
          "_ailist_lookahead_overlapping_keeper"
        )
        .transform(calculateMaxEnd)
        .unionByName(accumulatorDF)

      val extractedDF = preparedDF
        .filter(F.col("_ailist_lookahead_overlapping_keeper") === false)
        .drop(
          "_ailist_lookahead",
          "_ailist_lookahead_overlapping",
          "_ailist_lookahead_overlapping_count",
          "_ailist_lookahead_overlapping_keeper"
        )

      if(extractedDF.isEmpty)
        keepersDF
      else
        iterate(extractedDF, keepersDF, maxIterations - 1)
    }
  }

  private def calculateMaxEnd(dataFrame: DataFrame): DataFrame = {
    import IntervalColumns._

    val maxEndWindow = Window
      .partitionBy(KEY, BUCKET, _COMPONENT)
      .orderBy(FROM, TO)
      .rowsBetween(Window.unboundedPreceding, 0)

    dataFrame
      .withColumn(_MAX_E, F.max(TO).over(maxEndWindow))
  }
}
