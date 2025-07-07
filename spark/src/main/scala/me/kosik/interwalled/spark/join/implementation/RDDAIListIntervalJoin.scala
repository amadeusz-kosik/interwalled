package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{BucketedInterval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import me.kosik.interwalled.spark.join.api.{BucketizedJoin, IntervalJoin}
import me.kosik.interwalled.spark.join.implementation.DriverAIListIntervalJoin.{PreparedInput, bucketizer}
import me.kosik.interwalled.utility.bucketizer.{BucketingConfig, Bucketizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


class RDDAIListIntervalJoin(bucketingConfig: Option[BucketingConfig]) extends IntervalJoin {
  private val bucketizer = new Bucketizer(bucketingConfig)

  override protected def prepareInput[T : TypeTag](input: Input[T]): PreparedInput[T] =
    (bucketizer.bucketize(input.lhsData), bucketizer.bucketize(input.rhsData))

  protected def doJoin[T : TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared: BucketedIntervals[T]): DataFrame = {
    import lhsInputPrepared.sparkSession.implicits._

    val aiListsLHS = datasetToAILists(lhsInputPrepared.rdd)
    val aiListsRHS = {
      val rhsInputRDD: RDD[BucketedInterval[T]] = rhsInputPrepared.rdd
      rhsInputRDD.map(interval => (interval._bucket, interval.key) -> interval)
    }

    val joinedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case ((bucket, key), (lhsAIListsIterator, rhsIntervalsIterator)) =>
        logDebug(s"Computing bucket $bucket : $key.")

        val aiLists = lhsAIListsIterator
          .toSeq

        val rhsIntervals = rhsIntervalsIterator
          .map(_.toInterval)
          .toSeq

        val intervalsPairs: Iterable[IntervalsPair[T]] = for {
          aiList        <- aiLists
          rhsInterval   <- rhsIntervals
          lhsInterval   <- aiList.overlapping(rhsInterval).asScala
        } yield IntervalsPair(rhsInterval.key, lhsInterval, rhsInterval)

        logDebug(s"Intervals pairs computed for $bucket.")
        intervalsPairs
      }

    val joined = joinedRDD
      .toDF()
      .drop(
        lhsInputPrepared.col(IntervalColumns.BUCKET),
        rhsInputPrepared.col(IntervalColumns.BUCKET)
      )

    joined
  }

  override protected def finalizeResult[T : TypeTag](joinedResultRaw: DataFrame): Dataset[IntervalsPair[T]] = {
    import joinedResultRaw.sparkSession.implicits._

    bucketizer
      .deduplicate(joinedResultRaw)
      .as[IntervalsPair[T]]
  }

  private def datasetToAILists[T : TypeTag](inputRDD: RDD[BucketedInterval[T]]): RDD[((Long, String), AIList[T])] = {
    val grouped = inputRDD.mapPartitions { partition =>
      partition
        .toList
        .groupBy(bi => (bi.key, bi._bucket))
        .iterator
      }

    val mapped = grouped.mapPartitions { partition =>
      partition
        .map { case ((key, _bucket), intervals) =>
          val aiListBuilder = new AIListBuilder[T]()
          intervals.foreach(bi => aiListBuilder.put(bi.toInterval))
          ((_bucket, key), aiListBuilder.build())
        }
    }

    mapped
  }
}
