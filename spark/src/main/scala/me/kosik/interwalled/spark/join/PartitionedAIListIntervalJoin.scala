package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalsPair}
import me.kosik.interwalled.utility.Bucketizer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.annotation.nowarn
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


class PartitionedAIListIntervalJoin(bucketSize: Long) extends IntervalJoin with Logging {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession
    import spark.implicits._

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val bucketizer = new Bucketizer(bucketSize)

    val lhsInputBucketed = lhsInput.transform(bucketizer.bucketize)
    val rhsInputBucketed = rhsInput.transform(bucketizer.bucketize)

    val aiListsLHS = datasetToAILists(lhsInputBucketed.rdd)
    val aiListsRHS = rhsInputBucketed.rdd.map(interval => (interval._bucket, interval.key) -> interval)

    val joinedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case ((bucket, key), (lhsAIListsIterator, rhsIntervalsIterator)) =>
        logInfo(s"Computing bucket $bucket : $key.")

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

        logInfo(s"Intervals pairs computed for $bucket.")
        intervalsPairs
      }

    joinedRDD.toDF().as[IntervalsPair[T]]
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
