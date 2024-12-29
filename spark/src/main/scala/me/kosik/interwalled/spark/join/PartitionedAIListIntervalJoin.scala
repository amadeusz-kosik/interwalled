package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalsPair}
import me.kosik.interwalled.utility.Bucketizer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.runtime.universe._


object PartitionedAIListIntervalJoin extends IntervalJoin with Logging {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession
    import spark.implicits._

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val bucketizer = new Bucketizer(10_000)

    val lhsInputBucketed = lhsInput.transform(bucketizer.bucketize)
    val rhsInputBucketed = rhsInput.transform(bucketizer.bucketize)

    val aiListsLHS = datasetToAILists(lhsInputBucketed.rdd)
    val aiListsRHS = rhsInputBucketed.rdd.map(interval => interval._bucket -> interval)


    val joinedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case (bucket, (lhsAILists, rhsIntervals)) =>
        logInfo(s"Computing bucket $bucket.")

        val lhsAIListsMaps: Map[String, Seq[AIList[T]]] = lhsAILists
          .groupBy(_._1)
          .map { case (key, lists) => key -> lists.map(_._2).toSeq }

        val intervalsPairs: Iterable[IntervalsPair[T]] = for {
          rhsBucketedInterval   <- rhsIntervals.toSeq
          rhsInterval            = Interval(
            rhsBucketedInterval.key,
            rhsBucketedInterval.from,
            rhsBucketedInterval.to,
            rhsBucketedInterval.value
          )
          aiLists               <- lhsAIListsMaps.getOrElse(rhsBucketedInterval.key, Seq.empty)
          overlapping           <- aiLists.overlapping(rhsInterval).asScala
        } yield IntervalsPair(rhsInterval.key, overlapping, rhsInterval)

        logInfo(s"Intervals pairs computed for $bucket.")
        intervalsPairs
      }

    joinedRDD.toDF().as[IntervalsPair[T]]
  }

  private def datasetToAILists[T : TypeTag](inputRDD: RDD[BucketedInterval[T]]): RDD[(Long, (String, AIList[T]))] = {
    val grouped = inputRDD.mapPartitions { partition =>
      partition
        .toList
        .groupBy(bi => (bi.key, bi._bucket))
        .iterator
      }

    val mapped = grouped.mapPartitions { partition =>
      partition
        .map { case (key, _bucket) -> intervals =>
          val aiListBuilder = new AIListBuilder[T]()
          intervals.foreach(bi => aiListBuilder.put(Interval(bi.key, bi.from, bi.to, bi.value)))
          _bucket -> (key, aiListBuilder.build())
        }
        .iterator
    }

    mapped
  }
}
