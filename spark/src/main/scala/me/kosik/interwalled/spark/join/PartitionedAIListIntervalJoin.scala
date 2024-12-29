package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalsPair}
import me.kosik.interwalled.utility.Bucketizer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.annotation.nowarn
import scala.collection.mutable
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
      .flatMap { case (bucket, (lhsAIListsMaps, rhsIntervals)) =>
        logInfo(s"Computing bucket $bucket.")

        val intervalsPairs: Iterable[IntervalsPair[T]] = for {
          aiListsMap            <- lhsAIListsMaps
          rhsBucketedInterval   <- rhsIntervals
          aiList                <- aiListsMap.get(rhsBucketedInterval.key).toSeq
          rhsInterval            = Interval(
            rhsBucketedInterval.key,
            rhsBucketedInterval.from,
            rhsBucketedInterval.to,
            rhsBucketedInterval.value
          )
          overlapping           <- aiList.overlapping(rhsInterval).asScala
        } yield IntervalsPair(rhsInterval.key, overlapping, rhsInterval)

        logInfo(s"Intervals pairs computed for $bucket.")
        intervalsPairs
      }

    joinedRDD.toDF().as[IntervalsPair[T]]
  }

  private def datasetToAILists[T : TypeTag](inputRDD: RDD[BucketedInterval[T]]): RDD[(Long, Map[String, AIList[T]])] = {
    inputRDD
      .mapPartitions { partition =>

        val buckets: mutable.HashMap[Long, mutable.HashMap[String, AIListBuilder[T]]] =
          new mutable.HashMap[Long, mutable.HashMap[String, AIListBuilder[T]]]

        partition.foreach { bucketedInterval =>
          buckets
            .getOrElse(bucketedInterval._bucket, { new mutable.HashMap[String, AIListBuilder[T]]})
            .getOrElse(bucketedInterval.key, { new AIListBuilder[T]() })
            .put(Interval(
              bucketedInterval.key,
              bucketedInterval.from,
              bucketedInterval.to,
              bucketedInterval.value
            ))
        }

        buckets.map { case(bucket, builders) =>
          bucket -> builders.map { case (key, aiListBuilder) =>
            key -> aiListBuilder.build()
          }.toMap
        }.iterator
      }
  }
}
