package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions => f}

import scala.annotation.nowarn
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


// FIXME
class BroadcastPartitionedAIListIntervalJoin(bucketSize: Long) extends IntervalJoin {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val aiLists: Map[String, AIList[T]] = lhsInput
      .repartition(f.col(IntervalColumns.KEY))
      .rdd
      .groupBy(_.key)
      .map { case (key, intervals) =>
        val aiListBuilder = new AIListBuilder[T]()
        intervals.foreach(aiListBuilder.put)
        key -> aiListBuilder.build()
      }
      .collect()
      .toMap

    val intervalListsBroadcast = spark.sparkContext
      .broadcast(aiLists)

    val joinedRDD = rhsInput.mapPartitions( _.flatMap { rhsInterval =>
      intervalListsBroadcast.value.get(rhsInterval.key) match {
        case Some(aiList) =>
          aiList
            .overlapping(rhsInterval)
            .asScala
            .map(lhsInterval => IntervalsPair(lhsInterval.key, lhsInterval, rhsInterval))

        case None =>
          Iterator.empty
      }
    })

    joinedRDD.toDF().as[IntervalsPair[T]]
  }
}
