package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.runtime.universe._


object BroadcastAIListIntervalJoin extends IntervalJoin {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val aiLists: Map[String, AIList[T]] = lhsInput.collect()
      .groupBy(_.key)
      .map { case (key, intervalsArray) =>
        val aiListBuilder = new AIListBuilder[T]()
        intervalsArray.foreach(aiListBuilder.put)
        (key, aiListBuilder.build())
      }

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
