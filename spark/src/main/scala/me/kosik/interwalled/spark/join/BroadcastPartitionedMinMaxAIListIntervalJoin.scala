package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{MinMaxAIList, MinMaxAIListBuilder}
import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.utility.FPUtils
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions => f}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.runtime.universe._


object BroadcastPartitionedMinMaxAIListIntervalJoin extends IntervalJoin {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val aiListsWithBoundaries: Map[String, MinMaxAIList[T]] = lhsInput
      .repartition(f.col(IntervalColumns.KEY))
      .rdd
      .groupBy(_.key)
      .map { case (key, intervals) =>
        val aiListBuilder = new MinMaxAIListBuilder[T]()
        intervals.foreach(aiListBuilder.put)
        key -> aiListBuilder.build()
      }
      .collect()
      .toMap

    val intervalListsBroadcast = spark.sparkContext
      .broadcast(aiListsWithBoundaries.map {
        case (key, minMaxAIList) => key -> minMaxAIList.aiList
      })

    val boundaries = aiListsWithBoundaries.flatMap {
      case (key, minMaxAIList) => FPUtils.flipOption(minMaxAIList.leftBound, minMaxAIList.rightBound) match {
        case Some((leftBound, rightBound)) =>
          Some(key -> (leftBound, rightBound))

        case _ =>
          None
      }
    }

    val boundariesBroadcast = spark.sparkContext.broadcast(boundaries)

    val filteredRhsInput = rhsInput.filter { interval =>
      boundariesBroadcast.value.get(interval.key) match {
        case Some((min, max)) =>
          ! ((interval.from > max) || (interval.to < min))

        case None =>
          false
      }
    }

    val joinedRDD = filteredRhsInput.mapPartitions( _.flatMap { rhsInterval =>
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
