package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions => f}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.reflect.runtime.universe._


object PartitionedAIListIntervalJoin extends IntervalJoin {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession
    import spark.implicits._

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val aiListsLHS = datasetToAILists(lhsInput.rdd)
    val aiListsRHS = rhsInput.rdd.map(interval => (interval.key, interval))

    val joinedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case (key, (lhsAILists, rhsIntervals)) =>
        for {
          lhsAIList   <- lhsAILists
          rhsInterval <- rhsIntervals
          overlapping <- lhsAIList.overlapping(rhsInterval).asScala
        } yield IntervalsPair(key, overlapping, rhsInterval)
      }

    joinedRDD.toDF().as[IntervalsPair[T]]
  }

  private def datasetToAILists[T : TypeTag](inputRDD: RDD[Interval[T]]) = {
    inputRDD
      .groupBy(_.key)
      .map { case (key, intervals) =>
        val aiListBuilder = new AIListBuilder[T]()
        intervals.foreach(aiListBuilder.put)
        key -> aiListBuilder.build()
      }
  }
}
