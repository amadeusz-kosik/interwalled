package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import me.kosik.interwalled.spark.join.api.{IntervalJoin}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{Input, Result}
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics.{InputStats, ResultStats}
import me.kosik.interwalled.utility.bucketizer.{BucketingConfig, Bucketizer, DummyBucketizer}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

import scala.annotation.nowarn
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


object DriverAIListIntervalJoin extends IntervalJoin {
  private val bucketizer = DummyBucketizer

  override protected def prepareInput[T : TypeTag](input: Input[T]): PreparedInput[T] =
    (bucketizer.bucketize(input.lhsData), bucketizer.bucketize(input.rhsData))

  protected def doJoin[T : TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared:  BucketedIntervals[T]): DataFrame ={
    implicit val spark: SparkSession = lhsInputPrepared.sparkSession

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val aiLists: Map[String, AIList[T]] = lhsInputPrepared.collect()
      .groupBy(_.key)
      .map { case (key, intervalsArray) =>
        val aiListBuilder = new AIListBuilder[T]()
        intervalsArray.map(_.toInterval).foreach(aiListBuilder.put)
        (key, aiListBuilder.build())
      }

    val intervalListsBroadcast = spark.sparkContext
      .broadcast(aiLists)

    val joinedRDD = rhsInputPrepared.mapPartitions( _.flatMap { rhsInterval =>
      intervalListsBroadcast.value.get(rhsInterval.key) match {
        case Some(aiList) =>
          aiList
            .overlapping(rhsInterval.toInterval)
            .asScala
            .map(lhsInterval => IntervalsPair(lhsInterval.key, lhsInterval, rhsInterval.toInterval))

        case None =>
          Iterator.empty
      }
    })

    joinedRDD.toDF()
  }

  override protected def finalizeResult[T : TypeTag](joinedResultRaw: DataFrame): Dataset[IntervalsPair[T]] = {
    import joinedResultRaw.sparkSession.implicits._

    joinedResultRaw
      .as[IntervalsPair[T]]
      .transform(bucketizer.deduplicate)
  }
}
