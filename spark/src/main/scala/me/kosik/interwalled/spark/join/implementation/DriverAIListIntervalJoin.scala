package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.{AIListConfiguration, IntervalsPair}
import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.model.SparkIntervalsPair
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import org.apache.spark.sql._

import scala.collection.JavaConverters._


object DriverAIListIntervalJoin extends IntervalJoin {

  override def toString: String = {
    "driver-ailist"
  }

  override protected def prepareInput(input: PreparedInput): PreparedInput = input

  override protected def doJoin(input: PreparedInput): Dataset[SparkIntervalsPair] = {
    implicit val spark: SparkSession = input.lhsData.sparkSession
    import spark.implicits._

    val aiLists: Map[String, AIList[String]] = input.lhsData.collect()
      .groupBy(_.key)
      .map { case (key, intervalsArray) =>
        val aiListBuilder = new AIListBuilder[String](AIListConfiguration.DEFAULT)
        intervalsArray.map(_.toAIListInterval).foreach(aiListBuilder.put)
        (key, aiListBuilder.build())
      }

    val intervalListsBroadcast = spark.sparkContext
      .broadcast(aiLists)

    val joinedRDD = input.rhsData.mapPartitions( _.flatMap { rhsInterval =>
      intervalListsBroadcast.value.get(rhsInterval.key) match {
        case Some(aiList) =>
          aiList
            .overlapping(rhsInterval.toAIListInterval)
            .asScala
            .map(lhsInterval => SparkIntervalsPair(lhsInterval, rhsInterval.toAIListInterval))

        case None =>
          Iterator.empty
      }
    })

    joinedRDD
      .toDF()
      .as[SparkIntervalsPair]
  }

  override protected def finalizeResult(result: Result): Result =
    result
}
