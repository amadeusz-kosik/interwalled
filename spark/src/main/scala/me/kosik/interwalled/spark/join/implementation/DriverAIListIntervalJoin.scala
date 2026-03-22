package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.{AIListConfiguration, IntervalsPair}
import me.kosik.interwalled.ailist.{AIList, AIListBuilder}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import org.apache.spark.sql._

import scala.collection.JavaConverters._


object DriverAIListIntervalJoin extends IntervalJoin {

  override def toString: String = {
    "driver-ailist"
  }

  override protected def prepareInput(input: PreparedInput): PreparedInput = input

  override protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
    implicit val spark: SparkSession = input.lhsData.sparkSession
    import spark.implicits._

    val aiLists: Map[String, AIList] = input.lhsData.collect()
      .groupBy(_.key)
      .map { case (key, intervalsArray) =>
        val aiListBuilder = new AIListBuilder(AIListConfiguration.apply)
        intervalsArray.map(_.withoutBucketing).foreach(aiListBuilder.put)
        (key, aiListBuilder.build())
      }

    val intervalListsBroadcast = spark.sparkContext
      .broadcast(aiLists)

    val joinedRDD = input.rhsData.mapPartitions( _.flatMap { rhsInterval =>
      intervalListsBroadcast.value.get(rhsInterval.key) match {
        case Some(aiList) =>
          aiList
            .overlapping(rhsInterval.withoutBucketing)
            .asScala
            .map(lhsInterval => IntervalsPair(
              lhsInterval.key,
              lhsInterval.from,
              lhsInterval.to,
              lhsInterval.value,
              rhsInterval.from,
              rhsInterval.to,
              rhsInterval.value
            ))

        case None =>
          Iterator.empty
      }
    })

    joinedRDD
      .toDF()
      .as[IntervalsPair]
  }

  override protected def finalizeResult(result: Result): Result =
    result
}
