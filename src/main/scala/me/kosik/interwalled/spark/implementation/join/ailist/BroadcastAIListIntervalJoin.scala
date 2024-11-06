package me.kosik.interwalled.spark.implementation.join.ailist

import me.kosik.interwalled.ailist.{AIList, AIListBuilder, Interval}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

import scala.jdk.CollectionConverters.IteratorHasAsScala


object BroadcastAIListIntervalJoin extends Serializable {

  def overlapJoin(spark: SparkSession, broadcastRDD: RDD[(String, Interval[InternalRow])],partitionedRDD: RDD[(String, Interval[InternalRow])]): RDD[(InternalRow, InternalRow)] = {

    /* Collect only Reference regions and the index of indexedRdd1 */
    val intervalTrees: Map[String, AIList[InternalRow]] = {
      val collectedIntervals = broadcastRDD.collect()
      val trees = collectedIntervals
        .groupBy { case (partition, _) => partition }
        .map { case (partition, intervals) =>
          val aiList = new AIListBuilder[InternalRow](10, 20, 10, 64) // FIXME: hardcoded values
          intervals
            .map { case (_, values) => values }
            .foreach(aiList.put)
          partition -> aiList.build()
        }
      trees
    }

    val intervalTreesBroadcast = spark.sparkContext
      .broadcast(intervalTrees)

    val joinedRDD: RDD[(InternalRow, InternalRow)] = partitionedRDD.mapPartitions { partition =>

      partition.flatMap { case (partition, interval) =>
        intervalTreesBroadcast.value.get(partition) match {
          case Some(tree) =>
            tree.overlapping(interval)
              .asScala
              .map(broadcastInterval =>  broadcastInterval.value -> interval.value)

          case None =>
            Iterator.empty[(InternalRow, InternalRow)]
        }
      }
    }

    joinedRDD
  }
}
