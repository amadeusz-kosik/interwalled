package me.kosik.interwalled.spark.implementation.join.ailist

import me.kosik.interwalled.ailist.{AIList, AIListBuilder, Interval}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

import scala.jdk.CollectionConverters.IteratorHasAsScala


object FullAIListIntervalJoin extends Serializable {

  def overlapJoin(spark: SparkSession, leftRDD: RDD[(String, Interval[InternalRow])], rightRDD: RDD[(String, Interval[InternalRow])]): RDD[(InternalRow, InternalRow)] = {

    val rightAIListRDD = toDistributedAILists(rightRDD)

    val joinedRDD = (leftRDD cartesian rightAIListRDD)
      .mapPartitions { partitionIterator =>
        partitionIterator
          .filter { case ((lhsGroupKey, _), (rhsGroupKey, _)) =>
            lhsGroupKey == rhsGroupKey
          }
          .map { case ((_, lhsRow), (_, rhsTree)) =>
            (lhsRow, rhsTree)
          }
          .flatMap { case (lhsRow, rhsTree) =>
            rhsTree
              .overlapping(lhsRow)
              .asScala
              .map(lhsRow.value -> _.value)
          }
      }

    joinedRDD
  }

  private def toDistributedAILists(rdd: RDD[(String, Interval[InternalRow])]) = {
    rdd.mapPartitions { partitionIterator: Iterator[(String, Interval[InternalRow])] =>
      partitionIterator.toArray
        .groupBy { case (partition, _) => partition }
        .map { case (partition, intervals) =>
          val aiList = new AIListBuilder[InternalRow](10, 20, 10, 64) // FIXME: hardcoded values again
          intervals
            .map { case (_, values) => values }
            .foreach(aiList.put)
          partition -> aiList.build()
        }
        .iterator
    }
  }
}
