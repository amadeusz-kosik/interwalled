package com.eternalsh.interwalled
package spark.strategy

import spark.plan.{BroadcastAIListIntervalJoinPlan, IntervalJoinPlanRDDMetadata}
import spark.strategy.extractor.{BroadcastIntervalJoinPredicatePattern, FullIntervalJoinPredicatePattern, IntervalSource}

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}


class AIListIntervalJoinStrategy(spark: SparkSession) extends Strategy with Serializable with PredicateHelper {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case BroadcastIntervalJoinPredicatePattern(broadcast, partitioned) =>
      log.info(
        s"""Running broadcast interval join with following parameters:
           |  partition (chromosome) = (${broadcast.grouping}, ${partitioned.grouping})
           |  broadcast: left, partition: right
           |  lhsStart = ${broadcast.start}
           |  lhsEnd = ${broadcast.end}
           |  rhsStart = ${partitioned.start}
           |  rhsEnd = ${partitioned.end}
           |""".stripMargin
      )

      val broadcastMetadata = toJoinPlanRDDMetadata(broadcast)
      val partitionedMetadata = toJoinPlanRDDMetadata(partitioned)

      BroadcastAIListIntervalJoinPlan(broadcastMetadata, partitionedMetadata, spark) :: Nil

    case FullIntervalJoinPredicatePattern(left, right) =>
      log.info(
        s"""Running fully partitioned interval join with following parameters:
           |  partition (chromosome) = (${left.grouping}, ${right.grouping})
           |  lhsStart = ${left.start}
           |  lhsEnd = ${left.end}
           |  rhsStart = ${right.start}
           |  rhsEnd = ${right.end}
           |""".stripMargin
      )

      Nil // FIXME

    case _ =>
      Nil
  }

  private def toJoinPlanRDDMetadata(intervalSource: IntervalSource): IntervalJoinPlanRDDMetadata =
    IntervalJoinPlanRDDMetadata(
      planLater(intervalSource.plan),
      intervalSource.grouping,
      intervalSource.start,
      intervalSource.end
    )
}



