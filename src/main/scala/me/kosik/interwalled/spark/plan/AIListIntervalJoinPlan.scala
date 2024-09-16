package me.kosik.interwalled.spark.plan

import me.kosik.interwalled.spark.plan.metadata.IntervalJoinPlanRDDMetadata
import org.apache.spark.sql.catalyst.expressions.{Attribute, InterpretedProjection}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

abstract class AIListIntervalJoinPlan(
  lhsRDDMetadata: IntervalJoinPlanRDDMetadata,
  rhsRDDMetadata: IntervalJoinPlanRDDMetadata
) extends BinaryExecNode {

  override def left: SparkPlan =
    lhsRDDMetadata.plan

  override def right: SparkPlan =
    rhsRDDMetadata.plan

  @transient override lazy val output: Seq[Attribute] =
    left.output ++ right.output

  @transient protected lazy val lhsKeyGenerator: InterpretedProjection =
    AIListIntervalJoinPlanHelper.getKeyGenerator(lhsRDDMetadata)

  @transient protected lazy val rhsKeyGenerator: InterpretedProjection =
    AIListIntervalJoinPlanHelper.getKeyGenerator(rhsRDDMetadata)
}
