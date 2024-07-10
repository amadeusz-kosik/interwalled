package com.eternalsh.interwalled
package spark.plan

import com.eternalsh.interwalled.algorithm.Interval
import com.eternalsh.interwalled.spark.implementation.BroadcastAIListIntervalJoin
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, InterpretedProjection, UnsafeRow}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

case class IntervalJoinPlanRDDMetadata(
  plan: SparkPlan,
  groupingExpression: Expression,
  intervalStartExpression: Expression,
  intervalEndExpression: Expression
) {

  def getKeyGenerator: InterpretedProjection =
    new InterpretedProjection(Seq(
      intervalStartExpression,
      intervalEndExpression,
      groupingExpression
    ), plan.output)
}

object IntervalJoinPlanRDDMetadata {
  def extractFromInternalRow(keyGenerator: InterpretedProjection)(internalRow: InternalRow): (String, Interval[InternalRow]) = {
    val keys = keyGenerator(internalRow)
    val grouping = keys.getString(2)
    val interval = Interval(keys.getLong(0), keys.getLong(1), internalRow)
    grouping -> interval
  }
}

@DeveloperApi
case class BroadcastAIListIntervalJoinPlan(
  broadcastRDD: IntervalJoinPlanRDDMetadata,
  partitionedRDD: IntervalJoinPlanRDDMetadata,
  sparkSession: SparkSession
) extends BinaryExecNode {

  override def left: SparkPlan =
    broadcastRDD.plan

  override def right: SparkPlan =
    partitionedRDD.plan

  @transient
  override lazy val output: Seq[Attribute] = left.output ++ right.output

  @transient private lazy val broadcastKeyGenerator: InterpretedProjection =
    broadcastRDD.getKeyGenerator

  @transient private lazy val partitionedKeyGenerator: InterpretedProjection =
    partitionedRDD.getKeyGenerator

  override protected def withNewChildrenInternal(left: SparkPlan, right: SparkPlan): SparkPlan =
    copy(broadcastRDD = broadcastRDD.copy(plan = left), partitionedRDD = partitionedRDD.copy(plan = right))

  override protected def doExecute(): RDD[InternalRow] = {
    val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)

    val leftRDD = left
      .execute()
      .map(IntervalJoinPlanRDDMetadata.extractFromInternalRow(broadcastKeyGenerator))

    val rightRDD = right
      .execute()
      .map(IntervalJoinPlanRDDMetadata.extractFromInternalRow(partitionedKeyGenerator))

    BroadcastAIListIntervalJoin
      .overlapJoin(sparkSession, leftRDD, rightRDD)
      .mapPartitions { partition => {
        partition.map { case (lRow, rRow) => joiner.join(lRow.asInstanceOf[UnsafeRow], rRow.asInstanceOf[UnsafeRow])}
      }}
  }
}
