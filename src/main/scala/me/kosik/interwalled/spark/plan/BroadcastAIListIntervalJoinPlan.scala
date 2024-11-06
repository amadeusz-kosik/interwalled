package me.kosik.interwalled.spark.plan

import me.kosik.interwalled.spark.implementation.join.ailist.BroadcastAIListIntervalJoin
import me.kosik.interwalled.spark.plan.metadata.IntervalJoinPlanRDDMetadata
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.SparkPlan


@DeveloperApi
case class BroadcastAIListIntervalJoinPlan(
  broadcastRDDMetadata: IntervalJoinPlanRDDMetadata,
  partitionedRDDMetadata: IntervalJoinPlanRDDMetadata,
  sparkSession: SparkSession
) extends AIListIntervalJoinPlan(broadcastRDDMetadata, partitionedRDDMetadata) {

  override protected def withNewChildrenInternal(left: SparkPlan, right: SparkPlan): SparkPlan =
    copy(broadcastRDDMetadata = broadcastRDDMetadata.copy(plan = left), partitionedRDDMetadata = partitionedRDDMetadata.copy(plan = right))

  override protected def doExecute(): RDD[InternalRow] = {
    val broadcastRDD = left
      .execute()
      .map(AIListIntervalJoinPlanHelper.extractGroupingKeys(lhsKeyGenerator))

    val partitionedRDD = right
      .execute()
      .map(AIListIntervalJoinPlanHelper.extractGroupingKeys(rhsKeyGenerator))

    BroadcastAIListIntervalJoin
      .overlapJoin(sparkSession, broadcastRDD, partitionedRDD)
      .mapPartitions { partition => {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        partition.map { case (lRow, rRow) => joiner.join(lRow.asInstanceOf[UnsafeRow], rRow.asInstanceOf[UnsafeRow])}
      }}
  }
}
