package me.kosik.interwalled.spark.plan

import me.kosik.interwalled.spark.implementation.join.ailist.FullAIListIntervalJoin
import me.kosik.interwalled.spark.plan.metadata.IntervalJoinPlanRDDMetadata
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.SparkPlan


@DeveloperApi
case class FullAIListIntervalJoinPlan(
  lhsRDDMetadata: IntervalJoinPlanRDDMetadata,
  rhsRDDMetadata: IntervalJoinPlanRDDMetadata,
  sparkSession: SparkSession
) extends AIListIntervalJoinPlan(lhsRDDMetadata, rhsRDDMetadata) {

  override protected def withNewChildrenInternal(left: SparkPlan, right: SparkPlan): SparkPlan =
    copy(lhsRDDMetadata = lhsRDDMetadata.copy(plan = left), rhsRDDMetadata = rhsRDDMetadata.copy(plan = right))

  override protected def doExecute(): RDD[InternalRow] = {
    val lhsRDD = left
      .execute()
      .map(AIListIntervalJoinPlanHelper.extractGroupingKeys(lhsKeyGenerator))

    val rhsRDD = right
      .execute()
      .map(AIListIntervalJoinPlanHelper.extractGroupingKeys(rhsKeyGenerator))

    FullAIListIntervalJoin
      .overlapJoin(sparkSession, lhsRDD, rhsRDD)
      .mapPartitions { partition => {
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        partition.map { case (lRow, rRow) => joiner.join(lRow.asInstanceOf[UnsafeRow], rRow.asInstanceOf[UnsafeRow])}
      }}
  }
}
