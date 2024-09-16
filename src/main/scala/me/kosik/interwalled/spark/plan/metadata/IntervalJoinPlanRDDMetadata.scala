package me.kosik.interwalled.spark.plan.metadata

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan


case class IntervalJoinPlanRDDMetadata(
  plan: SparkPlan,
  groupingExpression: Expression,
  intervalStartExpression: Expression,
  intervalEndExpression: Expression
)
