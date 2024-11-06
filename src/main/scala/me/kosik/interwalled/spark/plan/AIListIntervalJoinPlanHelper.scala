package me.kosik.interwalled.spark.plan

import me.kosik.interwalled.ailist.Interval
import me.kosik.interwalled.spark.plan.metadata.IntervalJoinPlanRDDMetadata
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.InterpretedProjection


@DeveloperApi
object AIListIntervalJoinPlanHelper {
  // FIXME: AIList specific
  def extractGroupingKeys(keyGenerator: InterpretedProjection)(internalRow: InternalRow): (String, Interval[InternalRow]) = {
    val keys = keyGenerator(internalRow)
    val grouping = keys.getString(2)
    val interval = Interval(keys.getLong(0), keys.getLong(1), internalRow)
    grouping -> interval
  }

  def getKeyGenerator(rddMetadata: IntervalJoinPlanRDDMetadata): InterpretedProjection =
    new InterpretedProjection(Seq(
      rddMetadata.intervalStartExpression,
      rddMetadata.intervalEndExpression,
      rddMetadata.groupingExpression
    ), rddMetadata.plan.output)
}
