package me.kosik.interwalled.spark.sorted.data

import org.apache.spark.sql.{Column, DataFrame}

class BaselineSortedDataCorrectnessTest extends AbstractJoinTestSuite {

  override def validateSparkExecutionPlan(planString: String): Boolean =
    ! planString.contains("IntervalJoinPlan")

  override def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame =
    lhsDF.repartition(1000).join(rhsDF.repartition(1000), joinPredicate, joinType)
}
