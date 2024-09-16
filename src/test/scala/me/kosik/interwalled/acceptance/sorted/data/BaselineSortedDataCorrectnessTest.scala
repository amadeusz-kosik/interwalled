package me.kosik.interwalled.acceptance.sorted.data

import me.kosik.interwalled.acceptance.SortedDataCorrectnessTest
import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Column, DataFrame}

class BaselineSortedDataCorrectnessTest extends SortedDataCorrectnessTest {

  override def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame =
    lhsDF.join(rhsDF, joinPredicate, joinType)
}
