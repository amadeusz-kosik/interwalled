package me.kosik.interwalled.spark.sorted.data

import me.kosik.interwalled.spark.SortedDataCorrectnessTest
import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Column, DataFrame}

class BaselineSortedDataCorrectnessTest extends SortedDataCorrectnessTest {

  override def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame =
    lhsDF.join(rhsDF, joinPredicate, joinType)
}
