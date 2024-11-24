package me.kosik.interwalled.spark.sorted.data

import me.kosik.interwalled.spark.SortedDataCorrectnessTest
import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Column, DataFrame}

class BroadcastAIListSortedDataCorrectnessTest extends SortedDataCorrectnessTest {

  override def beforeEach(): Unit = {
    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil
  }

  override def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame =
    lhsDF.join(broadcast(rhsDF), joinPredicate, joinType)
}
