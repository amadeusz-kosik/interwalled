package me.kosik.interwalled.spark.join

class PartitionedAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = PartitionedAIListIntervalJoin
}
