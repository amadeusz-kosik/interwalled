package me.kosik.interwalled.spark.join

class BroadcastPartitionedMinMaxAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = BroadcastPartitionedMinMaxAIListIntervalJoin
}
