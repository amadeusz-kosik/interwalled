package me.kosik.interwalled.spark.join

class BroadcastPartitionedAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new BroadcastPartitionedAIListIntervalJoin(10 * 1000L)
}
