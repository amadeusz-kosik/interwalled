package me.kosik.interwalled.spark.join

class PartitionedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new PartitionedNativeAIListIntervalJoin(PartitionedNativeAIListIntervalJoinConfig(10000, 10))
}

