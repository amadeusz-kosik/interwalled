package me.kosik.interwalled.spark.join

class PartitionedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new PartitionedNativeAIListIntervalJoin(10000, 10)
}

