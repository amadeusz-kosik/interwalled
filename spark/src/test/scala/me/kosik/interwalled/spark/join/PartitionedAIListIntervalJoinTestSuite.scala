package me.kosik.interwalled.spark.join


class PartitionedAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new PartitionedAIListIntervalJoin(bucketSize = 10000)
}

