package me.kosik.interwalled.spark.join

class PartitionedAIListIntervalJoinTestSuite(val bucketSize: Long) extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new PartitionedAIListIntervalJoin(bucketSize = bucketSize)
}

