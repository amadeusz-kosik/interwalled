package me.kosik.interwalled.spark.join


class BroadcastAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = BroadcastAIListIntervalJoin
}
