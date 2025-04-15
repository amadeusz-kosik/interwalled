package me.kosik.interwalled.spark.join

class NativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = NativeAIListIntervalJoin
}

