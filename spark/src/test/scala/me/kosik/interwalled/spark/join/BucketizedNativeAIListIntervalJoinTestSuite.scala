package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale

class BucketizedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    new NativeAIListIntervalJoin(config, Some(BucketScale(10)))
  }
}

