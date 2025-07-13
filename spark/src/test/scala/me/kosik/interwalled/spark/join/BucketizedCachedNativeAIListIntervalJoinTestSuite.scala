package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.CachedNativeAIListIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale

class BucketizedCachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    new CachedNativeAIListIntervalJoin(config, Some(BucketScale(10)))
  }
}

