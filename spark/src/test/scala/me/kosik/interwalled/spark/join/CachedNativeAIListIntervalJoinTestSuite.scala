package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.CachedNativeAIListIntervalJoin

class CachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    new CachedNativeAIListIntervalJoin(config, None)
  }
}

