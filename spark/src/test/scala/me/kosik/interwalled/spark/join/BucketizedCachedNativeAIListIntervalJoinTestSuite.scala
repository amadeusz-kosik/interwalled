package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.CachedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


class BucketizedCachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    new CachedNativeAIListIntervalJoin(config, Some(PreprocessorConfig(1000)))
  }
}

