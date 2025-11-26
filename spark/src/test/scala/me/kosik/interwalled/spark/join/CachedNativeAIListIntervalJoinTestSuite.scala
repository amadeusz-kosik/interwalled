package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.NativeAIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.implementation.CachedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig

class CachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    val preprocessorConfig = PreprocessorConfig.empty
    new CachedNativeAIListIntervalJoin(NativeAIListConfig(config, preprocessorConfig))
  }
}

