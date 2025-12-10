package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.implementation.ailist.native.ailist.CachedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor.PreprocessorConfig


class CachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    val preprocessorConfig = PreprocessorConfig.empty
    new CachedNativeAIListIntervalJoin(NativeAIListIntervalJoin.Config(config, preprocessorConfig))
  }
}

