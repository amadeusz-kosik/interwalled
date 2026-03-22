package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.model.AIListConfiguration
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.implementation.ailist.native.ailist.CachedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig


class CachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfiguration.apply
    val preprocessorConfig = PreprocessorConfig.empty
    new CachedNativeAIListIntervalJoin(NativeAIListIntervalJoin.Config(config, preprocessorConfig))
  }
}

