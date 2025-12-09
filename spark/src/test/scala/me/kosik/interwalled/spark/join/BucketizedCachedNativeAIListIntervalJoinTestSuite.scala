package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.implementation.ailist.native.ailist.CachedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig


class BucketizedCachedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    val preprocessorConfig = PreprocessorConfig.empty.copy(bucketizerConfig = Some(BucketizerConfig(1000)))
    new CachedNativeAIListIntervalJoin(NativeAIListIntervalJoin.Config(config, preprocessorConfig))
  }
}

