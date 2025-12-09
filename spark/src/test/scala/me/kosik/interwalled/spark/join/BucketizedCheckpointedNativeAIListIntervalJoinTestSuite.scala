package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.implementation.ailist.native.ailist.CheckpointedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig


class BucketizedCheckpointedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    val preprocessorConfig = PreprocessorConfig.empty.copy(bucketizerConfig = Some(BucketizerConfig(1000)))
    new CheckpointedNativeAIListIntervalJoin(NativeAIListIntervalJoin.Config(config, preprocessorConfig), "temporary/interwalled-chkpt")
  }
}

