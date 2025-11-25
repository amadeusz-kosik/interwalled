package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.NativeAIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.implementation.CheckpointedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig


class BucketizedCheckpointedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    val preprocessorConfig = PreprocessorConfig.empty.copy(bucketizerConfig = Some(BucketizerConfig(1000)))
    new CheckpointedNativeAIListIntervalJoin(NativeAIListConfig(config, preprocessorConfig), "temporary/interwalled-chkpt")
  }
}

