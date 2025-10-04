package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.CheckpointedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


class BucketizedCheckpointedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfig()
    new CheckpointedNativeAIListIntervalJoin("temporary/interwalled-chkpt", config, Some(PreprocessorConfig(1000)))
  }
}

