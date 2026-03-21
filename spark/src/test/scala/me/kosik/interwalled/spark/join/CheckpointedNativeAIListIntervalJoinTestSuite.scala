package me.kosik.interwalled.spark.join

import me.kosik.interwalled.ailist.model.AIListConfiguration
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.NativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.implementation.ailist.native.ailist.CheckpointedNativeAIListIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig


class CheckpointedNativeAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val config = AIListConfiguration.DEFAULT
    val preprocessorConfig = PreprocessorConfig.empty
    new CheckpointedNativeAIListIntervalJoin(NativeAIListIntervalJoin.Config(config, preprocessorConfig), "temporary/interwalled-chkpt")
  }
}

