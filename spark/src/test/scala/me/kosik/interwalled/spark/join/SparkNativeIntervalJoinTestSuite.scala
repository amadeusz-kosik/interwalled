package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.spark.native.{SparkNativeConfig, SparkNativeIntervalJoin}
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig


class SparkNativeIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val preprocessorConfig = PreprocessorConfig.empty
    new SparkNativeIntervalJoin(SparkNativeConfig(preprocessorConfig))
  }
}
