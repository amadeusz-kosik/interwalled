package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.SparkNativeIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig


class SparkNativeIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val preprocessorConfig = PreprocessorConfig.empty
    new SparkNativeIntervalJoin(SparkNativeIntervalJoin.Config(preprocessorConfig))
  }
}
