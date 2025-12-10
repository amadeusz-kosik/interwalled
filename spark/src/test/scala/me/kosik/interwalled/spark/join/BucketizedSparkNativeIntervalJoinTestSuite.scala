package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.SparkNativeIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.Bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor.PreprocessorConfig


class BucketizedSparkNativeIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = {
    val preprocessorConfig = PreprocessorConfig.empty.copy(bucketizerConfig = Some(BucketizerConfig(1000)))
    new SparkNativeIntervalJoin(SparkNativeIntervalJoin.Config(preprocessorConfig))
  }
}
