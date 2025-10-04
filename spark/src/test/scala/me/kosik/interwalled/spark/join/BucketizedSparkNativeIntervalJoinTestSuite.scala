package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.spark.native.SparkNativeIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


class BucketizedSparkNativeIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new SparkNativeIntervalJoin(Some(PreprocessorConfig(1000)))
}
