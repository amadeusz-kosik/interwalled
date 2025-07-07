package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.SparkNativeIntervalJoin
import me.kosik.interwalled.utility.bucketizer.BucketScale


class BucketizedSparkNativeIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = new SparkNativeIntervalJoin(Some(BucketScale(10)))
}
