package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.{CachedNativeAIListIntervalJoin, CheckpointedNativeAIListIntervalJoin}
import me.kosik.interwalled.spark.join.implementation.{DriverAIListIntervalJoin, RDDAIListIntervalJoin, SparkNativeIntervalJoin}
import me.kosik.interwalled.utility.bucketizer.{BucketCount, BucketScale}


object JoinStrategies {

  val values: Map[String, IntervalJoin] = {
    val checkpointDir = "temporary/checkpoint/"

    Array(
      new CachedNativeAIListIntervalJoin(AIListConfig(10,  20, 10), None),
      new CachedNativeAIListIntervalJoin(AIListConfig(10,  80, 40), None),
      new CachedNativeAIListIntervalJoin(AIListConfig(10, 320, 160), None),

      new CheckpointedNativeAIListIntervalJoin(checkpointDir, AIListConfig(10,  20, 10),  None),
      new CheckpointedNativeAIListIntervalJoin(checkpointDir, AIListConfig(10,  80, 40),  None),
      new CheckpointedNativeAIListIntervalJoin(checkpointDir, AIListConfig(10, 320, 160), None),

      new CheckpointedNativeAIListIntervalJoin(checkpointDir, AIListConfig(10, 20, 10), Some(BucketScale(2))),
      new CheckpointedNativeAIListIntervalJoin(checkpointDir, AIListConfig(10, 20, 10), Some(BucketScale(4))),
      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketScale( 2))),
      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketScale( 4))),
      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketScale( 8))),
      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketScale(16))),
      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketScale(32))),

      DriverAIListIntervalJoin

//      new RDDAIListIntervalJoin(bucketingConfig = None),
//      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketCount(100L))),
//      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketCount(1000L))),
//      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketCount(10000L))),
//      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketCount(100000L))),
//      new RDDAIListIntervalJoin(bucketingConfig = Some(BucketCount(1000000L))),
//
//      new SparkNativeIntervalJoin(bucketingConfig = None),
//      new SparkNativeIntervalJoin(bucketingConfig = Some(BucketCount(10L))),
//      new SparkNativeIntervalJoin(bucketingConfig = Some(BucketCount(100L))),
//      new SparkNativeIntervalJoin(bucketingConfig = Some(BucketCount(1000L))),
//      new SparkNativeIntervalJoin(bucketingConfig = Some(BucketCount(10000L))),
//      new SparkNativeIntervalJoin(bucketingConfig = Some(BucketCount(100000L)))
    ).map(join => join.toString -> join).toMap
  }

}
