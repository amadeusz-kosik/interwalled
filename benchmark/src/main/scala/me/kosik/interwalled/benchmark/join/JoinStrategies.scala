package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.{DriverAIListIntervalJoin, NativeAIListIntervalJoin, RDDAIListIntervalJoin, SparkNativeIntervalJoin}
import me.kosik.interwalled.spark.join.implementation.ailist.native.ailist.{CachedNativeAIListIntervalJoin, CheckpointedNativeAIListIntervalJoin}
import me.kosik.interwalled.spark.join.preprocessor.Bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.Repartitioner.RepartitionerConfig
import me.kosik.interwalled.spark.join.preprocessor.Salter.SalterConfig
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig


object JoinStrategies {

  private val checkpointDir = "/mnt/temporary/checkpoint.parquet"

  private val defaultAIListConfig = AIListConfig(
    // Default values:
    //    maximumComponentsCount            = 10,
    //    intervalsCountToCheckLookahead    = 20,
    //    intervalsCountToTriggerExtraction = 10,
    //    minimumComponentSize              = 64
  )

  private val emptyPreprocessorConfig = PreprocessorConfig.empty

  private val bucketPer1000PreprocessorConfig = emptyPreprocessorConfig.copy(
    bucketizerConfig    = Some(BucketizerConfig(1000L)),
    repartitionerConfig = Some(RepartitionerConfig(true))
  )

  private val bucketPer1000000PreprocessorConfig = emptyPreprocessorConfig.copy(
    bucketizerConfig    = Some(BucketizerConfig(1000000L)),
    repartitionerConfig = Some(RepartitionerConfig(true))
  )

  private val saltPer1000000PreprocessorConfig = emptyPreprocessorConfig.copy(
    salterConfig        = Some(SalterConfig(1000000L)),
    repartitionerConfig = Some(RepartitionerConfig(true))
  )

  private val saltPer10000000PreprocessorConfig = emptyPreprocessorConfig.copy(
    salterConfig        = Some(SalterConfig(10000000L)),
    repartitionerConfig = Some(RepartitionerConfig(true))
  )


  // -------------------------------------------------------------------------------------------------------------------

  val values: Map[String, IntervalJoin] = {
      Array(
        // First benchmark - choosing the join algorithm.

        new CachedNativeAIListIntervalJoin(
          NativeAIListIntervalJoin.Config(defaultAIListConfig, emptyPreprocessorConfig)
        ),

        new CheckpointedNativeAIListIntervalJoin(
          NativeAIListIntervalJoin.Config(defaultAIListConfig, emptyPreprocessorConfig),
          checkpointDir
        ),

        DriverAIListIntervalJoin,

        new RDDAIListIntervalJoin(
          RDDAIListIntervalJoin.Config(defaultAIListConfig, emptyPreprocessorConfig)
        ),

        new SparkNativeIntervalJoin(
          SparkNativeIntervalJoin.Config(emptyPreprocessorConfig)
        ),

        // Second benchmark - choosing correct preprocessing options
        new RDDAIListIntervalJoin(
          RDDAIListIntervalJoin.Config(defaultAIListConfig, bucketPer1000PreprocessorConfig)
        ),

        new RDDAIListIntervalJoin(
          RDDAIListIntervalJoin.Config(defaultAIListConfig, bucketPer1000000PreprocessorConfig)
        ),

        new RDDAIListIntervalJoin(
          RDDAIListIntervalJoin.Config(defaultAIListConfig, saltPer1000000PreprocessorConfig)
        ),

        new RDDAIListIntervalJoin(
          RDDAIListIntervalJoin.Config(defaultAIListConfig, saltPer10000000PreprocessorConfig)
        )
    ).map(join => join.toString -> join).toMap
  }
}
