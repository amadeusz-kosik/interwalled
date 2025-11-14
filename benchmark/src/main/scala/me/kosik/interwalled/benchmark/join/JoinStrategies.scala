package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.preprocessing.PreprocessingStrategies
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.NativeAIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.implementation.{CachedNativeAIListIntervalJoin, CheckpointedNativeAIListIntervalJoin}
import me.kosik.interwalled.spark.join.implementation.driver.DriverAIListIntervalJoin
import me.kosik.interwalled.spark.join.implementation.rdd.{RDDAIListConfig, RDDAIListIntervalJoin}
import me.kosik.interwalled.spark.join.implementation.spark.native.{SparkNativeConfig, SparkNativeIntervalJoin}
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


object JoinStrategies {

  private val checkpointDir = "/mnt/temporary/checkpoint.parquet"

  private val aiListConfigs    = for {
    maximumComponentsCount              <- Array(10)
    intervalsCountToCheckLookaheadRatio <- Array(2.0)
    intervalsCountToTriggerExtraction   <- Array(10)
    intervalsCountToCheckLookahead       = (intervalsCountToCheckLookaheadRatio * intervalsCountToTriggerExtraction).toInt
    minimumComponentSize                <- Array(64)
  } yield AIListConfig(maximumComponentsCount, intervalsCountToCheckLookahead, intervalsCountToTriggerExtraction, minimumComponentSize)

  private val preprocessingConfigs = PreprocessingStrategies.preprocessorConfigs

  // -------------------------------------------------------------------------------------------------------------------

  private val cachedAIListIntervalJoin: Array[IntervalJoin] = {
    for {
      aiListConfig        <- aiListConfigs
      config = NativeAIListConfig(
        aiListConfig        = aiListConfig,
        preprocessorConfig  = PreprocessorConfig.empty
      )
    } yield new CachedNativeAIListIntervalJoin(config)
  }

  private val checkpointedAIListIntervalJoin: Array[IntervalJoin] = {
    for {
      aiListConfig    <- aiListConfigs
      config = NativeAIListConfig(
        aiListConfig        = aiListConfig,
        preprocessorConfig  = PreprocessorConfig.empty
      )
    } yield new CheckpointedNativeAIListIntervalJoin(config, checkpointDir)
  }

  private val rddAIListIntervalJoin: Array[IntervalJoin] = {
    for {
      aiListConfig        <- aiListConfigs
      preprocessingConfig <- preprocessingConfigs
        config = RDDAIListConfig(
        aiListConfig        = aiListConfig,
        preprocessorConfig  = preprocessingConfig
      )
    } yield new RDDAIListIntervalJoin(config)
  }

  private val sparkNativeIntervalJoin: Array[IntervalJoin] = {
    Array(new SparkNativeIntervalJoin(SparkNativeConfig(PreprocessorConfig.empty)))
  }

  val values: Map[String, IntervalJoin] = {
    (
      Array(DriverAIListIntervalJoin)
        ++ cachedAIListIntervalJoin
        ++ checkpointedAIListIntervalJoin
        ++ rddAIListIntervalJoin
        ++ sparkNativeIntervalJoin
    ).map(join => join.toString -> join).toMap
  }
}
