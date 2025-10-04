package me.kosik.interwalled.spark.join.implementation.ailist

import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ExecutorConfig
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


case class NativeAIListConfig(aiListConfig: AIListConfig, override val preprocessorConfig:  PreprocessorConfig)
  extends ExecutorConfig
