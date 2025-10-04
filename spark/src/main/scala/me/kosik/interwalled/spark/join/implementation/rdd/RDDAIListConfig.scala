package me.kosik.interwalled.spark.join.implementation.rdd

import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ExecutorConfig
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


case class RDDAIListConfig(aiListConfig: AIListConfig, override val preprocessorConfig: PreprocessorConfig)
  extends ExecutorConfig with Serializable
