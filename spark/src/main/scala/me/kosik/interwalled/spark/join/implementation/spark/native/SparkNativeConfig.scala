package me.kosik.interwalled.spark.join.implementation.spark.native

import me.kosik.interwalled.spark.join.implementation.ExecutorConfig
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig


case class SparkNativeConfig(override val preprocessorConfig: PreprocessorConfig)
    extends ExecutorConfig
