package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig

trait ExecutorConfig {
  def preprocessorConfig: PreprocessorConfig
}
