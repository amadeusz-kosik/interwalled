package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.spark.join.preprocessor.Preprocessor.PreprocessorConfig

trait ExecutorConfig {
  def preprocessorConfig: PreprocessorConfig
}
