package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig


trait ExecutorConfig {
  def preprocessorConfig: PreprocessorConfig
}
