package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.deoutlier.DeoutlierConfig
import me.kosik.interwalled.spark.join.preprocessor.salter.SalterConfig


case class PreprocessorConfig(
  bucketizerConfig: Option[BucketizerConfig],
  salterConfig:     Option[SalterConfig],
  deoutlierConfig:  Option[DeoutlierConfig]
)

object PreprocessorConfig {

  def empty: PreprocessorConfig =
    PreprocessorConfig(None, None, None)
}