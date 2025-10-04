package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.salter.SalterConfig


case class PreprocessorConfig(
  bucketizerConfig: Option[BucketizerConfig],
  salterConfig:     Option[SalterConfig]
)