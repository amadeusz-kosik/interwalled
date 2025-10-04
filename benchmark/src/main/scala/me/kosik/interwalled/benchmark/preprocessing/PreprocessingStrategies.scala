package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.benchmark.utils.Implicits._
import me.kosik.interwalled.spark.join.preprocessor.{Preprocessor, PreprocessorConfig}
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.salter.SalterConfig

import scala.language.postfixOps


object PreprocessingStrategies {

  private val bucketingConfigs = Array(
    Some(BucketizerConfig(  1L K)),
    Some(BucketizerConfig(  1L M)),
    Some(BucketizerConfig(  1L B)),
    None
  )

  private val saltingConfigs = Array(
    Some(SalterConfig(  1L M)),
    Some(SalterConfig( 10L M)),
    Some(SalterConfig(100L M)),
    Some(SalterConfig(  1L B)),
    None
  )

  val preprocessorConfigs: Array[PreprocessorConfig] = for {
    bucketingConfig <- bucketingConfigs
    saltingConfig   <- saltingConfigs
  } yield PreprocessorConfig(bucketingConfig, saltingConfig)


  val values: Map[String, Preprocessor] = {
    preprocessorConfigs
      .map(new Preprocessor(_))
      .map(preprocessor => preprocessor.toString -> preprocessor)
      .toMap
  }
}
