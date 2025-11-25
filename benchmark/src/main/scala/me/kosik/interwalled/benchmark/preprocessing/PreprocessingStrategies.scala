package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.benchmark.utils.Implicits._
import me.kosik.interwalled.spark.join.preprocessor.{Preprocessor, PreprocessorConfig}
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.deoutlier.DeoutlierConfig
import me.kosik.interwalled.spark.join.preprocessor.repartitioner.RepartitionerConfig
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

  private val deoutlierConfigs = Array(
    Some(DeoutlierConfig(900)),
    Some(DeoutlierConfig(950)),
    Some(DeoutlierConfig(990)),
    Some(DeoutlierConfig(995)),
    Some(DeoutlierConfig(999)),
    None
  )

  private val repartitionerConfigs = Array(
    Some(RepartitionerConfig(true)),
    Some(RepartitionerConfig(false))
  )

  val preprocessorConfigs: Array[PreprocessorConfig] = for {
    bucketingConfig     <- bucketingConfigs
    saltingConfig       <- saltingConfigs
    deoutlierConfig     <- deoutlierConfigs
    repartitionerConfig <- repartitionerConfigs
  } yield PreprocessorConfig(bucketingConfig, saltingConfig, deoutlierConfig, repartitionerConfig)


  val values: Map[String, Preprocessor] = {
    preprocessorConfigs
      .map(new Preprocessor(_))
      .map(preprocessor => preprocessor.toString -> preprocessor)
      .toMap
  }
}
