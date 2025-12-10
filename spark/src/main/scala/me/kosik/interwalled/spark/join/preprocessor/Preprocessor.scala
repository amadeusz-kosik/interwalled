package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import me.kosik.interwalled.spark.join.preprocessor.Bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.Deoutlier.DeoutlierConfig
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.Repartitioner.RepartitionerConfig
import me.kosik.interwalled.spark.join.preprocessor.Salter.SalterConfig
import me.kosik.interwalled.utility.OptionalTransformer


class Preprocessor(config: PreprocessorConfig) extends Serializable {

  private lazy val bucketizer: OptionalTransformer =
    OptionalTransformer(config.bucketizerConfig.map(new Bucketizer(_)))

  private lazy val salter: OptionalTransformer =
    OptionalTransformer(config.salterConfig.map(new Salter(_)))

  private lazy val deoutlier: OptionalTransformer =
    OptionalTransformer(config.deoutlierConfig.map(new Deoutlier(_)))

  private lazy val repartitioner: OptionalTransformer =
    OptionalTransformer(config.repartitionerConfig.map(new Repartitioner(_)))


  override def toString: String = {
    Array(config.bucketizerConfig, config.salterConfig, config.deoutlierConfig)
      .flatten
      .map(_.toString + "-")
      .mkString
  }

  def prepareInput(input: PreparedInput): PreparedInput = {
    val stepBucketizer    = bucketizer(input)
    val stepSalter        = salter(stepBucketizer)
    val stepDeoutlier     = deoutlier(stepSalter)
    val stepRepartitioner = repartitioner(stepDeoutlier)

    stepRepartitioner
  }

  def finalizeResult(results: Result): Result = {
    val stepBucketizer = bucketizer(results)
    stepBucketizer
  }
}

object Preprocessor {
  trait PreprocessorStep extends Serializable {
    def processInput(input: PreparedInput): PreparedInput =
      input

    def processResult(result: Result): Result =
      result
  }

  case class PreprocessorConfig(
    bucketizerConfig:     Option[BucketizerConfig],
    salterConfig:         Option[SalterConfig],
    deoutlierConfig:      Option[DeoutlierConfig],
    repartitionerConfig:  Option[RepartitionerConfig]
  )

  object PreprocessorConfig {

    def empty: PreprocessorConfig =
      PreprocessorConfig(None, None, None, None)
  }
}

