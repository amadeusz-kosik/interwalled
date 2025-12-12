package me.kosik.interwalled.spark.join.preprocessor.generic

import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import me.kosik.interwalled.spark.join.preprocessor.Bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.Deoutlier.DeoutlierConfig
import me.kosik.interwalled.spark.join.preprocessor.MinMaxPruner.MinMaxPrunerConfig
import me.kosik.interwalled.spark.join.preprocessor.Repartitioner.RepartitionerConfig
import me.kosik.interwalled.spark.join.preprocessor.Salter.SalterConfig
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.{Bucketizer, Deoutlier, MinMaxPruner, Repartitioner, Salter}


class Preprocessor(config: PreprocessorConfig) extends Serializable {

  private lazy val transformers: Transformers = Transformers(Seq(
    OptionalTransformer(config.bucketizerConfig.map(new Bucketizer(_))),
    OptionalTransformer(config.salterConfig.map(new Salter(_))),
    OptionalTransformer(config.deoutlierConfig.map(new Deoutlier(_))),
    OptionalTransformer(config.repartitionerConfig.map(new Repartitioner(_))),
    OptionalTransformer(config.minMaxPrunerConfig.map(new MinMaxPruner(_)))
  ))

  override def toString: String = {
    Array(config.bucketizerConfig, config.salterConfig, config.deoutlierConfig)
      .flatten
      .map(_.toString + "-")
      .mkString
  }

  def prepareInput(input: PreparedInput): PreparedInput =
    transformers(input)

  def finalizeResult(results: Result): Result =
    transformers(results)
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
    repartitionerConfig:  Option[RepartitionerConfig],
    minMaxPrunerConfig:   Option[MinMaxPrunerConfig]
  )

  object PreprocessorConfig {
    def empty: PreprocessorConfig =
      PreprocessorConfig(None, None, None, None, None)
  }
}

