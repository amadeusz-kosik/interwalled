package me.kosik.interwalled.spark.join.preprocessor.generic

import me.kosik.interwalled.ailist.model.IntervalsPair
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.preprocessor.Bucketizer.BucketizerConfig
import me.kosik.interwalled.spark.join.preprocessor.Repartitioner.RepartitionerConfig
import me.kosik.interwalled.spark.join.preprocessor.Salter.SalterConfig
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import me.kosik.interwalled.spark.join.preprocessor.{Bucketizer, Repartitioner, Salter}
import org.apache.spark.sql.Dataset


class Preprocessor(config: PreprocessorConfig) extends Serializable {

  private lazy val transformers: Transformers = Transformers(Seq(
    OptionalTransformer(config.bucketizerConfig.map(new Bucketizer(_))),
    OptionalTransformer(config.salterConfig.map(new Salter(_))),
    OptionalTransformer(config.repartitionerConfig.map(new Repartitioner(_)))
  ))

  override def toString: String = {
    Array(config.bucketizerConfig, config.salterConfig)
      .flatten
      .map(_.toString + "-")
      .mkString
  }

  def prepareInput(input: PreparedInput): PreparedInput =
    transformers(input)

  def finalizeResult(results: Dataset[IntervalsPair]): Dataset[IntervalsPair] =
    transformers(results)
}

object Preprocessor {
  trait PreprocessorStep extends Serializable {
    def processInput(input: PreparedInput): PreparedInput =
      input

    def processResult(result: Dataset[IntervalsPair]): Dataset[IntervalsPair] =
      result
  }

  case class PreprocessorConfig(
    bucketizerConfig:     Option[BucketizerConfig],
    salterConfig:         Option[SalterConfig],
    repartitionerConfig:  Option[RepartitionerConfig]
  )

  object PreprocessorConfig {
    def empty: PreprocessorConfig =
      PreprocessorConfig(None, None, None)
  }
}

