package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.domain.{IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.Bucketizer
import me.kosik.interwalled.spark.join.preprocessor.salter.Salter
import me.kosik.interwalled.utility.OptionalTransformer
import org.apache.spark.sql.{Dataset, functions => F}


class Preprocessor(config: PreprocessorConfig) extends Serializable {

  private lazy val bucketizer: OptionalTransformer[Bucketizer] =
    OptionalTransformer(config.bucketizerConfig.map(new Bucketizer(_)))

  private lazy val salter: OptionalTransformer[Salter] =
    OptionalTransformer(config.salterConfig.map(new Salter(_)))


  override def toString: String = {
    Array(config.bucketizerConfig, config.salterConfig)
      .flatten
      .map(_.toString + "-")
      .mkString
  }

  def prepareInput(input: PreparedInput): PreparedInput = {
    import IntervalColumns._

    val stepBucketizer  = bucketizer(input)(t => t.processInput)
    val stepSalter      = salter(stepBucketizer)(t => t.processInput)

    stepSalter.repartition(F.col(BUCKET), F.col(KEY))
  }

  def finalizeResult(results: Dataset[IntervalsPair]): Dataset[IntervalsPair] = {
    val stepBucketizer  = bucketizer(results)(t => t.processOutput)
    stepBucketizer
  }
}
