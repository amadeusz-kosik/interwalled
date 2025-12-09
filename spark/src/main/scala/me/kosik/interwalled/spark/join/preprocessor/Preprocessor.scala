package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.ailist.IntervalColumns
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import me.kosik.interwalled.spark.join.preprocessor.bucketizer.Bucketizer
import me.kosik.interwalled.spark.join.preprocessor.deoutlier.Deoutlier
import me.kosik.interwalled.spark.join.preprocessor.repartitioner.Repartitioner
import me.kosik.interwalled.spark.join.preprocessor.salter.Salter
import me.kosik.interwalled.utility.OptionalTransformer
import org.apache.spark.sql.{Dataset, functions => F}


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


trait PreprocessorStep extends Serializable {
  def processInput(input: PreparedInput): PreparedInput =
    input

  def processResult(result: Result): Result =
    result
}