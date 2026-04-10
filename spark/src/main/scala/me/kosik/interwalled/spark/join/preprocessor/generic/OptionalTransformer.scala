package me.kosik.interwalled.spark.join.preprocessor.generic

import me.kosik.interwalled.ailist.model.IntervalsPair
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput}
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorStep
import org.apache.spark.sql.Dataset


sealed trait OptionalTransformer {
  def apply(input: PreparedInput): PreparedInput
  def apply(input: Dataset[IntervalsPair]): Dataset[IntervalsPair]
}

object OptionalTransformer {
  def apply(maybeTransformer: Option[PreprocessorStep]): OptionalTransformer = maybeTransformer match {
    case Some(transformer) =>
      SomeTransformer(transformer)

    case None =>
      NoneTransformer
  }

  private case class SomeTransformer(transformer: PreprocessorStep) extends OptionalTransformer {
    override def apply(input: PreparedInput): PreparedInput =
      transformer.processInput(input)

    override def apply(result: Dataset[IntervalsPair]): Dataset[IntervalsPair] =
      transformer.processResult(result)
  }

  private case object NoneTransformer extends OptionalTransformer {
    override def apply(input: PreparedInput): PreparedInput =
      input

    override def apply(result: Dataset[IntervalsPair]): Dataset[IntervalsPair] =
      result
  }
}


