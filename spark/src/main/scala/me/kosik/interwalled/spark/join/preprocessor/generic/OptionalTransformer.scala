package me.kosik.interwalled.spark.join.preprocessor.generic

import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorStep


sealed trait OptionalTransformer {
  def apply(input: PreparedInput): PreparedInput
  def apply(input: Result): Result
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

    override def apply(result: Result): Result =
      transformer.processResult(result)
  }

  private case object NoneTransformer extends OptionalTransformer {
    override def apply(input: PreparedInput): PreparedInput =
      input

    override def apply(result: Result): Result =
      result
  }
}


