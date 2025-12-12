package me.kosik.interwalled.spark.join.preprocessor.generic

import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}


case class Transformers(steps: Seq[OptionalTransformer]) {

  def apply(input: PreparedInput): PreparedInput =
    steps.foldLeft(input)((input, transformer) => transformer(input))

  def apply(input: Result): Result =
    steps.foldRight(input)((transformer, input) => transformer(input))

  override def toString: String =
    steps.map(_.toString).mkString("-")
}
