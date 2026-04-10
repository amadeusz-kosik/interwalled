package me.kosik.interwalled.spark.join.preprocessor.generic

import me.kosik.interwalled.ailist.model.IntervalsPair
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput}
import org.apache.spark.sql.Dataset


case class Transformers(steps: Seq[OptionalTransformer]) {

  def apply(input: PreparedInput): PreparedInput =
    steps.foldLeft(input)((input, transformer) => transformer(input))

  def apply(input: Dataset[IntervalsPair]): Dataset[IntervalsPair] =
    steps.foldRight(input)((transformer, input) => transformer(input))

  override def toString: String =
    steps.map(_.toString).mkString("-")
}
