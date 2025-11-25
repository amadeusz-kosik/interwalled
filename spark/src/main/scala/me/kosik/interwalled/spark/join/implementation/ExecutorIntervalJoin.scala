package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.domain.IntervalsPair
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.{PreparedInput, Result}
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor
import org.apache.spark.sql.Dataset


trait ExecutorIntervalJoin extends IntervalJoin {

  protected def config: ExecutorConfig

  protected def name: String

  private lazy val preprocessor: Preprocessor = new Preprocessor(config.preprocessorConfig)

  override def toString: String =
    preprocessor.toString + name

  override protected def prepareInput(input: PreparedInput): PreparedInput =
    preprocessor.prepareInput(input)

  override protected def finalizeResult(results: Result): Result =
    preprocessor.finalizeResult(results)
}
