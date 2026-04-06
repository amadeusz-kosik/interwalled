package me.kosik.interwalled.benchmark.common.results

import me.kosik.interwalled.benchmark.common.results.model.{BenchmarkFailure, BenchmarkOutcome, BenchmarkResult, BenchmarkSuccess}

import scala.util.{Failure, Success}


sealed trait CSVFormatter[T] {
  protected val DELIMITER = ","

  def header: String

  def row(row: T): String
}

object BenchmarkOutcomeCSVFormatter extends CSVFormatter[BenchmarkOutcome] {

  override def header: String = {
    Seq("benchmark_name", "data_suite", "is_success", "time_elapsed", "result_rows_count", "error_message")
      .mkString(DELIMITER) + "\n"
  }

  override def row(outcome: BenchmarkOutcome): String = {
    val fields: Seq[String] = Seq(
      outcome.benchmarkName,
      outcome.dataSuiteName
    ) ++ { outcome.result match {
      case BenchmarkSuccess(timeElapsed, resultsRowCount) => Seq(timeElapsed.toString, resultsRowCount.toString, "")
      case BenchmarkFailure(error)                        => Seq("", "", error.getMessage.split(",").head)
    }}

    fields.mkString(DELIMITER) + "\n"
  }
}
