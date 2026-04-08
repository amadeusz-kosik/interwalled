package me.kosik.interwalled.benchmark.common.results

import me.kosik.interwalled.benchmark.common.results.model.{BenchmarkFailure, BenchmarkOutcome, BenchmarkSuccess}


sealed trait CSVFormatter[T] {
  protected val DELIMITER = ","

  def header: String

  def row(row: T): String
}

object BenchmarkOutcomeCSVFormatter extends CSVFormatter[BenchmarkOutcome[_]] {

  override def header: String = {
    Seq("benchmark_name", "data_suite", "is_success", "time_elapsed", "result_rows_count", "error_message")
      .mkString(DELIMITER) + "\n"
  }

  override def row(outcome: BenchmarkOutcome[_]): String = {
    val fields: Seq[String] = Seq(
      outcome.benchmarkName,
      outcome.dataSuiteMetadata.suite
    ) ++ { outcome.result match {
      case BenchmarkSuccess(timeElapsed, _, resultsRowCount)  => Seq(timeElapsed.toString, resultsRowCount.toString, "")
      case BenchmarkFailure(error)                            => Seq("", "", error.getMessage.split(",").head)
    }}

    fields.mkString(DELIMITER) + "\n"
  }
}
