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
      case BenchmarkSuccess(timeElapsed, resultsRowCount, _) =>
        Seq("true", timeElapsed.milliseconds.toString, resultsRowCount.toString, "")

      case BenchmarkFailure(Some(timeElapsed), Some(resultsRowCount), error) =>
        Seq("false", timeElapsed.milliseconds.toString, resultsRowCount.toString, error.getMessage.split(",").head)

      case BenchmarkFailure(None, None, error) =>
        Seq("false", "", "", error.getMessage.split(",").head)

      case anyOther =>
        Seq("false", "", "", "Unknown outcome: " + anyOther.toString.replaceAll(",", ";"))
    }}

    fields.mkString(DELIMITER) + "\n"
  }
}
