package me.kosik.interwalled.benchmark.utils.csv

import me.kosik.interwalled.benchmark.join.BenchmarkResult
import me.kosik.interwalled.benchmark.preprocessing.PreprocessingResult
import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats

import scala.util.{Failure, Success}


sealed trait CSVFormatter[T] {
  protected val DELIMITER = ","

  def header: String

  def row(row: T): String
}

object BenchmarkCSVFormatter extends CSVFormatter[BenchmarkResult] {

  override def header: String = {
    Seq("data_suite", "join_name", "elapsed_time", "result", "result_rows_count")
      .mkString(DELIMITER) + "\n"
  }

  override def row(result: BenchmarkResult): String = {
    val coreFields: Seq[String] = Seq(
      result.dataSuite.suite,
      result.join.toString
    )

    val resultFields: Seq[String] = result.result match {
      case Success(elapsedTime) =>
        Seq(elapsedTime.milliseconds.toString, "success")

      case Failure(exception) =>
        Seq("", exception.getMessage.split("\n").head.replace(",", ";"))
    }

    val statistics: Seq[String] = result.statistics match {
      case Some(IntervalJoinRunStats(resultRowsCount)) =>
        Seq(resultRowsCount).map(_.toString)

      case None =>
        Seq.empty[String]
    }

    (coreFields ++ resultFields ++ statistics).mkString(DELIMITER) + "\n"
  }
}

object PreprocessingCSVFormatter extends CSVFormatter[PreprocessingResult] {

  override def header: String = {
    Seq("data_suite", "preprocessor", "lhs_rows_count", "lhs_rows_per_partition", "rhs_rows_count", "rhs_rows_per_partition")
      .mkString(DELIMITER) + "\n"
  }

  override def row(result: PreprocessingResult): String = {
    Array(
      result.dataSuite,
      result.preprocessor,
      result.lhsRowsCount.toString,
      result.lhsRowsPerPartition.mkString("-"),
      result.rhsRowsCount.toString,
      result.rhsRowsPerPartition.mkString("-")
    ).mkString(DELIMITER) + "\n"
  }
}
