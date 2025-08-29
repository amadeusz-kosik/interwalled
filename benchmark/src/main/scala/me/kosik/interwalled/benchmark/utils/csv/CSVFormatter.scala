package me.kosik.interwalled.benchmark.utils.csv

import me.kosik.interwalled.benchmark.utils.BenchmarkResult
import me.kosik.interwalled.spark.join.api.model.IntervalStatistics

import scala.util.{Failure, Success}


object CSVFormatter {

  private val DELIMITER = ","

  def header: String = {
    Seq(
      "data_suite", "join_name", "elapsed_time", "result",
      "database_raw_rows_count", "database_final_rows_count",
      "query_raw_rows_count", "query_final_rows_count",
      "result_raw_rows_count", "result_final_rows_count",
    )
    .mkString(DELIMITER) + "\n"
  }

  def row(result: BenchmarkResult): String = {
    val coreFields: Seq[String] = Seq(
      result.dataSuite.suite,
      result.join.toString
    )

    val resultFields: Seq[String] = result.result match {
      case Success(elapsedTime) =>
        Seq(elapsedTime.milliseconds.toString, "success")

      case Failure(exception) =>
        Seq("", exception.getMessage.split("\n").head)
    }

    val statistics: Seq[String] = result.statistics match {
      case Some(IntervalStatistics(database, query, result)) => Seq(
        database.rawRowsCount,
        database.finalRowsCount,
        query.rawRowsCount,
        query.finalRowsCount,
        result.rawRowsCount,
        result.finalRowsCount
      ).map(_.toString)

      case None => Seq.empty[String]
    }

    (coreFields ++ resultFields ++ statistics).mkString(DELIMITER) + "\n"
  }

}
