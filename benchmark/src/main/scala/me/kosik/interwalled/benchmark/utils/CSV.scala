package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.spark.join.api.model.IntervalStatistics

import scala.util.{Failure, Success}


object CSV {

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
    val coreFields = Seq(
      result.dataSuite,
      result.joinName
    )

    val resultFields = result.result match {
      case Success(elapsedTime) =>
        Seq(elapsedTime.milliseconds, "success")

      case Failure(exception) =>
        Seq("", exception.getMessage.split("\n").head)
    }

    val statistics = result.statistics match {
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
