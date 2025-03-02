package me.kosik.interwalled.benchmark.utils

import scala.util.{Failure, Success}


object CSV {

  private val DELIMITER = ","

  def header: String = {
    Seq("data_suite", "clusters_count", "rows_per_cluster", "join_name", "elapsed_time", "result")
      .mkString(DELIMITER) + "\n"
  }

  def row(result: BenchmarkResult): String = {
    Seq(
      result.dataSuite,
      result.clustersCount,
      result.rowsPerCluster,
      result.joinName,
      result.elapsedTime.milliseconds,
      result.result match {
        case Success(_) => "success"
        case Failure(exception) => exception.getMessage.split("\n").head
      }
    ).mkString(DELIMITER) + "\n"
  }

}
