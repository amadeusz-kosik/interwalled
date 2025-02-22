package me.kosik.interwalled.benchmark.utils

import scala.util.{Failure, Success}


object CSV {

  private val DELIMITER = ","

  def toCSV(rows: Seq[BenchmarkResult]): Seq[String] = {
    val header = Seq("data_suite", "clusters_count", "rows_per_cluster", "join_name", "elapsed_time", "result")
    val body = rows
      .map { result => Seq(
        result.dataSuite,
        result.clustersCount,
        result.rowsPerCluster,
        result.joinName,
        result.elapsedTime.milliseconds,
        result.result match {
          case Success(_) => "success"
          case Failure(exception) => exception.getMessage.split("\n").head
        }
      )}

    Seq(header.mkString(DELIMITER)) ++ body.map(_.mkString(DELIMITER))
  }
}
