package me.kosik.interwalled.benchmark.data

import me.kosik.interwalled.domain.Interval
import me.kosik.interwalled.domain.IntervalColumns
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.slf4j.LoggerFactory


object TestDataLoader {
  lazy private val logger = LoggerFactory.getLogger(getClass)

  def load(suite: String, databasePath: String, queryPath: String)(implicit sparkSession: SparkSession): TestData = {
    val database = load(databasePath)
    val query    = load(queryPath)

    TestData(suite, database, query)
  }

  private def load(datasetPath: String)(implicit sparkSession: SparkSession): TestData.TestDataset = {
    import sparkSession.implicits._
    import IntervalColumns._

    val datasetPathPrefix: String = datasetPath.split("/")(1)

    val dataset = datasetPathPrefix match {
      case "edge" =>
        logger.info(s"Loading edge case data: $datasetPath")

        sparkSession
          .read.parquet(datasetPath)
          .as[Interval[String]]

      case "databio-8p" =>
        logger.info(s"Loading databio-8p data: $datasetPath")

        sparkSession
          .read.parquet(datasetPath)
          .select(
            F.col("contig")   .cast("STRING").as(KEY),
            F.col("pos_start").cast("LONG").as(FROM),
            F.col("pos_end")  .cast("LONG").as(TO),
            F.lit("").as(VALUE)
          )
          .as[Interval[String]]

      case _ =>
        logger.warn(s"Unknown dataset path prefix: $datasetPathPrefix for $datasetPath, please update TestDataLoader.")

        sparkSession
          .read.parquet(datasetPath)
          .as[Interval[String]]

    }

    TestData.TestDataset(datasetPath, dataset)
  }
}
