package me.kosik.interwalled.benchmark.common.test.data

import me.kosik.interwalled.benchmark.common.test.data.model.TestDataRow
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}


object TestDataSuiteLoader {

  def load(dataDirectory: String, suite: TestDataSuiteMetadata)(implicit sparkSession: SparkSession): TestDataSuite = {
    val databaseData = load(dataDirectory, suite.databasePaths, suite.limit)
    val queryData    = load(dataDirectory, suite.queryPaths, suite.limit)

    TestDataSuite(suite, databaseData, queryData)
  }

  private def load(dataDirectory: String, paths: DataPaths, limit: Option[Long])(implicit sparkSession: SparkSession): Dataset[TestDataRow] = {
    val loaders: Map[String, DataFrame => DataFrame] = Map(
      "databio-8p" -> { df =>

        df.select(
          F.col("contig").as("key"),
          F.col("pos_start").as("from"),
          F.col("pos_end").as("to"),
          F.uuid().as("value")
        )
      },

      "unit-test-data" -> { df =>
        df.select(
          F.lit("IW000").as("key"),
          F.col("from"),
          F.col("to"),
          F.col("value")
        )
      }
    )

    def doRead(path: String): Dataset[TestDataRow] = {
      import sparkSession.implicits._

      val inputPathPrefix = path.split("/")(0)
      val inputDataset = sparkSession.read
        .parquet(f"${dataDirectory}/$path")
        .transform(loaders.getOrElse(inputPathPrefix, throw new IllegalArgumentException("Unknown test data prefix.")))
        .as[TestDataRow]

      limit match {
        case Some(limitValue) =>
          inputDataset
            .filter(F.col("from") <= limitValue)
            .filter(F.col("to")   <= limitValue)

        case None =>
          inputDataset
      }
    }

    import sparkSession.implicits._
    paths.paths.foldLeft(sparkSession.emptyDataset[TestDataRow])((datasets, path) => datasets.unionByName(doRead(path)))
  }
}
