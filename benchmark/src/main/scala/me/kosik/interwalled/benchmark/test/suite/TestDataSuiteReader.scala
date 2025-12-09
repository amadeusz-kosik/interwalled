package me.kosik.interwalled.benchmark.test.suite

import me.kosik.interwalled.benchmark.app.ApplicationEnv
import me.kosik.interwalled.ailist.IntervalColumns
import me.kosik.interwalled.ailist.test.TestDataRow
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}


object TestDataSuiteReader {

  def readDatabase(dataSuite: TestDataSuite, env: ApplicationEnv): Dataset[TestDataRow] =
    read(dataSuite, dataSuite.databasePaths, env)

  def readQuery(dataSuite: TestDataSuite, env: ApplicationEnv): Dataset[TestDataRow] =
    read(dataSuite, dataSuite.queryPaths, env)

  private def loaders: Map[String, DataFrame => DataFrame] = Map(
    "databio-8p" -> { df =>
      import IntervalColumns._

      df.select(
        F.col("contig").as(KEY),
        F.col("pos_start").as(FROM),
        F.col("pos_end").as(TO),
        F.uuid().as(VALUE)
      )
    },
    "test-data" -> { df =>
      import IntervalColumns._

      df.select(
        F.lit("IW000").as(KEY),
        F.col(FROM),
        F.col(TO),
        F.col(VALUE)
      )
    }
  )

  // FixMe refactor
  private def read(suite: TestDataSuite, paths: Array[String], env: ApplicationEnv): Dataset[TestDataRow] = {

    def doRead(path: String): Dataset[TestDataRow] = {
      import env.sparkSession.implicits._

      val inputPathPrefix = path.split("/")(0)
      val inputDataset = env.sparkSession.read
        .parquet(f"${env.dataDirectory}/$path")
        .transform(loaders.getOrElse(inputPathPrefix, throw new IllegalArgumentException("Unknown test data prefix.")))
        .as[TestDataRow]

      suite.limit match {
        case Some(limit) =>
          inputDataset
            .filter(F.col("from") <= limit)
            .filter(F.col("to")   <= limit)

        case None =>
          inputDataset
      }
    }

    import env.sparkSession.implicits._
    paths.foldLeft(env.sparkSession.emptyDataset[TestDataRow])((datasets, path) => datasets.unionByName(doRead(path)))
  }
}
