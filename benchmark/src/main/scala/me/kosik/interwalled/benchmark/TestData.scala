package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.Interval
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestData(
  suite: String,
  database: Dataset[Interval[String]],
  query: Dataset[Interval[String]]
) {
  override def toString: String = s"TestData($suite)"

  def sparkSession: SparkSession =
    database.sparkSession
}

object TestData {

  def load(pathPrefix: String, suite: String)(implicit sparkSession: SparkSession): TestData = TestData(
    suite,
    load(s"$pathPrefix/$suite/database.parquet"),
    load(s"$pathPrefix/$suite/query.parquet")
  )

  private def load(path: String)(implicit sparkSession: SparkSession): Dataset[Interval[String]] = {
    import sparkSession.implicits._
    sparkSession.read.parquet(path).as[Interval[String]]
  }
}
