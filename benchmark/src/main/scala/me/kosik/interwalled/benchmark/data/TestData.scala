package me.kosik.interwalled.benchmark.data

import me.kosik.interwalled.domain.Interval
import org.apache.spark.sql.{Dataset, SparkSession}


case class TestData(
  suite:    String,
  database: TestData.TestDataset,
  query:    TestData.TestDataset
) {
  override def toString: String =
    s"TestData($suite, database = ${database.name}, query = ${query.name})"

  def sparkSession: SparkSession =
    database.dataset.sparkSession
}

object TestData {
  case class TestDataset(
    name: String,
    dataset: Dataset[Interval[String]]
  )
}
