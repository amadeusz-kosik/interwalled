package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.data.TestData
import me.kosik.interwalled.domain.test.TestResultRow
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import org.apache.spark.sql.SparkSession


trait Benchmark {

  def prepareBenchmark: BenchmarkCallback = {
    val fn = (testData: TestData) => {
      val sparkSession: SparkSession = testData.sparkSession
      import sparkSession.implicits._

      joinImplementation
        .join(Input(testData.database.dataset, testData.query.dataset))
        .data.as[TestResultRow]
    }

    val statistics = (testData: TestData) => {
      joinImplementation
        .join(Input(testData.database.dataset, testData.query.dataset), gatherStatistics = true)
        .statistics
    }

    BenchmarkCallback(this.toString, fn, statistics)
  }

  def joinImplementation: IntervalJoin
}
