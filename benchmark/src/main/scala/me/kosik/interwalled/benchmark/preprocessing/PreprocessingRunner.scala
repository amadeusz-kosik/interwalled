package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.ailist.model.Interval
import me.kosik.interwalled.benchmark.app.ApplicationEnv
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteLoader
import me.kosik.interwalled.spark.join.api.model.IntervalJoin
import org.apache.spark.sql.SparkSession


object PreprocessingRunner {

  def run(request: PreprocessingRequest, env: ApplicationEnv): PreprocessingResult = {
    implicit val sparkSession: SparkSession = env.sparkSession
    import sparkSession.implicits._

    val testData          = TestDataSuiteLoader.load(env.dataDirectory, request.dataSuite)
    val inputData         = IntervalJoin.Input(testData.database.as[Interval], testData.query.as[Interval])
    val preparedData      = inputData.toPreparedInput
    val preprocessedData  = request.preprocessor.prepareInput(preparedData)

    PreprocessingResult(
      request.dataSuite.suite,
      request.preprocessor.toString,
      preprocessedData.lhsData.count(),
      preprocessedData.lhsData.rdd.mapPartitions(i => Array(i.length).toIterator).collect(),
      preprocessedData.rhsData.count(),
      preprocessedData.rhsData.rdd.mapPartitions(i => Array(i.length).toIterator).collect()
    )
  }
}