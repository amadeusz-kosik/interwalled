package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.benchmark.app.ApplicationEnv
import me.kosik.interwalled.benchmark.test.suite.TestDataSuiteReader
import me.kosik.interwalled.model.SparkInterval
import me.kosik.interwalled.spark.join.api.model.IntervalJoin


object PreprocessingRunner {

  def run(request: PreprocessingRequest, env: ApplicationEnv): PreprocessingResult = {
    import env.sparkSession.implicits._

    val database  = TestDataSuiteReader.readDatabase(request.dataSuite, env)
    val query     = TestDataSuiteReader.readQuery(request.dataSuite, env)

    val inputData         = IntervalJoin.Input(database.as[SparkInterval], query.as[SparkInterval])
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