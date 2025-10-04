package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.benchmark.test.suite.TestDataSuite
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor


case class PreprocessingRequest(
  dataSuite: TestDataSuite,
  preprocessor: Preprocessor
)
