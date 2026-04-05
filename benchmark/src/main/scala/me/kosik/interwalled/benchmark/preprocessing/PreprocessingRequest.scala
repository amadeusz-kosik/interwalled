package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuite
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor


case class PreprocessingRequest(
  dataSuite: TestDataSuite,
  preprocessor: Preprocessor
)
