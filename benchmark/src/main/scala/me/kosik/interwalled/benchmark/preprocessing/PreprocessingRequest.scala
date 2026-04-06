package me.kosik.interwalled.benchmark.preprocessing

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor


case class PreprocessingRequest(
  dataSuite: TestDataSuiteMetadata,
  preprocessor: Preprocessor
)
