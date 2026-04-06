package me.kosik.interwalled.benchmark.test.suite

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata

object TestDataSuites {

  @deprecated
  val values: Map[String, TestDataSuiteMetadata] =
    (TestDataSuiteMetadata.databioSuites ++ TestDataSuiteMetadata.artificialSuites)
      .map(suite => suite.suite -> suite)
      .toMap
}
