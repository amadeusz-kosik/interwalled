package me.kosik.interwalled.benchmark.sequila.data

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.benchmark.common.test.data.model.TestDataRow
import org.apache.spark.sql.Dataset


case class TestDataSuite(
  metadata: TestDataSuiteMetadata,
  database: Dataset[TestDataRow],
  query:    Dataset[TestDataRow]
)
