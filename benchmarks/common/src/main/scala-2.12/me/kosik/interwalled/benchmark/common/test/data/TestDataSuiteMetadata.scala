package me.kosik.interwalled.benchmark.common.test.data

import me.kosik.interwalled.benchmark.common.test.data.model.TestDataSizeLimit

case class TestDataSuiteMetadata(
  suite:          String,
  databasePaths:  DatabasePaths,
  queryPaths:     QueryPaths,
  limit:          TestDataSizeLimit
) {
  override def toString: String =
    s"TestDataSuiteMetadata($suite, $databasePaths, $queryPaths, $limit)"
}


object TestDataSuiteMetadata {
  def apply(suite: String, databasePath: String, queryPath: String, limit: Option[Long]): TestDataSuiteMetadata = TestDataSuiteMetadata(
    suite         = suite,
    databasePaths = DatabasePaths(databasePath),
    queryPaths    = QueryPaths(queryPath),
    limit         = TestDataSizeLimit(limit)
  )
}