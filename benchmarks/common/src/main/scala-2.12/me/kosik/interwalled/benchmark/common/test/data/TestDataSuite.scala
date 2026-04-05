package me.kosik.interwalled.benchmark.common.test.data

case class TestDataSuite(
  suite:          String,
  databasePaths:  DatabasePaths,
  queryPaths:     QueryPaths,
  limit:          Option[Long]
) {
  override def toString: String =
    s"TestDataSuite($suite, $databasePaths, $queryPaths, limit = $limit)"
}


object TestDataSuite {

  def apply(suite: String, databasePath: String, queryPath: String, limit: Option[Long]): TestDataSuite = TestDataSuite(
    suite         = suite,
    databasePaths = DatabasePaths(databasePath),
    queryPaths    = QueryPaths(queryPath),
    limit         = limit
  )
}