package me.kosik.interwalled.benchmark.test.suite


case class TestDataSuite(
  suite:          String,
  databasePaths:  Array[String],
  queryPaths:     Array[String],
  limit:          Option[Long]
) {
  override def toString: String =
    s"TestData($suite, " +
      s"database = ${databasePaths.mkString("Array(", ", ", ")")}, " +
      s"query = ${queryPaths.mkString("Array(", ", ", "" +
    ")")}"
}


object TestDataSuite {

  def apply(suite: String, databasePath: String, queryPath: String, limit: Option[Long]): TestDataSuite = TestDataSuite(
    suite         = suite,
    databasePaths = Array(databasePath),
    queryPaths    = Array(queryPath),
    limit         = limit
  )
}