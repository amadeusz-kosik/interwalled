package me.kosik.interwalled.benchmark.common.test.data

case class TestDataSuiteMetadata(
  suite:          String,
  databasePaths:  DatabasePaths,
  queryPaths:     QueryPaths,
  limit:          Option[Long]
) {
  override def toString: String =
    s"TestDataSuiteMetadata($suite, $databasePaths, $queryPaths, limit = $limit)"
}


object TestDataSuiteMetadata {

  val artificialSuites: Array[TestDataSuiteMetadata] = {
    val sizes = Array(
      100L,
      500L,
      1000L,
      5000L,
      10 * 1000L,
      50 * 1000L,
      100 * 1000L,
      500 * 1000L,
      1000 * 1000L,
      5000 * 1000L,
      10 * 1000 * 1000L,
      50 * 1000 * 1000L,
      100 * 1000 * 1000L
    )

    val suites = Array(
      TestDataSuiteMetadata(
        "one-to-one",
        "test-data/single-point-continuous.parquet",
        "test-data/single-point-continuous.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "one-to-even",
        "test-data/single-point-continuous.parquet",
        "test-data/single-point-even.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "odd-to-even",
        "test-data/single-point-odd.parquet",
        "test-data/single-point-even.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "one-to-long-continuous",
        "test-data/single-point-continuous.parquet",
        "test-data/long-continuous.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "one-to-long-overlap",
        "test-data/single-point-continuous.parquet",
        "test-data/long-overlap.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-short-overlap",
        "test-data/short-continuous.parquet",
        "test-data/short-overlap.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-long-overlap",
        "test-data/short-continuous.parquet",
        "test-data/long-overlap.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-random-normal-short",
        "test-data/short-continuous.parquet",
        "test-data/random-normal-short.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-random-poisson-short",
        "test-data/short-continuous.parquet",
        "test-data/random-poisson-short.parquet",
        None
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-random-uniform-short",
        "test-data/short-continuous.parquet",
        "test-data/random-uniform-short.parquet",
        None
      )
    )

    for {
      rawSuite <- suites
      size     <- sizes
      suite     = TestDataSuiteMetadata(
        suite         = f"${rawSuite.suite}-$size",
        databasePaths = rawSuite.databasePaths,
        queryPaths    = rawSuite.queryPaths,
        limit         = Some(size)
      )
    } yield suite
  }

  val databioSuites: Array[TestDataSuiteMetadata] = Array(
    TestDataSuiteMetadata(
      "databio-s-1-2",
      "databio-8p/fBrain-DS14718/",
      "databio-8p/exons/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-s-2-7",
      "databio-8p/exons/",
      "databio-8p/ex-anno/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-s-1-0",
      "databio-8p/fBrain-DS14718/",
      "databio-8p/chainRn4/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-m-7-0",
      "databio-8p/ex-anno/",
      "databio-8p/chainRn4/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-m-7-3",
      "databio-8p/ex-anno/",
      "databio-8p/chainOrnAna1/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-l-0-8",
      "databio-8p/chainRn4/",
      "databio-8p/ex-rna/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-l-4-8",
      "databio-8p/chainVicPac2/",
      "databio-8p/ex-rna/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-l-7-8",
      "databio-8p/ex-anno/",
      "databio-8p/ex-rna/",
      None
    ),
    TestDataSuiteMetadata(
      "databio-xl-3-0",
      "databio-8p/chainOrnAna1/",
      "databio-8p/chainRn4/",
      None
    )
  )

  def apply(suite: String, databasePath: String, queryPath: String, limit: Option[Long]): TestDataSuiteMetadata = TestDataSuiteMetadata(
    suite         = suite,
    databasePaths = DatabasePaths(databasePath),
    queryPaths    = QueryPaths(queryPath),
    limit         = limit
  )
}