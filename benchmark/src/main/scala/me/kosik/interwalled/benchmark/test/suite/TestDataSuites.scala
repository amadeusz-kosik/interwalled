package me.kosik.interwalled.benchmark.test.suite


object TestDataSuites {

  private val sizes = Array(
             1 * 1000L,
             5 * 1000L,
            10 * 1000L,
            50 * 1000L,
           100 * 1000L,
           500 * 1000L,
      1 * 1000 * 1000L,
      5 * 1000 * 1000L,
     10 * 1000 * 1000L,
     50 * 1000 * 1000L,
    100 * 1000 * 1000L
  )

  private val rawSuites = Array(
    TestDataSuite(
      "one-to-one",
      "test-data/single-point-continuous.parquet",
      "test-data/single-point-continuous.parquet",
      None
    ),
    TestDataSuite(
      "one-to-none",
      "test-data/single-point-even.parquet",
      "test-data/single-point-odd.parquet",
      None
    )
  )

  private val databioSuites = Array(
    TestDataSuite(
      "databio-s-1-2",
      "databio-8p/fBrain-DS14718/",
      "databio-8p/exons/",
      None
    ),
    TestDataSuite(
      "databio-s-2-7",
      "databio-8p/exons/",
      "databio-8p/ex-anno/",
      None
    ),
    TestDataSuite(
      "databio-s-1-0",
      "databio-8p/fBrain-DS14718/",
      "databio-8p/chainRn4/",
      None
    ),
    TestDataSuite(
      "databio-m-7-0",
      "databio-8p/ex-anno/",
      "databio-8p/chainRn4/",
      None
    ),
    TestDataSuite(
      "databio-m-7-3",
      "databio-8p/ex-anno/",
      "databio-8p/chainOrnAna1/",
      None
    ),
    TestDataSuite(
      "databio-l-0-8",
      "databio-8p/chainRn4/",
      "databio-8p/ex-rna/",
      None
    ),
    TestDataSuite(
      "databio-l-4-8",
      "databio-8p/chainVicPac2/",
      "databio-8p/ex-rna/",
      None
    ),
    TestDataSuite(
      "databio-l-7-8",
      "databio-8p/ex-anno/",
      "databio-8p/ex-rna/",
      None
    ),
    TestDataSuite(
      "databio-xl-3-0",
      "databio-8p/chainOrnAna1/",
      "databio-8p/chainRn4/",
      None
    )
  )

  val values: Map[String, TestDataSuite] = (for {
    rawSuite <- rawSuites
    size     <- sizes
    suite     = TestDataSuite(
      suite         = f"${rawSuite.suite}-$size",
      databasePaths = rawSuite.databasePaths,
      queryPaths    = rawSuite.queryPaths,
      limit         = Some(size)
    )
  } yield suite).union(databioSuites).map(suite => suite.suite -> suite).toMap
}
