package me.kosik.interwalled.benchmark.common.test.data

import me.kosik.interwalled.benchmark.common.test.data.model.TestDataSizeLimit


object TestDataSuites {
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
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "one-to-even",
        "test-data/single-point-continuous.parquet",
        "test-data/single-point-even.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "odd-to-even",
        "test-data/single-point-odd.parquet",
        "test-data/single-point-even.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "one-to-long-continuous",
        "test-data/single-point-continuous.parquet",
        "test-data/long-continuous.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "one-to-long-overlap",
        "test-data/single-point-continuous.parquet",
        "test-data/long-overlap.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-short-overlap",
        "test-data/short-continuous.parquet",
        "test-data/short-overlap.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-long-overlap",
        "test-data/short-continuous.parquet",
        "test-data/long-overlap.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-random-normal-short",
        "test-data/short-continuous.parquet",
        "test-data/random-normal-short.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-random-poisson-short",
        "test-data/short-continuous.parquet",
        "test-data/random-poisson-short.parquet",
        None,
        100L // FIXME
      ),
      TestDataSuiteMetadata(
        "short-continuous-to-random-uniform-short",
        "test-data/short-continuous.parquet",
        "test-data/random-uniform-short.parquet",
        None,
        100L // FIXME
      )
    )

    for {
      rawSuite <- suites
      size     <- sizes
      suite     = TestDataSuiteMetadata(
        suite          = f"${rawSuite.suite}-$size",
        databasePaths  = rawSuite.databasePaths,
        queryPaths     = rawSuite.queryPaths,
        limit          = TestDataSizeLimit(size),
        expectedOutput =  100L // FIXME
      )
    } yield suite
  }

  val databioSuites: Array[TestDataSuiteMetadata] = Array(

    // -- small -- //
//    TestDataSuiteMetadata(
//      "databio-s-36",
//      "databio-8p/fBrain/",
//      "databio-8p/exons/",
//      None,
//      54246L
//    ),
//    TestDataSuiteMetadata(
//      "databio-s-35",
//      "databio-8p/fBrain/",
//      "databio-8p/ex-anno/",
//      None,
//      73437
//    ),
//    TestDataSuiteMetadata(
//      "databio-s-34",
//      "databio-8p/exons/",
//      "databio-8p/ex-anno/",
//      None,
//      273500L
//    ),
//    TestDataSuiteMetadata(
//      "databio-s-33",
//      "databio-8p/chainRn4/",
//      "databio-8p/fBrain/",
//      None,
//      320955L
//    ),
//    TestDataSuiteMetadata(
//      "databio-s-32",
//      "databio-8p/fBrain/",
//      "databio-8p/chainVicPac2/",
//      None,
//      357564L
//    ),
//    TestDataSuiteMetadata(
//      "databio-s-31",
//      "databio-8p/fBrain/",
//      "databio-8p/chainOrnAna1/",
//      None,
//      398738L
//    ),
    TestDataSuiteMetadata(
      "databio-s-30",
      "databio-8p/fBrain/",
      "databio-8p/chainXenTro3Link/",
      None,
      625718L
    ),

//    // -- medium -- //
//    TestDataSuiteMetadata(
//      "databio-m-29",
//      "databio-8p/fBrain/",
//      "databio-8p/chainMonDom5Link/",
//      None,
//      2380147L
//    ),
//    TestDataSuiteMetadata(
//      "databio-m-28",
//      "databio-8p/chainRn4/",
//      "databio-8p/exons/",
//      None,
//      2633098L
//    ),
//    TestDataSuiteMetadata(
//      "databio-m-27",
//      "databio-8p/chainRn4/",
//      "databio-8p/ex-anno/",
//      None,
//      2761621L
//    ),
//    TestDataSuiteMetadata(
//      "databio-m-26",
//      "databio-8p/exons/",
//      "databio-8p/chainOrnAna1/",
//      None,
//      3255513L
//    ),
//    TestDataSuiteMetadata(
//      "databio-m-25",
//      "databio-8p/chainOrnAna1/",
//      "databio-8p/ex-anno/",
//      None,
//      4408383L
//    )
  )
}
