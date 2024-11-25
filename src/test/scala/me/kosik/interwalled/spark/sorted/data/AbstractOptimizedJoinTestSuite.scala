package me.kosik.interwalled.spark.sorted.data


abstract class AbstractOptimizedJoinTestSuite extends AbstractJoinTestSuite {

  test("1000 rows, 1:1 join") {
    runTestSuite("one-to-one", 1_000L)
  }

  test("1000 rows, 1:4 join") {
    runTestSuite("one-to-many", 1_000L)
  }

  test("1000 rows, 1:all join") {
    runTestSuite("one-to-all", 1_000L)
  }
}
