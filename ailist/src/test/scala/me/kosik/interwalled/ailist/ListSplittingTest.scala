package me.kosik.interwalled.ailist

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class ListSplittingTest extends AnyFunSuite with Matchers {
  import AIListTestHelper._

  test("Splitting list into components test: basic splitting intervals") {
    val intervals = Array(
      Interval("CH1",   1,  11, "NIL"),
      Interval("CH1",   1,  12, "NIL"),
      Interval("CH1",   2,  13, "NIL"),
      Interval("CH1",   2,  14, "NIL"),
      Interval("CH1",   3,  15, "NIL"),

      Interval("CH1",   4,  64, "NIL"),

      Interval("CH1",   4,  12, "NIL"),
      Interval("CH1",   5,  10, "NIL"),
      Interval("CH1",   6,  12, "NIL"),
      Interval("CH1",   6,  14, "NIL"),
      Interval("CH1",   7,  18, "NIL"),

      Interval("CH1",   8,  64, "NIL"),
      Interval("CH1",   8,  62, "NIL"),
      Interval("CH1",   9,  63, "NIL"),
      Interval("CH1",   9,  61, "NIL"),
      Interval("CH1",  10,  60, "NIL"),

      Interval("CH1",  10,  33, "NIL"),
      Interval("CH1",  11,  32, "NIL"),
      Interval("CH1",  12,  33, "NIL"),
      Interval("CH1",  12,  32, "NIL"),
      Interval("CH1",  15,  33, "NIL"),
    )

    val aiList = buildList(intervals, new AIListConfig(10, 5, 5, 3))

    assert(21 == aiList.size,               "All 21 Intervals should be persisted.")
    assert( 2 == aiList.getComponentsCount, "Intervals ending with > 60 should be placed in a separate component.")
  }

  test("Splitting list into components test - no splitting intervals due to minimum component length") {
    val intervals = Array(
      Interval("CH1",   1,  11, "NIL"),
      Interval("CH1",   1,  12, "NIL"),
      Interval("CH1",   2,  13, "NIL"),
      Interval("CH1",   2,  14, "NIL"),
      Interval("CH1",   3,  15, "NIL"),

      Interval("CH1",   4,  64, "NIL"),

      Interval("CH1",   4,  12, "NIL"),
      Interval("CH1",   5,  10, "NIL"),
      Interval("CH1",   6,  12, "NIL"),
      Interval("CH1",   6,  14, "NIL"),
      Interval("CH1",   7,  18, "NIL"),

      Interval("CH1",   8,  64, "NIL"),
      Interval("CH1",   8,  62, "NIL"),
      Interval("CH1",   9,  63, "NIL"),
      Interval("CH1",   9,  61, "NIL"),
      Interval("CH1",  10,  60, "NIL"),

      Interval("CH1",  10,  33, "NIL"),
      Interval("CH1",  11,  32, "NIL"),
      Interval("CH1",  12,  33, "NIL"),
      Interval("CH1",  12,  32, "NIL"),
      Interval("CH1",  15,  33, "NIL"),
    )

    val aiList = buildList(intervals, new AIListConfig(10, 5, 5, 25))

    assert(21 == aiList.size,               "All 21 Intervals should be persisted.")
    assert( 1 == aiList.getComponentsCount, "All intervals should be placed in the same component.")
  }

  test("Splitting list into components test - no splitting intervals due to maximum component count") {
    val intervals = Array(
      Interval("CH1",   1,  11, "NIL"),
      Interval("CH1",   1,  12, "NIL"),
      Interval("CH1",   2,  13, "NIL"),
      Interval("CH1",   2,  14, "NIL"),
      Interval("CH1",   3,  15, "NIL"),

      Interval("CH1",   4,  64, "NIL"),

      Interval("CH1",   4,  12, "NIL"),
      Interval("CH1",   5,  10, "NIL"),
      Interval("CH1",   6,  12, "NIL"),
      Interval("CH1",   6,  14, "NIL"),
      Interval("CH1",   7,  18, "NIL"),

      Interval("CH1",   8,  64, "NIL"),
      Interval("CH1",   8,  62, "NIL"),
      Interval("CH1",   9,  63, "NIL"),
      Interval("CH1",   9,  61, "NIL"),
      Interval("CH1",  10,  60, "NIL"),

      Interval("CH1",  10,  33, "NIL"),
      Interval("CH1",  11,  32, "NIL"),
      Interval("CH1",  12,  33, "NIL"),
      Interval("CH1",  12,  32, "NIL"),
      Interval("CH1",  15,  33, "NIL"),
    )

    val aiList = buildList(intervals, new AIListConfig(1, 5, 5, 3))

    assert(21 == aiList.size,               "All 21 Intervals should be persisted.")
    assert( 1 == aiList.getComponentsCount, "All intervals should be placed in the same component.")
  }
}
