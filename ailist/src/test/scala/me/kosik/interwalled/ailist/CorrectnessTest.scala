package me.kosik.interwalled.ailist

import me.kosik.interwalled.domain.Interval
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class CorrectnessTest extends AnyFunSuite with Matchers {
  import AIListTestHelper._

  /* Each correctness test asserts that list stores exactly the same elements as they
   *  were put into the list: no duplication, no data loss. */

  test("Data validation: no overlapping intervals") {
    val lhs = (1 to 1000) map { i => Interval("CH1", i +    0, i +    0, i)}
    val rhs = (1 to 1000) map { i => Interval("CH1", i + 1003, i + 1003, i)}

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }

  test("Data validation: 1:1 matching, two linear lists.") {
    val lhs = (1 to 100) map { i => Interval("CH1", i, i, "L")}
    val rhs = (1 to 100) map { i => Interval("CH1", i, i, "R")}

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }

  test("Data validation: 1 right interval matching all left intervals.") {
    val lhs = (1 to 10000) map { i => Interval("CH1", i, i, "L")}
    val rhs = Interval("CH1", 0, 10000, "R") :: Nil

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }

  test("Data validation: 1 left interval matching all right intervals.") {
    val lhs = Interval("CH1", 0, 10000, "L") :: Nil
    val rhs = (1 to 10000) map { i => Interval("CH1", i, i, "R")}

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }

  test("Data validation: 1 right interval matching all left intervals, uneven intervals distribution.") {
    val lhs = ((1 to 100) map { i =>
      Interval("CH1", i, i + 1, "L")
    }) ++ ((1 to 100) map { i =>
      Interval("CH1", i, i + 50, "L")
    })

    val rhs = Interval("CH1", 0, 10000, "R") :: Nil

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }

  test("Data validation: all to all matching.") {
    val lhs = (1 to 100) map { i => Interval("CH1", i, 1000 + i, "L")}
    val rhs = (1 to 100) map { i => Interval("CH1", i, 1000 + i, "R")}

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }

  test("Data validation: sparse-16 sample.") {
    val lhs = (1 to   6) map { i => Interval("CH1", i * 16, i * 16, "L")}
    val rhs = (1 to 100) map { i => Interval("CH1", i, i, "R")}

    val aiList = buildList(lhs)
    val actual = buildResult(aiList, rhs)
    val expected = buildExpected(lhs, rhs)

    assertEqual(expected, actual)
  }
}
