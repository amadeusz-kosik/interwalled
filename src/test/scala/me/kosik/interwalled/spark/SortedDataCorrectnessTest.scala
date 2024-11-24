package me.kosik.interwalled.spark

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.BeforeAndAfterEach


abstract class SortedDataCorrectnessTest extends CommonDFSuiteBase with BeforeAndAfterEach {

  def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame

  test("1:1 join") {
    val lhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i))
    val rhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i))

    val actual = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")

    actual.show()
    actual.count() shouldEqual 100
  }

  test("1:2 join") {
    val lhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i + 1))
    val rhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i))

    val actual = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")

    actual.show()
    actual.count() shouldEqual 199
  }

  test("1:all join") {
    val lhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i +    1))
    val rhsDF = createDF(  1L, lhsSchema, i => Row("ch-01", i, i + 1000))

    val actual = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")

    actual.show()
    actual.count() shouldEqual 100
  }
}
