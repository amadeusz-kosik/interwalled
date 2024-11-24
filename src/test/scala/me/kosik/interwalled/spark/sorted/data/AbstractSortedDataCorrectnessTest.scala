package me.kosik.interwalled.spark.sorted.data

import me.kosik.interwalled.spark.CommonDFSuiteBase
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.BeforeAndAfterEach


abstract class AbstractSortedDataCorrectnessTest extends CommonDFSuiteBase with BeforeAndAfterEach {

  def validateSparkExecutionPlan(planString: String): Boolean

  def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame

  private def getDataReader(dataSuiteName: String): String => DataFrame = {
    datasetName: String => spark.read.parquet(s"./data/$dataSuiteName/$datasetName.parquet")
  }

  private def runTestSuite(dataSuiteName: String): () => Any = () => {
    val reader = getDataReader(dataSuiteName)

    val lhsDF = reader("in-lhs")
    val rhsDF = reader("in-rhs")

    val expected = reader("out-result")
    val actual = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")

    assertDataFrameNoOrderEquals(expected, actual)
  }

  test("Check if valid join implementation is used") {
    val reader = getDataReader("one-to-one")

    val lhsDF = reader("in-lhs")
    val rhsDF = reader("in-rhs")

    val result = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")
    val planString = result.queryExecution.explainString(ExplainMode.fromString("simple"))

    assert(validateSparkExecutionPlan(planString), "The Spark Engine has not used the expected join implementation.")
  }

  test("1:1 join")(runTestSuite("one-to-one"))

//  test("1:2 join") {
//    val lhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i + 1))
//    val rhsDF = createDF(100L, lhsSchema, i => Row("ch-01", i, i))
//
//    val actual = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")
//
//    actual.show()
//    actual.count() shouldEqual 199
//  }

  test("1:all join")(runTestSuite("one-to-all"))
}
