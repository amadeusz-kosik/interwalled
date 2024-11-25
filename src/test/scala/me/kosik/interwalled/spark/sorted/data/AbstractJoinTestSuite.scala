package me.kosik.interwalled.spark.sorted.data

import me.kosik.interwalled.spark.CommonDFSuiteBase
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.execution.ExplainMode
import org.scalatest.BeforeAndAfterEach

abstract class AbstractJoinTestSuite extends CommonDFSuiteBase with BeforeAndAfterEach {

  def validateSparkExecutionPlan(planString: String): Boolean

  def join(lhsDF: DataFrame, rhsDF: DataFrame, joinPredicate: Column, joinType: String): DataFrame

  protected def getDataReader(dataSuiteName: String): String => DataFrame = {
    datasetName: String => spark.read.parquet(s"./data/$dataSuiteName/$datasetName.parquet")
  }

  protected def runTestSuite(dataSuiteName: String, dataSuiteSize: Long): Unit = {
    val reader = getDataReader(dataSuiteName)

    val lhsDF = reader(s"$dataSuiteSize/in-lhs")
    val rhsDF = reader(s"$dataSuiteSize/in-rhs")

    val expected = reader(s"$dataSuiteSize/out-result")
    val actual = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner").select(
      lhsDF("from")       as("fromLHS"),
      lhsDF("to")         as("toLHS"),
      rhsDF("from")       as("fromRHS"),
      rhsDF("to")         as("toRHS"),
      lhsDF("chromosome") as("chromosome")
    )

    assertDataFrameNoOrderEquals(expected, actual)
  }


  test("Check if valid join implementation is used") {
    val reader = getDataReader("one-to-one")

    val lhsDF = reader("100/in-lhs")
    val rhsDF = reader("100/in-rhs")

    val result = join(lhsDF, rhsDF, getJoinPredicate(lhsDF, rhsDF), "inner")
    val planString = result.queryExecution.explainString(ExplainMode.fromString("simple"))

    assert(validateSparkExecutionPlan(planString), "The Spark Engine has not used the expected join implementation.")
  }

  test("100 rows, 1:1 join") {
    runTestSuite("one-to-one", 100L)
  }

  test("100 rows, 1:4 join") {
    runTestSuite("one-to-many", 100L)
  }

  test("100 rows, 1:all join") {
    runTestSuite("one-to-all", 100L)
  }
}
