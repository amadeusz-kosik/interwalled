package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.test.data.generator.model._
import org.apache.spark.sql.{Dataset, SparkSession}

trait TestCase {
  def testCaseName: String

  def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow]

  def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow]

  def generateResult(implicit spark: SparkSession): Dataset[TestResultRow] = {
    import spark.implicits._

    val lhsData = generateLHS.collect()
    val rhsData = generateRHS.collect()

    lhsData
      .flatMap(lhs => rhsData
        .filter(rhs => lhs.from <= rhs.to && rhs.from <= lhs.to && lhs.chromosome == rhs.chromosome)
        .map(rhs => TestResultRow(lhs.from, lhs.to, rhs.from, rhs.to, lhs.chromosome))
      )
      .toSeq
      .toDS()
  }


  final protected def generateLinear(rowsCount: Long)(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    spark.sparkContext.range(1L, rowsCount + 1)
      .map(i => TestDataRow(i, i, "CH1"))
      .toDS()
  }
}
