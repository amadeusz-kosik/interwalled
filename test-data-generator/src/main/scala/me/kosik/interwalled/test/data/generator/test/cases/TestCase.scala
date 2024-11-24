package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.test.data.generator.model._
import org.apache.spark.sql.{Dataset, SparkSession}

trait TestCase {
  def testCaseName: String

  def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow]

  def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow]

  def generateResult(implicit spark: SparkSession): Dataset[TestResultRow]
}
