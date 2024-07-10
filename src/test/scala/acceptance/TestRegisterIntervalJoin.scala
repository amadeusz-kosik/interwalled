package com.eternalsh.interwalled
package acceptance

import com.eternalsh.interwalled.spark.strategy.AIListIntervalJoinStrategy
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{ExplainMode, SimpleMode}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.Seq
import scala.util.Random

class TestRegisterIntervalJoin extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfter {

  val lhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  val rhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  def createTempView(range: Long, name: String, schema: StructType, mapping: Long => Row): Unit = {
    val rdd = spark.sparkContext
      .parallelize(1L to range)
      .map(mapping)

    val df = spark
      .createDataFrame(rdd, schema)

    df.createOrReplaceTempView(name)
  }

  before {
    createTempView(100L, "view_lhs", lhsSchema, i => Row("ch-01", i, i + 1))
    createTempView(100L, "view_rhs", rhsSchema, i => Row("ch-01", i, i + 1))
  }

  private val SQL_STRING_SIMPLE = """ |
    | SELECT * FROM view_lhs lhs
    |   INNER JOIN view_rhs rhs ON (
    |     lhs.start <= rhs.end AND
    |     rhs.start <= lhs.end AND
    |     lhs.chromosome = rhs.chromosome
    |   )""".stripMargin

  test("Without registering IntervalJoinStrategy") {
    spark.experimental.extraStrategies = Nil

    val plan = spark.sqlContext
      .sql(SQL_STRING_SIMPLE)
      .queryExecution.explainString(ExplainMode.fromString("simple"))

    assertTrue(! plan.contains("BroadcastAIListIntervalJoinPlan"))
  }

  test("Register AIListIntervalJoinStrategy") {
    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil

    val plan: String = spark.sqlContext
      .sql(SQL_STRING_SIMPLE)
      .queryExecution.explainString(ExplainMode.fromString("simple"))

    assertTrue(plan.contains("BroadcastAIListIntervalJoinPlan"))
  }
}
