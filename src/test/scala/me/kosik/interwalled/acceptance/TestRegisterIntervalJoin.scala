package me.kosik.interwalled.acceptance

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ExplainMode, SimpleMode}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.Seq
import scala.util.Random

class TestRegisterIntervalJoin extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfter {

  lazy val lhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  lazy val rhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  def createDF(range: Long, schema: StructType, mapping: Long => Row): DataFrame = {
    val rdd = spark.sparkContext
      .parallelize(1L to range)
      .map(mapping)

    spark.createDataFrame(rdd, schema)
  }

  lazy val lhsDF: DataFrame =
    createDF(100L, lhsSchema, i => Row("ch-01", i, i + 1))

  lazy val rhsDF: DataFrame =
    createDF(100L, lhsSchema, i => Row("ch-01", i, i + 1))


  test("Without registering IntervalJoinStrategy") {
    spark.experimental.extraStrategies = Nil

    val job = lhsDF.join(
      rhsDF,
      (lhsDF("chromosome") === rhsDF("chromosome")) &&
        (lhsDF("start") <= rhsDF("end")) &&
        (rhsDF("start") <= lhsDF("end")),
      "inner"
    )

    val plan: String = job.queryExecution.explainString(ExplainMode.fromString("simple"))
    assert("Spark should not use Interwalled join plan.", false, plan.contains("AIListIntervalJoinPlan"))
  }

  test("Register AIListIntervalJoinStrategy, run broadcast version") {
    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil

    val job = lhsDF.join(
      broadcast(rhsDF),
        (lhsDF("chromosome") === rhsDF("chromosome")) &&
        (lhsDF("start") <= rhsDF("end")) &&
        (rhsDF("start") <= lhsDF("end")),
      "inner"
    )

    val plan: String = job.queryExecution.explainString(ExplainMode.fromString("simple"))
    assertTrue(plan.contains("BroadcastAIListIntervalJoinPlan"))
  }

  ignore("Register AIListIntervalJoinStrategy, run full version") {
    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil

    val job = lhsDF.join(
      rhsDF,
      (lhsDF("chromosome") === rhsDF("chromosome")) &&
        (lhsDF("start") <= rhsDF("end")) &&
        (rhsDF("start") <= lhsDF("end")),
      "inner"
    )

    val plan: String = job.queryExecution.explainString(ExplainMode.fromString("simple"))
    assert("Spark should use Interwalled join plan.",       true,   plan.contains("AIListIntervalJoinPlan"))
    assert("Spark should not use broadcast for the join.",  false,  plan.contains("BroadcastAIListIntervalJoinPlan"))
  }
}
