package me.kosik.interwalled.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ExplainMode, SimpleMode}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter


class TestRegisterIntervalJoin extends CommonDFSuiteBase with BeforeAndAfter {

  lazy val lhsDF: DataFrame =
    createDF(100L, lhsSchema, i => Row("ch-01", i, i))

  lazy val rhsDF: DataFrame =
    createDF(100L, lhsSchema, i => Row("ch-01", i, i))

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

  test("Register AIListIntervalJoinStrategy, run full version") {
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
