
package me.kosik.interwalled.spark

import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

class TestIntervalJoinCorrectness extends CommonDFSuiteBase with BeforeAndAfter with Matchers {

  lazy val lhsDF: DataFrame =
    createDF(100L, lhsSchema, i => Row("ch-01", i, i))

  lazy val rhsDF: DataFrame =
    createDF(100L, lhsSchema, i => Row("ch-01", i, i))

  private lazy val JoinPredicate =
    (lhsDF("chromosome") === rhsDF("chromosome")) && (lhsDF("start") <= rhsDF("end")) && (rhsDF("start") <= lhsDF("end"))


  test("Smoke test - without any Interwalled methods Spark should return valid results") {
    spark.experimental.extraStrategies = Nil

    val actual = lhsDF
      .join(broadcast(rhsDF), JoinPredicate, "inner")
      .cache()

    actual.show()
    actual.count() shouldEqual 100
  }

  test("BroadcastAIListIntervalJoinPlan should return valid results") {
    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil

    val actual = lhsDF
      .join(broadcast(rhsDF), JoinPredicate, "inner")
      .cache()

    actual.show()
    actual.count() shouldEqual 100
  }

  test("FullAIListIntervalJoinPlan should return valid results") {
    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil

    val actual = lhsDF
      .join(rhsDF, JoinPredicate, "inner")
      .cache()

    actual.show()
    actual.count() shouldEqual 100
  }
}
