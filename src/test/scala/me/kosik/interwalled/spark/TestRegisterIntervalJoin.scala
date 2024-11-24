package me.kosik.interwalled.spark


class TestRegisterIntervalJoin {


//  test("Register AIListIntervalJoinStrategy, run full version") {
//    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil
//
//    val job = lhsDF.join(
//      rhsDF,
//      (lhsDF("chromosome") === rhsDF("chromosome")) &&
//        (lhsDF("start") <= rhsDF("end")) &&
//        (rhsDF("start") <= lhsDF("end")),
//      "inner"
//    )
//
//    val plan: String = job.queryExecution.explainString(ExplainMode.fromString("simple"))
//    assert("Spark should use Interwalled join plan.",       true,   plan.contains("AIListIntervalJoinPlan"))
//    assert("Spark should not use broadcast for the join.",  false,  plan.contains("BroadcastAIListIntervalJoinPlan"))
//  }
}
