package com.eternalsh.interwalled
package spark

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}

class IntervalJoinStrategy(sparkSession: SparkSession) extends Strategy with Serializable {

  private def flattenAndPredicates(condition: Expression): Seq[Expression] = condition match {

    case And(leftCondition, rightCondition) =>
      flattenAndPredicates(leftCondition) ++ flattenAndPredicates(rightCondition)

    case _ =>
      List(condition)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case join @ Join(left, right, joinType, _condition, hint) =>
      val condition = _condition.map(flattenAndPredicates).getOrElse(Seq.empty)

      log.warn(s"Considered interval join for $join.")
      log.warn("Not implemented yet.")
      Seq.empty[SparkPlan]


//    val intervalHolderClassName = spark.sqlContext.getConf(InternalParams.intervalHolderClass,
//      "org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack" )
//
//    plan match {
//      case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
//        IntervalTreeJoinOptim(planLater(left), planLater(right), rangeJoinKeys, spark,left,right,intervalHolderClassName) :: Nil
//      case ExtractRangeJoinKeysWithEquality(joinType, rangeJoinKeys, left, right) => {
//        val minOverlap = spark.sqlContext.getConf(InternalParams.minOverlap,"1")
//        val maxGap = spark.sqlContext.getConf(InternalParams.maxGap,"0")
//        val useJoinOrder = spark.sqlContext.getConf(InternalParams.useJoinOrder,"false")
//        log.info(
//          s"""Running SeQuiLa interval join with following parameters:
//             |minOverlap = ${minOverlap}
//             |maxGap = ${maxGap}
//             |useJoinOrder = ${useJoinOrder}
//             |intervalHolderClassName = ${intervalHolderClassName}
//             |""".stripMargin)
//        IntervalTreeJoinOptimChromosome(
//          planLater(left),
//          planLater(right),
//          rangeJoinKeys,
//          spark,
//          minOverlap.toInt,
//          maxGap.toInt,
//          useJoinOrder.toBoolean,
//          intervalHolderClassName,
//        ) :: Nil
//      }
//      case _ =>
//        Nil
//    }
  }
}
