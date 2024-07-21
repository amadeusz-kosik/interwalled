package com.eternalsh.interwalled
package spark.strategy.extractor

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, GreaterThanOrEqual, LessThanOrEqual, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinHint, JoinStrategyHint, LogicalPlan}

import scala.collection.Seq


case class IntervalSource(start: Expression, end: Expression, grouping: Expression, plan: LogicalPlan)


object IntervalJoinPredicate extends Logging with PredicateHelper with Serializable {
  type ExtractedType = (IntervalSource, IntervalSource)

  def extract(left: LogicalPlan, right: LogicalPlan, queryJoinCondition: Expression): Option[ExtractedType] = {
    // Wrappers for checking if two expressions come from the same LogicalPlan.
    //  In terms of domain: whether they form a single interval boundary.
    val canEvaluateLeft = canEvaluateAll(left)
    val canEvaluateRight = canEvaluateAll(right)

    val possibleConditions = flattenConditions(queryJoinCondition)
      .getOrElse(Seq.empty)

    val groupings = possibleConditions.filter(_.isInstanceOf[EqualTo]) match {
      case Nil =>
        log.info("Interval join is not available for not grouped data.")
        None

      case EqualTo(leftEq, rightEq) :: Nil if canEvaluate(leftEq, left) && canEvaluate(rightEq, right) =>
        Some(leftEq, rightEq)

      case EqualTo(rightEq, leftEq) :: Nil if canEvaluate(leftEq, left) && canEvaluate(rightEq, right) =>
        Some(leftEq, rightEq)

      case condition @ EqualTo(_, _) :: Nil =>
        log.info(s"Failed to assign left and right of ${condition} to both sides of the join.")
        None

      case manyConditions =>
        log.info(s"Current implementation of the interval join requires exactly one equality condition, multiple " +
          s"provided in ${manyConditions}.")
        None
    }

    val intervalConditions = possibleConditions
      .filter(condition => condition.isInstanceOf[LessThanOrEqual] || condition.isInstanceOf[GreaterThanOrEqual])

    (groupings, intervalConditions) match {
      case (None, _) =>
        log.info("Interval join not possible: Interwalled requires exactly one EQ condition (standard one), " +
          "none or multiple provided. Consider adding column with constant value or concatenating (folding) EQ columns.")
        None

      case (Some((lGrouping, rGrouping)), LessThanOrEqual(rhsBegin, lhsEnd) :: LessThanOrEqual(lhsBegin, rhsEnd) :: Nil)
        if canEvaluateLeft(Seq(lhsBegin, lhsEnd)) && canEvaluateRight(Seq(rhsBegin, rhsEnd)) =>
        Some((IntervalSource(lhsBegin, lhsEnd, lGrouping, left), IntervalSource(rhsBegin, rhsEnd, rGrouping, right)))

      case (Some((lGrouping, rGrouping)), LessThanOrEqual(lhsBegin, rhsEnd) :: LessThanOrEqual(rhsBegin, lhsEnd) :: Nil)
        if canEvaluateLeft(Seq(lhsBegin, lhsEnd)) && canEvaluateRight(Seq(rhsBegin, rhsEnd)) =>
        Some((IntervalSource(lhsBegin, lhsEnd, lGrouping, left), IntervalSource(rhsBegin, rhsEnd, rGrouping, right)))

      case _ =>
        log.info(s"Unsupported conditions: ${intervalConditions}")
        None
    }
  }

  private def canEvaluateAll(plan: LogicalPlan): Seq[Expression] => Boolean =
    (expressions: Seq[Expression]) => expressions.forall(super.canEvaluate(_, plan))

  private def flattenConditions(condition: Expression): Option[Seq[Expression]] = condition match {
    case Or(_, _) =>
      None

    case And(leftCondition, rightCondition) => for {
      left  <- flattenConditions(leftCondition)
      right <- flattenConditions(rightCondition)
    } yield left ++ right

    case any =>
      Some(Seq(any))
  }
}

object BroadcastIntervalJoinPredicatePattern extends SQLConfHelper with Serializable {
  def unapply(plan: LogicalPlan): Option[IntervalJoinPredicate.ExtractedType] = plan match {

    case Join(left, right, Inner, Some(condition), JoinHint(leftHint, _)) if canBroadcast(left, leftHint) =>
      IntervalJoinPredicate.extract(left, right, condition)

    case Join(left, right, Inner, Some(condition), JoinHint(_, rightHint)) if canBroadcast(right, rightHint) =>
      IntervalJoinPredicate.extract(right, left, condition)

    case _ =>
      None
  }

  private def canBroadcast(plan: LogicalPlan, hintInfo: Option[HintInfo]): Boolean = {
    // AQE version: conf.getConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD)

    val autoBroadcastJoinThreshold = conf.autoBroadcastJoinThreshold
    val sizeInBytes = if (plan.stats.sizeInBytes != Long.MaxValue) Some(plan.stats.sizeInBytes) else None
    val autoBroadcast = sizeInBytes.exists(autoBroadcastJoinThreshold >= _)

    import org.apache.spark.sql.catalyst.plans.logical.BROADCAST

    val hint = for {
      hint      <- hintInfo
      strategy  <- hint.strategy
    }  yield strategy

    hint.contains(BROADCAST) || autoBroadcast
  }
}

object FullIntervalJoinPredicatePattern extends SQLConfHelper with Serializable {
  def unapply(plan: LogicalPlan): Option[IntervalJoinPredicate.ExtractedType] = plan match {
    case Join(left, right, Inner, Some(condition), _) =>
      IntervalJoinPredicate.extract(left, right, condition)

    case _ =>
      None
  }
}