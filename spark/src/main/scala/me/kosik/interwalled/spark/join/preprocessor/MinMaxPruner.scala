package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.ailist.IntervalColumns
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.preprocessor.MinMaxPruner.MinMaxPrunerConfig
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorStep
import org.apache.spark.sql.{functions => F}
import org.slf4j.LoggerFactory


class MinMaxPruner(config: MinMaxPrunerConfig) extends PreprocessorStep {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def toString: String =
    config.toString

  override def processInput(input: PreparedInput): PreparedInput = {
    import IntervalColumns._

    val (lhsMin, lhsMax) = input.lhsData.agg(F.min(FROM), F.max(TO)).take(1).map(r => (r.getLong(0), r.getLong(1))).head
    val (rhsMin, rhsMax) = input.rhsData.agg(F.min(FROM), F.max(TO)).take(1).map(r => (r.getLong(0), r.getLong(1))).head

    logger.info(s"MinMaxPruner - LHS: min = $lhsMin, max = $lhsMax")
    logger.info(s"MinMaxPruner - RHS: min = $rhsMin, max = $rhsMax")

    val filters: Seq[PreparedInput => PreparedInput] = Seq(
      { in =>
        if(lhsMin < rhsMin)
          in.copy(lhsData = in.lhsData.filter(F.col(TO) >= rhsMin))
        else
          in.copy(rhsData = in.rhsData.filter(F.col(TO) >= lhsMin))
      },
      { in =>
        if(lhsMax > rhsMax)
          in.copy(lhsData = in.lhsData.filter(F.col(FROM) <= rhsMax))
        else
          in.copy(rhsData = in.rhsData.filter(F.col(FROM) <= lhsMax))
      },
    )

    filters.foldLeft(input)((input, filter) => filter(input))
  }
}

object MinMaxPruner {
  case class MinMaxPrunerConfig(prune: Boolean) {
    override def toString: String = s"min-max-prune-$prune"
  }
}