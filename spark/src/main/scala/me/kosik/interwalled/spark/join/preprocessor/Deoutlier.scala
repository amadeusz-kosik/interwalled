package me.kosik.interwalled.spark.join.preprocessor

import me.kosik.interwalled.ailist.{BucketedInterval, IntervalColumns}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.preprocessor.Deoutlier.DeoutlierConfig
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor.PreprocessorStep
import org.apache.spark.sql.{Dataset, functions => F}
import org.slf4j.LoggerFactory


class Deoutlier(config: DeoutlierConfig) extends PreprocessorStep {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def toString: String =
    config.toString

  override def processInput(input: PreparedInput): PreparedInput =
    input.copy(
      lhsData = process(input.lhsData),
      rhsData = process(input.rhsData)
    )

  private def process(data: Dataset[BucketedInterval]): Dataset[BucketedInterval] = {
    import IntervalColumns._

    val width       = F.col(TO) - F.col(FROM)
    val percentage  = F.lit(config.percentile.toDouble / 1000.0)
    val accuracy    = F.lit(1000000)

    val percentile = data
      .select(F.approx_percentile(width, percentage, accuracy))
      .take(1)
      .head
      .getLong(0)

    logger.info(s"Computed outlier threshold: ${data.queryExecution}")
    data.filter(width < F.lit(percentile))
  }
}

object Deoutlier {
  case class DeoutlierConfig(percentile: Int) {
    override def toString: String = s"deoutlier-$percentile"
  }
}