package me.kosik.interwalled.spark.join.preprocessor.repartitioner

import me.kosik.interwalled.ailist.IntervalColumns
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorStep
import org.apache.spark.sql.{functions => F}
import org.slf4j.LoggerFactory


class Repartitioner(config: RepartitionerConfig) extends PreprocessorStep {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def toString: String =
    config.toString

  override def processInput(input: PreparedInput): PreparedInput = {
    import IntervalColumns.{BUCKET, KEY}

    logger.info(s"Requested repartition: ${config.doRepartition}.")

    if(config.doRepartition)
      input.copy(
        lhsData = input.lhsData.repartition(F.col(BUCKET), F.col(KEY)),
        rhsData = input.rhsData.repartition(F.col(BUCKET), F.col(KEY))
      )
    else
      input
  }
}