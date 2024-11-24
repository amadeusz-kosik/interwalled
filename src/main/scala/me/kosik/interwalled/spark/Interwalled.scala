
package me.kosik.interwalled.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Interwalled {

  private val logger: Logger =
    Logger.getLogger(this.getClass.getCanonicalName)

  def register(sparkSession: SparkSession): Unit = {
    logger.info("Registering Interwalled extensions.")

    sparkSession.experimental.extraStrategies = Seq(
//      new IntervalTreeJoinStrategyOptim(spark),
//      new GenomicIntervalStrategy(spark)
    )
  }

}
