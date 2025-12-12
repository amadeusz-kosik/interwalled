package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.{IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.SparkNativeIntervalJoin.Config
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import org.apache.spark.sql.{Dataset, functions => F}


class SparkNativeIntervalJoin(override val config: Config) extends ExecutorIntervalJoin {

  protected val name: String = "spark-native"

  override protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
    import input.lhsData.sparkSession.implicits._
    val lhsInputPrepared = input.lhsData
    val rhsInputPrepared = input.rhsData

    lhsInputPrepared
      .join(
        rhsInputPrepared,
        lhsInputPrepared.col(IntervalColumns.KEY)     === rhsInputPrepared.col(IntervalColumns.KEY    ) and
        lhsInputPrepared.col(IntervalColumns.BUCKET)  === rhsInputPrepared.col(IntervalColumns.BUCKET )
      )
      .filter(
        (lhsInputPrepared.col(IntervalColumns.FROM) <=  rhsInputPrepared.col(IntervalColumns.TO)  ) and
        (lhsInputPrepared.col(IntervalColumns.TO)   >=  rhsInputPrepared.col(IntervalColumns.FROM))
      )
      .drop(
        lhsInputPrepared.col(IntervalColumns.BUCKET),
        rhsInputPrepared.col(IntervalColumns.BUCKET)
      )
      .select(
        lhsInputPrepared.col(IntervalColumns.KEY)     .alias("key"),

        F.struct(
          lhsInputPrepared.col(IntervalColumns.KEY)   .alias(s"${IntervalColumns.KEY}"),
          lhsInputPrepared.col(IntervalColumns.FROM)  .alias(f"${IntervalColumns.FROM}"),
          lhsInputPrepared.col(IntervalColumns.TO)    .alias(f"${IntervalColumns.TO}"),
          lhsInputPrepared.col(IntervalColumns.VALUE) .alias(f"${IntervalColumns.VALUE}")
        ).alias("lhs"),

        F.struct(
          rhsInputPrepared.col(IntervalColumns.KEY)   .alias(s"${IntervalColumns.KEY}"),
          rhsInputPrepared.col(IntervalColumns.FROM)  .alias(f"${IntervalColumns.FROM}"),
          rhsInputPrepared.col(IntervalColumns.TO)    .alias(f"${IntervalColumns.TO}"),
          rhsInputPrepared.col(IntervalColumns.VALUE) .alias(f"${IntervalColumns.VALUE}")
        ).alias("rhs")
      )
      .as[IntervalsPair]
  }
}

object SparkNativeIntervalJoin {
  case class Config(override val preprocessorConfig: PreprocessorConfig)
    extends ExecutorConfig
}