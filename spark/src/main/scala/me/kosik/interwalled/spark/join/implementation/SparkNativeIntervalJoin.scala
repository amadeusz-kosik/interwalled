package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.domain.{IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import me.kosik.interwalled.utility.bucketizer.{BucketingConfig, Bucketizer}
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}

import scala.reflect.runtime.universe._


class SparkNativeIntervalJoin(bucketingConfig: Option[BucketingConfig]) extends IntervalJoin {
  private val bucketizer = Bucketizer(bucketingConfig)

  override protected def prepareInput[T : TypeTag](input: Input[T]): PreparedInput[T] =
    (bucketizer.bucketize(input.lhsData), bucketizer.bucketize(input.rhsData))

  protected def doJoin[T : TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared:  BucketedIntervals[T]): DataFrame = {
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
  }

  override protected def finalizeResult[T : TypeTag](joinedResultRaw: DataFrame): Dataset[IntervalsPair[T]] = {
    import joinedResultRaw.sparkSession.implicits._

    joinedResultRaw
      .as[IntervalsPair[T]]
      .transform(bucketizer.deduplicate)
  }
}
