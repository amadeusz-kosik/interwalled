package me.kosik.interwalled.spark.join

import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.utility.Bucketizer
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._
import org.apache.spark.sql.{functions => F}


class SparkNativeIntervalJoin(bucketsCount: Long) extends IntervalJoin {

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    implicit val spark: SparkSession = lhsInput.sparkSession

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val bucketizer = new Bucketizer(bucketsCount)

    val lhsInputBucketed = lhsInput.transform(bucketizer.bucketize)
    val rhsInputBucketed = rhsInput.transform(bucketizer.bucketize)

    lhsInputBucketed
      .join(rhsInputBucketed,
        (lhsInputBucketed.col(IntervalColumns.KEY) === rhsInputBucketed.col(IntervalColumns.KEY)) and
        (lhsInputBucketed.col(IntervalColumns.BUCKET) === rhsInputBucketed.col(IntervalColumns.BUCKET))
      )
      .filter(
        (lhsInput.col(IntervalColumns.FROM) >=  rhsInput.col(IntervalColumns.TO)  ) and
        (lhsInput.col(IntervalColumns.TO)   >=  rhsInput.col(IntervalColumns.FROM))
      )
      .drop(
        lhsInputBucketed.col(IntervalColumns.BUCKET),
        rhsInputBucketed.col(IntervalColumns.BUCKET)
      )
      .distinct()
      .select(
        lhsInput.col(IntervalColumns.KEY)     .alias("key"),

        F.struct(
          lhsInput.col(IntervalColumns.KEY)   .alias(s"${IntervalColumns.KEY}"),
          lhsInput.col(IntervalColumns.FROM)  .alias(f"${IntervalColumns.FROM}"),
          lhsInput.col(IntervalColumns.TO)    .alias(f"${IntervalColumns.TO}"),
          lhsInput.col(IntervalColumns.VALUE) .alias(f"${IntervalColumns.VALUE}")
        ).alias("lhs"),

        F.struct(
          rhsInput.col(IntervalColumns.KEY)   .alias(s"${IntervalColumns.KEY}"),
          rhsInput.col(IntervalColumns.FROM)  .alias(f"${IntervalColumns.FROM}"),
          rhsInput.col(IntervalColumns.TO)    .alias(f"${IntervalColumns.TO}"),
          rhsInput.col(IntervalColumns.VALUE) .alias(f"${IntervalColumns.VALUE}")
        ).alias("rhs")
      )
      .as[IntervalsPair[T]]
  }

}
