package me.kosik.interwalled.utility.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, functions => F}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


trait Bucketizer {
  def bucketize[T : TypeTag](input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]]
  def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]]
}

class SimpleBucketizer(config: BucketingConfig) {
  import IntervalColumns.{BUCKET, FROM, KEY, TO}

  private lazy val bucketize: UserDefinedFunction =
    F.udf((from: Long, to: Long, bucketScale: Long) => ((from / bucketScale) to to / bucketScale).toArray)

  def bucketize[T : TypeTag](input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]] = {
    @nowarn implicit val iTT = typeTag[BucketedInterval[T]]
    implicit val iEncoder: Encoder[BucketedInterval[T]] = Encoders.product[BucketedInterval[T]]

    val bucketScale = config.bucketScale

    input
      .withColumn(BUCKET,
        F.explode(bucketize(input.col(FROM), input.col(TO), F.lit(bucketScale)))
      )
      .repartition(F.col(BUCKET), F.col(KEY))
      .as[BucketedInterval[T]]
  }

  def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]] = {
    input.distinct()
  }
}
