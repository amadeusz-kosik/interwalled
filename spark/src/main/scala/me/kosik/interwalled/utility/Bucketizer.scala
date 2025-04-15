package me.kosik.interwalled.utility

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Encoder, Encoders, functions => F}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


class Bucketizer(bucketScale: Long) {
  import IntervalColumns.{KEY, FROM, TO, BUCKET}

  private lazy val bucketize: UserDefinedFunction =
    F.udf((from: Long, to: Long, bucketScale: Long) => ((from / bucketScale) to to / bucketScale).toArray)

  def bucketize[T : TypeTag](input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]] = {
    @nowarn implicit val iTT = typeTag[BucketedInterval[T]]
    implicit val iEncoder: Encoder[BucketedInterval[T]] = Encoders.product[BucketedInterval[T]]

    input
      .withColumn(BUCKET,
        F.explode(bucketize(input.col(FROM), input.col(TO), F.lit(bucketScale)))
      )
      .repartition(F.col(BUCKET), F.col(KEY))
      .as[BucketedInterval[T]]
  }

}
