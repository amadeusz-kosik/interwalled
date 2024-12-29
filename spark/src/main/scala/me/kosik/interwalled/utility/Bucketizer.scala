package me.kosik.interwalled.utility

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, functions => F}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._

class Bucketizer(bucketSize: Long) {

  private lazy val bucketize: UserDefinedFunction =
    F.udf((from: Long, to: Long, bucketSize: Long) => ((from / bucketSize) to to / bucketSize).toArray)

  def bucketize[T : TypeTag](input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]] = {
    @nowarn implicit val iTT = typeTag[BucketedInterval[T]]
    implicit val iEncoder: Encoder[BucketedInterval[T]] = Encoders.product[BucketedInterval[T]]

    input
      .withColumn(IntervalColumns.BUCKET,
        F.explode(bucketize(input.col(IntervalColumns.FROM), input.col(IntervalColumns.TO), F.lit(bucketSize)))
      )
      .repartition(F.col(IntervalColumns.BUCKET))
      .as[BucketedInterval[T]]
  }

}
