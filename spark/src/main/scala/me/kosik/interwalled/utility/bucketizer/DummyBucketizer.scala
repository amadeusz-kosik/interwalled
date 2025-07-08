package me.kosik.interwalled.utility.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, functions => F}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


object DummyBucketizer extends Bucketizer with Serializable {

  override def bucketize[T : TypeTag](input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]] = {
    import IntervalColumns._

    @nowarn implicit val iTT = typeTag[BucketedInterval[T]]
    implicit val iEncoder: Encoder[BucketedInterval[T]] = Encoders.product[BucketedInterval[T]]

    input
      .withColumn(BUCKET, F.lit(0))
      .as[BucketedInterval[T]]
  }

  override def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]] = {
    input
  }
}
