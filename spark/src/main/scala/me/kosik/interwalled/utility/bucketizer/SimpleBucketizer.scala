package me.kosik.interwalled.utility.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Encoder, Encoders, functions => F}

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


class SimpleBucketizer(config: BucketingConfig) extends Bucketizer with Serializable {

  private lazy val bucketize: UserDefinedFunction =
    F.udf((from: Long, to: Long, bucketScale: Long) => ((from / bucketScale) to to / bucketScale).toArray)

  override def bucketize[T : TypeTag](input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]] = {
    import IntervalColumns._

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

  override def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]] = {
    input.distinct()
  }
}