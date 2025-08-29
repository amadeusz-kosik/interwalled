package me.kosik.interwalled.utility.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Encoder, Encoders, functions => F}
import org.slf4j.LoggerFactory

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


class SimpleBucketizer(config: BucketingConfig) extends Bucketizer with Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val bucketize: UserDefinedFunction =
    F.udf((from: Long, to: Long, min: Long, max: Long, scale: Long) => {
      val normalizedFrom = (from - min) * scale / (max - min)
      val normalizedTo   = ( to  - min) * scale / (max - min)

      (normalizedFrom to normalizedTo).toArray
    })

  override def bucketize[T : TypeTag](lhs: Dataset[Interval[T]], rhs: Dataset[Interval[T]]): (Dataset[BucketedInterval[T]], Dataset[BucketedInterval[T]]) = {
    import IntervalColumns._

    val (greaterDataset, scale) = {
      val lhsCount = lhs.count()
      val rhsCount = rhs.count()

      val (dataset, count) = if(lhsCount >= rhsCount)
        (lhs, lhsCount)
      else
        (rhs, rhsCount)

      (dataset, config.bucketScale(count))
    }

    val (bucketMin, bucketMax) = {
      val result = greaterDataset.agg(F.min(FROM).as("_min"), F.max(TO).as("_max")).collect().head
      (result.getLong(0), result.getLong(1))
    }

    logger.info(s"Bucketing with parameters: min: $bucketMin, max: $bucketMax, scale: $scale.")

    (
      _bucketize(bucketMin, bucketMax, scale, lhs),
      _bucketize(bucketMin, bucketMax, scale, rhs),
    )
  }

  def _bucketize[T : TypeTag](min: Long, max: Long, scale: Long, input: Dataset[Interval[T]]): Dataset[BucketedInterval[T]] = {
    import IntervalColumns._

    @nowarn implicit val iTT = typeTag[BucketedInterval[T]]
    implicit val iEncoder: Encoder[BucketedInterval[T]] = Encoders.product[BucketedInterval[T]]

    input
      .withColumn(BUCKET,
        F.explode(bucketize(input.col(FROM), input.col(TO), F.lit(min), F.lit(max), F.lit(scale)))
      )
      .repartition(F.col(BUCKET), F.col(KEY))
      .as[BucketedInterval[T]]

  }

  override def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]] = {
    input.distinct()
  }
}