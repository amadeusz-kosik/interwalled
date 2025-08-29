package me.kosik.interwalled.utility.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Encoder, Encoders, functions => F}
import org.slf4j.LoggerFactory

import scala.annotation.nowarn
import scala.reflect.runtime.universe._


class SaltingBucketizer(config: BucketingConfig) extends Bucketizer with Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def bucketize[T : TypeTag](lhs: Dataset[Interval[T]], rhs: Dataset[Interval[T]]): (Dataset[BucketedInterval[T]], Dataset[BucketedInterval[T]]) = {
    import IntervalColumns._

    @nowarn implicit val iTT = typeTag[BucketedInterval[T]]
    implicit val iEncoder: Encoder[BucketedInterval[T]] = Encoders.product[BucketedInterval[T]]

    val scale = config.bucketScale(math.max(lhs.count(), rhs.count()))
    logger.info(s"Bucketing with parameters: scale: $scale.")

    val lhsSalted = lhs
      .withColumn("_bucket_x", F.explode(F.lit((0L until scale).toArray)))
      .withColumn("_bucket_y", F.floor(F.rand() * F.lit(scale)).cast(DataTypes.LongType))
      .withColumn(BUCKET, F.col("_bucket_x") * F.lit(scale) + F.col("_bucket_y"))
      .drop("_bucket_x", "_bucket_y")
      .repartition(F.col(BUCKET), F.col(KEY))
      .as[BucketedInterval[T]]

    val rhsSalted = rhs
      .withColumn("_bucket_x", F.floor(F.rand() * F.lit(scale)).cast(DataTypes.LongType))
      .withColumn("_bucket_y", F.explode(F.lit((0L until scale).toArray)))
      .withColumn(BUCKET, F.col("_bucket_x") * F.lit(scale) + F.col("_bucket_y"))
      .drop("_bucket_x", "_bucket_y")
      .repartition(F.col(BUCKET), F.col(KEY))
      .as[BucketedInterval[T]]

    (lhsSalted, rhsSalted)
  }

  override def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]] =
    input
}