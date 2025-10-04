package me.kosik.interwalled.spark.join.preprocessor.salter

import me.kosik.interwalled.domain.{BucketedInterval, IntervalColumns}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{functions => F}
import org.slf4j.LoggerFactory


class Salter(config: SalterConfig) extends Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def toString: String =
    config.toString

  def processInput(input: PreparedInput): PreparedInput = {
    import IntervalColumns._
    import input.lhsData.sparkSession.implicits._

    val scale = config.calculateScale(math.max(input.lhsData.count(), input.rhsData.count()))
    logger.info(s"Salting with parameters: scale: $scale.")

    val lhsSalted = input.lhsData
      .withColumn("_bucket_y", F.floor(F.rand() * F.lit(scale)).cast(DataTypes.LongType))
      .withColumn("_bucket_x", F.explode(F.lit((0L until scale).toArray)))
      .withColumn(BUCKET, F.concat_ws(":", F.col(BUCKET), F.col("_bucket_x") * F.lit(scale) + F.col("_bucket_y")))
      .drop("_bucket_x", "_bucket_y")
      .as[BucketedInterval]

    val rhsSalted = input.rhsData
      .withColumn("_bucket_x", F.floor(F.rand() * F.lit(scale)).cast(DataTypes.LongType))
      .withColumn("_bucket_y", F.explode(F.lit((0L until scale).toArray)))
      .withColumn(BUCKET, F.concat_ws(":", F.col(BUCKET), F.col("_bucket_x") * F.lit(scale) + F.col("_bucket_y")))
      .drop("_bucket_x", "_bucket_y")
      .as[BucketedInterval]

    PreparedInput(lhsSalted, rhsSalted)
  }
}