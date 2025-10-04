package me.kosik.interwalled.spark.join.preprocessor.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, functions => F}
import org.slf4j.LoggerFactory


class Bucketizer(config: BucketizerConfig) extends Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val bucketize: UserDefinedFunction =
    F.udf((from: Long, to: Long, min: Long, max: Long, scale: Long) => {
      val normalizedFrom = (from - min) * scale / (max - min)
      val normalizedTo   = ( to  - min) * scale / (max - min)

      (normalizedFrom to normalizedTo).toArray
    })

  override def toString: String =
    config.toString

  def processInput(input: PreparedInput): PreparedInput = {
    import IntervalColumns._

    val (greaterDataset, scale) = {
      val lhsCount = input.lhsData.count()
      val rhsCount = input.rhsData.count()

      val (dataset, count) = if(lhsCount >= rhsCount)
        (input.lhsData, lhsCount)
      else
        (input.rhsData, rhsCount)

      (dataset, config.calculateScale(count))
    }

    val (bucketMin, bucketMax) = {
      val result = greaterDataset.agg(F.min(FROM).as("_min"), F.max(TO).as("_max")).collect().head
      (result.getLong(0), result.getLong(1))
    }

    logger.info(s"Bucketing with parameters: min: $bucketMin, max: $bucketMax, scale: $scale.")

    PreparedInput(
      _bucketize(bucketMin, bucketMax, scale, input.lhsData),
      _bucketize(bucketMin, bucketMax, scale, input.rhsData),
    )
  }

  def processOutput(input: Dataset[IntervalsPair]): Dataset[IntervalsPair] = {
    input.distinct()
  }

  private def _bucketize(min: Long, max: Long, scale: Long, input: Dataset[BucketedInterval]): Dataset[BucketedInterval] = {
    import IntervalColumns._
    import input.sparkSession.implicits._

    input
      .withColumn("_bucketizer",
        F.explode(bucketize(input.col(FROM), input.col(TO), F.lit(min), F.lit(max), F.lit(scale)))
      )
      .withColumn(BUCKET, F.concat_ws(":", F.col(BUCKET), F.col("_bucketizer").cast(DataTypes.StringType)))
      .drop("_bucketizer")
      .as[BucketedInterval]
  }

}