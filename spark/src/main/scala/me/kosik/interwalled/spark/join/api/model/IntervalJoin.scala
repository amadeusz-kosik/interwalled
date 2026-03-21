package me.kosik.interwalled.spark.join.api.model


import me.kosik.interwalled.ailist.IntervalColumns
import me.kosik.interwalled.model.{BucketedInterval, SparkInterval, SparkIntervalsPair}
import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats
import org.apache.spark.sql.{Column, Dataset, functions => F}


object IntervalJoin {

  case class Input(
    lhsData: Dataset[SparkInterval],
    rhsData: Dataset[SparkInterval]
  ) {
    def toPreparedInput: PreparedInput = {
      import lhsData.sparkSession.implicits._
      import IntervalColumns.BUCKET

      PreparedInput(
        lhsData.withColumn(BUCKET, F.lit("Bucket")).as[BucketedInterval],
        rhsData.withColumn(BUCKET, F.lit("Bucket")).as[BucketedInterval]
      )
    }
  }

  case class PreparedInput(
    lhsData: Dataset[BucketedInterval],
    rhsData: Dataset[BucketedInterval]
  ) {
    def repartition(partitionExpressions: Column *): PreparedInput = this.copy(
      lhsData = lhsData.repartition(partitionExpressions :_*),
      rhsData = rhsData.repartition(partitionExpressions :_*)
    )
  }

  case class Result(
    data: Dataset[SparkIntervalsPair],
    statistics: Option[IntervalJoinRunStats]
  )
}
