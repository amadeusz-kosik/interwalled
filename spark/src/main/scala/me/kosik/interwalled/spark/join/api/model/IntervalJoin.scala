package me.kosik.interwalled.spark.join.api.model


import me.kosik.interwalled.ailist.IntervalColumns
import me.kosik.interwalled.ailist.model.{Interval, IntervalsPair}
import me.kosik.interwalled.model.BucketedInterval
import org.apache.spark.sql.{Dataset, functions => F}


object IntervalJoin {

  case class Input(
    lhsData: Dataset[Interval],
    rhsData: Dataset[Interval]
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
  )
}
