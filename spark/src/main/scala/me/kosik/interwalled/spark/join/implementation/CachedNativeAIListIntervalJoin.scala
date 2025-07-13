package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.utility.bucketizer.{BucketingConfig, Bucketizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._


class CachedNativeAIListIntervalJoin(config: AIListConfig, bucketingConfig: Option[BucketingConfig])
  extends NativeAIListIntervalJoin(config, bucketingConfig) {

  override protected def doJoin[T: TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared: BucketedIntervals[T]): DataFrame = {
    lhsInputPrepared.cache()
    super.doJoin(lhsInputPrepared, rhsInputPrepared)
  }

  @tailrec
  final override protected def iterate(sourceDF: DataFrame, accumulatorDF: DataFrame, maxIterations: Int): DataFrame = {
    val spark = sourceDF.sparkSession
    import IntervalColumns._

    val (preExtractedDF, leftoversDF) = {
      if (maxIterations == 0) {
        val extractedDF = sourceDF.withColumn(_COMPONENT, F.lit(maxIterations))
        val leftoversDF = spark.emptyDataFrame

        (extractedDF, leftoversDF)

      } else {
        _iterate(sourceDF = sourceDF, maxIterations = maxIterations)
      }
    }

    val extractedDF = preExtractedDF
      .transform(calculateMaxEnd)
      .unionByName(accumulatorDF)

    if(leftoversDF.isEmpty) {
      extractedDF
    } else {
      iterate(leftoversDF, accumulatorDF, maxIterations - 1)
    }
  }
}
