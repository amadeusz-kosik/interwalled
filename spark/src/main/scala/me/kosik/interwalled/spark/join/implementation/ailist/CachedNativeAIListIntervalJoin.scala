package me.kosik.interwalled.spark.join.implementation.ailist

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.utility.bucketizer.BucketingConfig
import org.apache.spark.sql.{DataFrame, functions => F}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._


class CachedNativeAIListIntervalJoin(config: AIListConfig, bucketingConfig: Option[BucketingConfig])
  extends NativeAIListIntervalJoin(config, bucketingConfig) {

  override def toString: String = {
    val bucketPrefix = bucketingConfig.map(bc => s"bucketized-${bc}-").getOrElse("")
    f"${bucketPrefix}cached-native-ailist-${config.toShortString}"
  }

  override protected def doJoin[T: TypeTag](lhsInputPrepared: BucketedIntervals[T], rhsInputPrepared: BucketedIntervals[T]): DataFrame = {
    lhsInputPrepared.cache()
    super.doJoin(lhsInputPrepared, rhsInputPrepared)
  }

  @tailrec
  final override protected def iterate(sourceDF: DataFrame, alreadyExtracted: DataFrame, iteration: Int): DataFrame = {
    val spark = sourceDF.sparkSession
    import IntervalColumns._

    val (preExtractedDF, leftoversDF) = {
      if (iteration == config.maximumComponentsCount - 1) {
        val extractedDF = sourceDF.withColumn(_COMPONENT, F.lit(iteration))
        val leftoversDF = spark.emptyDataFrame

        (extractedDF, leftoversDF)

      } else {
        _iterate(sourceDF = sourceDF, iteration = iteration)
      }
    }

    val newExtracted = preExtractedDF
      .transform(calculateMaxEnd)
      .unionByName(alreadyExtracted)

    if(leftoversDF.isEmpty) {
      newExtracted
    } else {
      iterate(leftoversDF, newExtracted, iteration + 1)
    }
  }
}
