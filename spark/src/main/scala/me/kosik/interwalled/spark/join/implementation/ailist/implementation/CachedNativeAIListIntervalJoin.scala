package me.kosik.interwalled.spark.join.implementation.ailist.implementation

import me.kosik.interwalled.domain.{IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.ailist.{NativeAIListConfig, NativeAIListIntervalJoin}
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}

import scala.annotation.tailrec


class CachedNativeAIListIntervalJoin(override val config: NativeAIListConfig)
  extends NativeAIListIntervalJoin(config) {

  protected def name: String =
    s"cached-native-ailist-${config.aiListConfig.toShortString}"

  override protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
    input.lhsData.cache()
    super.doJoin(input)
  }

  @tailrec
  final override protected def iterate(sourceDF: DataFrame, alreadyExtracted: DataFrame, iteration: Int): DataFrame = {
    val spark = sourceDF.sparkSession
    import IntervalColumns._

    val (preExtractedDF, leftoversDF) = {
      if (iteration == config.aiListConfig.maximumComponentsCount - 1) {
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
