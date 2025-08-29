package me.kosik.interwalled.spark.join.implementation.ailist

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.utility.bucketizer.BucketingConfig
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, functions => F}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec


class CheckpointedNativeAIListIntervalJoin(checkpointDir: String, config: AIListConfig, bucketingConfig: Option[BucketingConfig])
  extends NativeAIListIntervalJoin(config, bucketingConfig) {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def toString: String = {
    val bucketPrefix = bucketingConfig.map(bc => s"bucketized-${bc}-").getOrElse("")
    f"${bucketPrefix}checkpointed-native-ailist-${config.toShortString}"
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

    val newExtracted = saveToDatabase(preExtractedDF.transform(calculateMaxEnd), iteration)

    if(leftoversDF.isEmpty) {
      newExtracted
    } else {
      iterate(leftoversDF, newExtracted, iteration + 1)
    }
  }

  private def saveToDatabase(data: DataFrame, iteration: Int): DataFrame = {
    import IntervalColumns._

    val saveMode = if(iteration == 0) SaveMode.Overwrite else SaveMode.Append

    data.write
      .mode(saveMode)
      .partitionBy(_COMPONENT, BUCKET, KEY)
      .parquet(checkpointDir)

    data.sparkSession
      .read
      .schema(data.schema)
      .parquet(checkpointDir)
  }
}