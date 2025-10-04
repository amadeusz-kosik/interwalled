package me.kosik.interwalled.spark.join.implementation.ailist.implementation

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.ailist.{NativeAIListConfig, NativeAIListIntervalJoin}
import me.kosik.interwalled.spark.join.preprocessor.PreprocessorConfig
import org.apache.spark.sql.{DataFrame, SaveMode, functions => F}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec


class CheckpointedNativeAIListIntervalJoin(override val config: NativeAIListConfig, checkpointDir: String)
  extends NativeAIListIntervalJoin(config) {

  protected val name: String =
    s"checkpointed-native-ailist-${config.aiListConfig.toShortString}"

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

    data.sparkSession.read
      .schema(data.schema)
      .parquet(checkpointDir)
      .withColumn(BUCKET, F.coalesce(F.col(BUCKET), F.lit("")))
  }
}