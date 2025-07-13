package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.utility.bucketizer.BucketingConfig
import org.apache.spark.sql.{DataFrame, SaveMode, functions => F}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._


class CheckpointedNativeAIListIntervalJoin(checkpointDir: String, config: AIListConfig, bucketingConfig: Option[BucketingConfig])
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

    val extractedDF = saveToDatabase(preExtractedDF.transform(calculateMaxEnd), maxIterations)

    if(leftoversDF.isEmpty) {
      extractedDF
    } else {
      iterate(leftoversDF, accumulatorDF, maxIterations - 1)
    }
  }

  private def saveToDatabase(data: DataFrame, iteration: Int): DataFrame = {
    import IntervalColumns._

    val saveMode = if(iteration == config.maximumComponentsCount) SaveMode.Overwrite else SaveMode.Append

    data.write
      .mode(saveMode)
      .partitionBy(_COMPONENT)
      .parquet(checkpointDir)

    data.sparkSession
      .read
      .schema(data.schema)
      .parquet(checkpointDir)
  }
}