package me.kosik.interwalled.spark.join

import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.utility.Bucketizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, functions => F}

import scala.annotation.{nowarn, tailrec}
import scala.reflect.runtime.universe._


class PartitionedNativeAIListIntervalJoin(bucketSplit: Long, maximumComponentsCount: Int) extends IntervalJoin {

  private val intervalsCountToCheckLookahead: Int = 20
  private val intervalsCountToTriggerExtraction: Int = 10

  override def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]] = {
    import IntervalColumns.{BUCKET, FROM, KEY, TO, VALUE, _COMPONENT, _MAX_E}

    implicit val spark: SparkSession = lhsInput.sparkSession

    @nowarn implicit val iTT = typeTag[Interval[T]]
    implicit val iEncoder: Encoder[Interval[T]] = Encoders.product[Interval[T]]

    @nowarn implicit val ipTT = typeTag[IntervalsPair[T]]
    implicit val ipEncoder: Encoder[IntervalsPair[T]] = Encoders.product[IntervalsPair[T]]

    val bucketizer = new Bucketizer(bucketSplit)

    val lhsInputBucketed = lhsInput.transform(bucketizer.bucketize)
    val rhsInputBucketed = rhsInput.transform(bucketizer.bucketize)

    lhsInputBucketed.cache()

    val joinLHS = {
      val emptyDF = spark
        .emptyDataset[Interval[T]]
        .toDF()
        .withColumn(BUCKET, F.lit(0))
        .withColumn(_COMPONENT, F.lit(0))
        .withColumn("_ailist_max_end", F.lit(0))

      val inputDF = lhsInputBucketed
        .toDF()
        .sort(KEY, BUCKET, FROM, TO)

      iterate(inputDF, emptyDF, maximumComponentsCount)
        .repartition(F.col(IntervalColumns.KEY), F.col(BUCKET), F.col(_COMPONENT))
        .rdd
        .map(row => (row.getAs[String](KEY), row.getAs[Long](BUCKET)) -> row)
    }

    val joinRHS = {
      rhsInputBucketed
        .toDF()
        .rdd
        .map(row => (row.getAs[String](KEY), row.getAs[Long](BUCKET)) -> row)
    }


    val joinResultRDD: RDD[IntervalsPair[T]] = (joinLHS cogroup joinRHS).flatMap { case ((key, bucket), (aiList, queries)) =>
      queries flatMap { query =>
        val queryFrom = query.getAs[Long](FROM)
        val queryTo = query.getAs[Long](TO)

        val matching = aiList
          .dropWhile(row => row.getAs[Long](_MAX_E) < queryFrom)
          .takeWhile(row => row.getAs[Long](FROM) <= queryTo)
          .filter(row => row.getAs[Long](TO) >= queryFrom)

        matching map { row =>
          val lhsInterval = Interval(
            key,
            row.getAs[Long](FROM),
            row.getAs[Long](TO),
            row.getAs[T](VALUE),
          )

          val rhsInterval = Interval(
            key,
            query.getAs[Long](FROM),
            query.getAs[Long](TO),
            query.getAs[T](VALUE),
          )

          IntervalsPair(key, lhsInterval, rhsInterval)
        }
      }
    }

    val joinResult = spark
      .createDataset(joinResultRDD)
      .distinct()

    joinResult
  }

  @tailrec
  private def iterate(sourceDF: DataFrame, accumulatorDF: DataFrame, maxIterations: Int): DataFrame = {
    import IntervalColumns.{BUCKET, FROM, KEY, TO, _COMPONENT}

    if(maxIterations == 0) {
      sourceDF
    } else {
      val sourceInputLookaheadWindow = Window
        .partitionBy(KEY, BUCKET)
        .orderBy(FROM, TO)
        .rowsBetween(1, intervalsCountToCheckLookahead)

      val preparedDF = sourceDF
        .withColumn("_ailist_lookahead", F.collect_set(TO).over(sourceInputLookaheadWindow))
        .withColumn("_ailist_lookahead_overlapping", F.filter(F.col("_ailist_lookahead"), _ <= F.col(TO)))
        .withColumn("_ailist_lookahead_overlapping_count", F.size(F.col("_ailist_lookahead_overlapping")))
        .withColumn("_ailist_lookahead_overlapping_keeper", F.col("_ailist_lookahead_overlapping_count") < F.lit(intervalsCountToTriggerExtraction))

      val keepersDF   = preparedDF
        .filter(F.col("_ailist_lookahead_overlapping_keeper") === true)
        .withColumn(_COMPONENT, F.lit(maxIterations))
        .drop(
          "_ailist_lookahead",
          "_ailist_lookahead_overlapping",
          "_ailist_lookahead_overlapping_count",
          "_ailist_lookahead_overlapping_keeper"
        )
        .transform(calculateMaxEnd)
        .unionByName(accumulatorDF)

      val extractedDF = preparedDF
        .filter(F.col("_ailist_lookahead_overlapping_keeper") === false)
        .drop(
          "_ailist_lookahead",
          "_ailist_lookahead_overlapping",
          "_ailist_lookahead_overlapping_count",
          "_ailist_lookahead_overlapping_keeper"
        )

      if(extractedDF.isEmpty)
        keepersDF
      else
        iterate(extractedDF, keepersDF, maxIterations - 1)
    }
  }

  private def calculateMaxEnd(dataFrame: DataFrame): DataFrame = {
    import IntervalColumns.{BUCKET, FROM, KEY, TO, _MAX_E, _COMPONENT}

    val maxEndWindow = Window
      .partitionBy(KEY, BUCKET, _COMPONENT)
      .orderBy(FROM, TO)
      .rowsBetween(Window.unboundedPreceding, 0)

    dataFrame
      .withColumn(_MAX_E, F.max(TO).over(maxEndWindow))
  }
}
