package me.kosik.interwalled.spark.join.implementation.ailist

import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.ExecutorIntervalJoin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}


abstract class NativeAIListIntervalJoin(override val config: NativeAIListConfig) extends ExecutorIntervalJoin {

  override protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
    import IntervalColumns._
    val lhsInputPrepared = input.lhsData
    val rhsInputPrepared = input.rhsData
    import lhsInputPrepared.sparkSession.implicits._

    val joinDatabase = {
      val emptyDF = lhsInputPrepared.sparkSession
        .emptyDataset[Interval]
        .toDF()
        .withColumn(BUCKET, F.lit(""))
        .withColumn(_COMPONENT, F.lit(0))
        .withColumn("_ailist_max_end", F.lit(0))

      val inputDF = lhsInputPrepared
        .toDF()

      val result = iterate(inputDF, emptyDF, 0)
        .repartition(F.col(IntervalColumns.KEY), F.col(BUCKET), F.col(_COMPONENT))
        .rdd
        .map(row => (row.getAs[String](KEY), row.getAs[String](BUCKET)) -> row)

      result
    }

    val joinQuery = rhsInputPrepared
      .toDF()
      .rdd
      .map(row => (row.getAs[String](KEY), row.getAs[String](BUCKET)) -> row)

    computeJoin(lhsInputPrepared.sparkSession, joinDatabase, joinQuery)
      .as[IntervalsPair]
  }

  private def computeJoin(sparkSession: SparkSession, joinDatabase: RDD[((String, String), Row)], joinQuery: RDD[((String, String), Row)]): DataFrame = {
    import IntervalColumns._
    import sparkSession.implicits._

    val databaseWithComponent: RDD[((String, String, Int), Row)] = joinDatabase
      .map { row =>
        val ((key: String, bucket), interval: Row) = row
        val component = interval.getAs[Int](_COMPONENT)

        (key, bucket, component) -> interval
      }

    val queryWithComponent: RDD[((String, String, Int), Row)] = {
      val componentsMap =
        sparkSession.sparkContext.broadcast[Array[Int]]((0 to config.aiListConfig.maximumComponentsCount).toArray)

      joinQuery.flatMap { row =>
        val ((key: String, bucket: String), interval: Row) = row
        componentsMap.value.map { component =>
          (key, bucket, component) -> interval
        }
      }
    }

    val crossJoinProduct: RDD[((String, String, Int), (Iterable[Row], Iterable[Row]))] =
      databaseWithComponent.cogroup(queryWithComponent)

    val joinResultRDD: RDD[IntervalsPair] = crossJoinProduct.flatMap { case ((key, _, _), (aiList, queries)) =>
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
            row.getAs(VALUE),
          )

          val rhsInterval = Interval(
            key,
            query.getAs[Long](FROM),
            query.getAs[Long](TO),
            query.getAs(VALUE),
          )

          IntervalsPair(key, lhsInterval, rhsInterval)
        }
      }
    }

    val joinResult = sparkSession
      .createDataset(joinResultRDD)
      .toDF

    joinResult
  }

  protected def iterate(sourceDF: DataFrame, accumulatorDF: DataFrame, iteration: Int): DataFrame

  protected def _iterate(sourceDF: DataFrame, iteration: Int): (DataFrame, DataFrame) = {
    import IntervalColumns._

    val sourceInputLookaheadWindow = Window
      .partitionBy(KEY, BUCKET)
      .orderBy(FROM, TO)
      .rowsBetween(1, config.aiListConfig.intervalsCountToCheckLookahead)

    val preparedDF = sourceDF
      .withColumn("_ailist_lookahead",                    F.collect_list(TO).over(sourceInputLookaheadWindow))
      .withColumn("_ailist_lookahead_overlapping",        F.filter(F.col("_ailist_lookahead"), _ <= F.col(TO)))
      .withColumn("_ailist_lookahead_overlapping_count",  F.size(F.col("_ailist_lookahead_overlapping")))
      .withColumn("_ailist_lookahead_overlapping_keeper", F.col("_ailist_lookahead_overlapping_count") < F.lit(config.aiListConfig.intervalsCountToTriggerExtraction))

    val extractedDF  = preparedDF
      .filter(F.col("_ailist_lookahead_overlapping_keeper") === true)
      .withColumn(_COMPONENT, F.lit(iteration))
      .drop(
        "_ailist_lookahead",
        "_ailist_lookahead_overlapping",
        "_ailist_lookahead_overlapping_count",
        "_ailist_lookahead_overlapping_keeper"
      )

    val leftoversDF = preparedDF
      .filter(F.col("_ailist_lookahead_overlapping_keeper") === false)
      .drop(
        "_ailist_lookahead",
        "_ailist_lookahead_overlapping",
        "_ailist_lookahead_overlapping_count",
        "_ailist_lookahead_overlapping_keeper"
      )

    (extractedDF, leftoversDF)
  }

  protected def calculateMaxEnd(dataFrame: DataFrame): DataFrame = {
    import IntervalColumns._

    val maxEndWindow = Window
      .partitionBy(KEY, BUCKET, _COMPONENT)
      .orderBy(FROM, TO)
      .rowsBetween(Window.unboundedPreceding, 0)

    dataFrame
      .withColumn(_MAX_E, F.max(TO).over(maxEndWindow))
  }
}
