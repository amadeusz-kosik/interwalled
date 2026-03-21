package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.AIListConfiguration
import me.kosik.interwalled.ailist.{AIList, AIListBuilder, IntervalColumns}
import me.kosik.interwalled.model.{BucketedInterval, SparkIntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin.Config
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import java.util
import scala.collection.JavaConverters._


class RDDAIListIntervalJoin(override val config: Config) extends ExecutorIntervalJoin {

  protected val name: String =
    s"rdd-ailist-${config.aiListConfig}" // FIXME

  protected def doJoin(input: PreparedInput): Dataset[SparkIntervalsPair] = {
    import input.lhsData.sparkSession.implicits._

    val aiListsLHS = createAILists(input.lhsData.rdd)
    val aiListsRHS = {
      val rhsInputRDD: RDD[BucketedInterval] = input.rhsData.rdd
      rhsInputRDD.map(interval => (interval.bucket, interval.key) -> interval)
    }

    val joinedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case ((bucket, _), (lhsAIListsIterator, rhsIntervalsIterator)) =>
        val aiLists = lhsAIListsIterator
        val rhsIntervals = rhsIntervalsIterator

        val intervalsPairs: Iterable[SparkIntervalsPair] = for {
          aiList            <- aiLists
          rhsSparkInterval  <- rhsIntervals
          rhsInterval        = rhsSparkInterval.toAIListInterval
          lhsInterval       <- aiList.overlapping(rhsInterval).asScala
        } yield SparkIntervalsPair(lhsInterval, rhsInterval)

        logDebug(s"Intervals pairs computed for $bucket.")
        intervalsPairs
      }

    val joined = joinedRDD
      .toDF()
      .drop(
        input.lhsData.col(IntervalColumns.BUCKET),
        input.rhsData.col(IntervalColumns.BUCKET)
      )
      .as[SparkIntervalsPair]

    joined
  }

  private def createAILists(inputRDD: RDD[BucketedInterval]): RDD[((String, String), AIList[String])] = {
    val grouped = inputRDD.mapPartitions { partition =>
      partition
        .toList
        .groupBy(bi => (bi.key, bi.bucket))
        .iterator
      }

    val mapped = grouped.mapPartitions { partition =>
      partition
        .map { case ((key, _bucket), intervals) =>
          val aiListBuilder: AIListBuilder[String] = new AIListBuilder[String](config.aiListConfig)
          intervals.foreach { bucketedInterval => aiListBuilder.put(bucketedInterval.toAIListInterval) }
          ((_bucket, key), aiListBuilder.build())
        }
    }

    mapped
  }
}

object RDDAIListIntervalJoin {
  case class Config(aiListConfig: AIListConfiguration, override val preprocessorConfig: PreprocessorConfig)
    extends ExecutorConfig with Serializable
}