package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.{AIListConfiguration, IntervalsPair}
import me.kosik.interwalled.ailist.{AIList, AIListBuilder, IntervalColumns}
import me.kosik.interwalled.model.BucketedInterval
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin.Config
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import java.util
import scala.collection.JavaConverters._


class RDDAIListIntervalJoin(override val config: Config) extends ExecutorIntervalJoin {

  protected val name: String =
    s"rdd-ailist" // FIXME

  protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
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

        val intervalsPairs: Iterable[IntervalsPair] = for {
          aiList              <- aiLists
          rhsBucketedInterval <- rhsIntervals
          rhsInterval          = rhsBucketedInterval.withoutBucketing
          lhsInterval         <- aiList.overlapping(rhsInterval).asScala
        } yield IntervalsPair(lhsInterval, rhsInterval)

        logDebug(s"Intervals pairs computed for $bucket.")
        intervalsPairs
      }

    val joined = joinedRDD
      .toDF()
      .drop(
        input.lhsData.col(IntervalColumns.BUCKET),
        input.rhsData.col(IntervalColumns.BUCKET)
      )
      .as[IntervalsPair]

    joined
  }

  private def createAILists(inputRDD: RDD[BucketedInterval]): RDD[((String, String), AIList)] = {
    val grouped = inputRDD.mapPartitions { partition =>
      partition
        .toList
        .groupBy(bi => (bi.key, bi.bucket))
        .iterator
      }

    val mapped = grouped.mapPartitions { partition =>
      partition
        .map { case ((key, _bucket), intervals) =>
          val aiListBuilder: AIListBuilder = new AIListBuilder(config.aiListConfig)
          intervals.foreach { bucketedInterval => aiListBuilder.put(bucketedInterval.withoutBucketing) }
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