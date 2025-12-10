package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.{AIList, AIListBuilder, BucketedInterval, IntervalColumns, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.config.AIListConfig
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin.Config
import me.kosik.interwalled.spark.join.preprocessor.Preprocessor.PreprocessorConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters._


class RDDAIListIntervalJoin(override val config: Config) extends ExecutorIntervalJoin {

  protected val name: String =
    s"rdd-ailist-${config.aiListConfig.toShortString}" // FIXME

  protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
    import input.lhsData.sparkSession.implicits._

    val aiListsLHS = createAILists(input.lhsData.rdd)
    val aiListsRHS = {
      val rhsInputRDD: RDD[BucketedInterval] = input.rhsData.rdd
      rhsInputRDD.map(interval => (interval.bucket, interval.key) -> interval.toInterval)
    }

    val joinedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case ((bucket, key), (lhsAIListsIterator, rhsIntervalsIterator)) =>
        val aiLists = lhsAIListsIterator
          .toSeq

        val rhsIntervals = rhsIntervalsIterator
          .toSeq

        val intervalsPairs: Iterable[IntervalsPair] = for {
          aiList        <- aiLists
          rhsInterval   <- rhsIntervals
          lhsInterval   <- aiList.overlapping(rhsInterval).asScala
        } yield IntervalsPair(key, lhsInterval, rhsInterval)

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
          val aiListBuilder = new AIListBuilder(config.aiListConfig.toJava)
          intervals.foreach(bi => aiListBuilder.put(bi.toInterval))
          ((_bucket, key), aiListBuilder.build())
        }
    }

    mapped
  }
}

object RDDAIListIntervalJoin {
  case class Config(aiListConfig: AIListConfig, override val preprocessorConfig: PreprocessorConfig)
    extends ExecutorConfig with Serializable
}