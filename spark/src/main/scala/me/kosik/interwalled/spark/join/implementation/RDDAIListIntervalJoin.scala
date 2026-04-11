package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.{AIListConfiguration, Interval, IntervalsPair}
import me.kosik.interwalled.ailist.{AIList, AIListBuilder, AIListSplitter, IntervalColumns}
import me.kosik.interwalled.model.BucketedInterval
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin.Config
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, functions => F}

import java.util
import scala.collection.JavaConverters._


class RDDAIListIntervalJoin(override val config: Config) extends ExecutorIntervalJoin {

  protected val name: String =
    s"rdd-ailist" // FIXME

  protected def doJoin(input: PreparedInput): Dataset[IntervalsPair] = {
    import input.lhsData.sparkSession.implicits._

    val (lhsData, rhsData) = {
      // Detect which dataset is bigger, use _smaller_ one as the dataset.
      //  The idea is to keep AI List computation smaller to create smaller partitions.
      if(input.lhsData.rdd.count() < input.rhsData.count())
        (input.lhsData, input.rhsData)
      else
        (input.rhsData, input.lhsData)
    }

    val aiListsLHS = createAILists(lhsData.rdd)
    val aiListsRHS = rhsData.rdd
      .mapPartitions { partition =>
        partition.toArray
          .groupBy { interval => (interval.bucket, interval.key) }
          .mapValues(_.map(_.withoutBucketing))
          .toIterator
      }

    val joinedRDD = aiListsLHS
      .join(aiListsRHS)
      .flatMap { case (_, (aiList, intervals)) =>
        intervals.flatMap { interval =>
          aiList
            .overlapping(interval)
            .asScala
            .map(lhsInterval => IntervalsPair(lhsInterval, interval))
        }
      }

    joinedRDD.toDS()
  }

  private def createAILists(inputRDD: RDD[BucketedInterval]): RDD[((String, String), AIList)] = {
    val mapped = inputRDD.mapPartitions { partition =>
      // Group intervals by keys, create map of (key, bucket) -> intervals
      // FIXME: 2N memory usage, may be optimized

      partition
        .toArray
        .groupBy { interval => (interval.bucket, interval.key) }
        .map { case ((bucket, key), intervals) =>
          val intervalsArrayList = new util.ArrayList[Interval](intervals.length)

          intervals
            .sortBy(i => (i.from, i.to))
            .foreach(bucketedInterval => intervalsArrayList.add(bucketedInterval.withoutBucketing))

          val aiListBuilder: AIListBuilder = new AIListBuilder(config.aiListConfig, intervalsArrayList)
          val aiList = aiListBuilder.build()

          logInfo(s"Created AIList for ($bucket, $key): ${aiList.length} elements.")
          ((bucket, key), aiList)
        }
        .toIterator
    }

    val split = mapped.flatMap { case (key, list) =>
        AIListSplitter.split(list).toSeq.map(aiList => (key, aiList))
    }

    split
  }
}

object RDDAIListIntervalJoin {
  case class Config(aiListConfig: AIListConfiguration, override val preprocessorConfig: PreprocessorConfig)
    extends ExecutorConfig with Serializable
}