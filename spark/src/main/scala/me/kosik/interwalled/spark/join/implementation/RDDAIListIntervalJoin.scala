package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.{AIListConfiguration, IntervalsPair}
import me.kosik.interwalled.ailist.{AIList, AIListBuilder, AIListSplitter, IntervalColumns}
import me.kosik.interwalled.model.BucketedInterval
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.PreparedInput
import me.kosik.interwalled.spark.join.implementation.RDDAIListIntervalJoin.Config
import me.kosik.interwalled.spark.join.implementation.model.RDDAIListIntervalRow
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

    val lhsRDD = lhsData
      .repartition(F.col(IntervalColumns.KEY), F.col(IntervalColumns.BUCKET))
      .sortWithinPartitions(F.col(IntervalColumns.FROM).asc, F.col(IntervalColumns.TO).asc)
      .rdd

    val aiListsLHS = createAILists(lhsRDD)
    val aiListsRHS = rhsData
      .repartition(F.col(IntervalColumns.KEY), F.col(IntervalColumns.BUCKET))
      .rdd
      .map(interval => (interval.bucket, interval.key) -> interval)

    val groupedRDD = aiListsLHS
      .cogroup(aiListsRHS)
      .flatMap { case ((bucket, key), (aiLists, intervals)) =>
        aiLists.map { aiList => (bucket, key) -> (aiList, intervals)}
      }

    val joinedRDD = groupedRDD
      .flatMap { case ((bucket, _), (aiList, rhsIntervalsIterator)) =>
        val rhsIntervals = rhsIntervalsIterator

        val intervalsPairs: Iterable[IntervalsPair] = for {
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

          val aiList = aiListBuilder.build()
          logInfo(s"Created AIList for $key: ${aiList.length} elements, in ${aiList.componentsLengths.toSeq} layout.")

          ((_bucket, key), aiListBuilder.build())
        }
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