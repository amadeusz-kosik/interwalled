package me.kosik.interwalled.spark.join.implementation

import me.kosik.interwalled.ailist.model.{AIListConfiguration, Interval, IntervalsPair}
import me.kosik.interwalled.ailist.{AIList, AIListBuilder, AIListSplitter}
import me.kosik.interwalled.model.BucketedInterval
import me.kosik.interwalled.spark.join.api.model.IntervalJoin
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, functions => F}

import java.util
import scala.collection.JavaConverters._


class RDDAIListIntervalJoin(aiListConfiguration: AIListConfiguration) extends Serializable with Logging {

  protected val name: String =
    s"rdd-ailist" // FIXME

  def buildAILists(dataset: Dataset[Interval]) = {
    val inputRDD = dataset.rdd

    val mappedRDD = inputRDD.mapPartitions { partition =>
      // Group intervals by keys, create map of (key, bucket) -> intervals
      // FIXME: 2N memory usage, may be optimized

      partition
        .toArray
        .groupBy(_.key)
        .map { case (key, intervals) =>
          val intervalsArrayList = new util.ArrayList[Interval](intervals.length)

          intervals
            .sortBy(i => (i.from, i.to))
            .foreach(interval => intervalsArrayList.add(interval))

          val aiListBuilder: AIListBuilder = new AIListBuilder(aiListConfiguration, intervalsArrayList)
          val aiList = aiListBuilder.build()

          (key, aiList)
        }
        .toIterator
    }

    val splitRDD = mappedRDD
      .flatMap { case (key, list) =>
        AIListSplitter.split(list).toSeq.map(aiList => (key, aiList))
      }

    // FIXME
    // - figure out how to store AILists
    // - figure out how to load them afterwards

    split
  }

  def join(input: IntervalJoin.Input): Dataset[IntervalsPair] = {
    import input.lhsData.sparkSession.implicits._

    val (lhsData, rhsData, rhsDataCount) = {
      // Detect which dataset is bigger, use _smaller_ one as the dataset.
      //  The idea is to keep AI List computation smaller to create smaller partitions.
      val lhsDataCount = input.lhsData.count()
      val rhsDataCount = input.rhsData.count()

      if(lhsDataCount < rhsDataCount)
        (input.lhsData, input.rhsData, rhsDataCount)
      else
        (input.rhsData, input.lhsData, lhsDataCount)
    }

    val (lhsSaltedData, rhsSaltedData) = {
      val partitions = Math.max(1, rhsDataCount / 2000.0).toInt
//      logInfo(s"Salting with factor: $saltingFactor.")

//      // LHS: split into smaller parts
//      // RHS: multiply rows
//      (
//        lhsData
//          .withColumn("bucket", F.floor(F.rand() * saltingFactor).cast(DataTypes.IntegerType))
//          .as[BucketedInterval]
//          .repartition(F.col("bucket"), F.col("key")),
//        rhsData
//          .select(
//            F.explode(F.lit((0 until saltingFactor).toArray)).as("bucket"),
//            F.col("key"),
//            F.col("from"),
//            F.col("to"),
//            F.col("value")
//          )
//          .as[BucketedInterval]
//          .repartition(F.col("bucket"), F.col("key"))
//      )

      (
        lhsData.withColumn("bucket", F.lit("")).repartition(partitions).as[BucketedInterval],
        rhsData.withColumn("bucket", F.lit("")).repartition(partitions).as[BucketedInterval]
      )
    }

    val aiListsLHS = createAILists(lhsSaltedData.rdd)

    val aiListsRHS = rhsSaltedData.rdd
      .mapPartitions { partition =>
        partition.toArray
          .groupBy { interval => (interval.bucket, interval.key) }
          .mapValues(_.map(_.withoutBucketing))
          .toIterator
      }

    val productRDD = aiListsLHS
      .cartesian(aiListsRHS)

    val joinedRDD = productRDD
      .mapPartitions(_.flatMap { case (((lhsBucket, lhsKey), aiList), ((rhsBucket, rhsKey), intervals)) =>
        if((lhsBucket == rhsBucket) && (lhsKey == rhsKey)) {
          intervals.flatMap { interval =>
            aiList
              .overlapping(interval)
              .asScala
              .map(lhsInterval => IntervalsPair(lhsInterval, interval))
          }
        } else {
          Seq.empty
        }
      })

//    val joinedRDD = aiListsLHS
//      .join(aiListsRHS)
//      .flatMap { case (_, (aiList, intervals)) =>
//        intervals.flatMap { interval =>
//          aiList
//            .overlapping(interval)
//            .asScala
//            .map(lhsInterval => IntervalsPair(lhsInterval, interval))
//        }
//      }

    joinedRDD.toDS()
  }

  private def createAILists(inputRDD: RDD[BucketedInterval]): RDD[((String, String), AIList)] = {

  }
}
