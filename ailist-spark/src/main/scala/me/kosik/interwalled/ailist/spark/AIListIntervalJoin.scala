package me.kosik.interwalled.ailist.spark

import me.kosik.interwalled.ailist.core.AIListBuilder
import me.kosik.interwalled.ailist.core.model.{Configuration, Interval}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}


class AIListIntervalJoin {
  def join(database: DataFrame, query: DataFrame): DataFrame = {
    import database.sparkSession.implicits._

    val databaseRDD = database
      .select(F.col("key"), F.col("from"), F.col("to"))
      .distinct()
      .rdd
      .mapPartitions { rowsIterator =>
        import scala.jdk.CollectionConverters._

        val spilledIterator =
          rowsIterator.toArray

        val groupedIntervals =
          spilledIterator.groupBy(_.getAs[String]("key"))

        val groupedAILists =
          groupedIntervals.toArray.flatMap { case (key, rows) =>
            val aiListBuilder =
              new AIListBuilder(Configuration.DEFAULT)

            val intervals =
              rows.map { row => new Interval(row.getAs[Long]("from"), row.getAs[Long]("to")) }

            val intervalsBuffer = intervals
              .toBuffer

            val aiLists =
              aiListBuilder.build(new java.util.ArrayList[Interval](intervalsBuffer.asJava))

            val aiListsWithKeys =
              aiLists.asScala.map(key -> _)

            aiListsWithKeys
          }

        groupedAILists.toIterator
      }

    val queryRDD = query
      .select(F.col("key"), F.col("from"), F.col("to"))
      .distinct()
      .rdd
      .mapPartitions { rowsIterator =>
        rowsIterator
          .toArray
          .groupBy(_.getAs[String]("key"))
          .mapValues(_.map(row => new Interval(row.getAs[Long]("from"), row.getAs[Long]("to"))))
          .toIterator
      }


    val joinedRDD =
      databaseRDD.join(queryRDD)

    val filteredRDD =
      joinedRDD.flatMap { case (key, (aiList, queries)) =>
        import scala.jdk.CollectionConverters._

        queries.flatMap { query =>
          aiList.overlapping(query).asScala.map { leftInterval =>
            (key, leftInterval.from(), leftInterval.to(), query.from(), query.to())
          }
        }

      }

    val filteredDF = filteredRDD
      .toDF("key", "lhsFrom", "lhsTo", "rhsFrom", "rhsTo")

    val resultDF = filteredDF
      .join(database,
          (database.col("key")  === filteredDF.col("key")) &&
          (database.col("from") === filteredDF.col("lhsFrom")) &&
          (database.col("to")   === filteredDF.col("lhsTo"))
      )
      .join(database,
          (query.col("key")  === filteredDF.col("key")) &&
          (query.col("from") === filteredDF.col("rhsFrom")) &&
          (query.col("to")   === filteredDF.col("rhsTo"))
      )
      .drop(
        filteredDF.col("key"),
        filteredDF.col("lhsFrom"),
        filteredDF.col("lhsTo"),
        filteredDF.col("rhsFrom"),
        filteredDF.col("rhsTo")
      )

    resultDF
  }
}
