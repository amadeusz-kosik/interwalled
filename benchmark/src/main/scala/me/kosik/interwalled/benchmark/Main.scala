package me.kosik.interwalled.benchmark

import me.kosik.interwalled.domain.{Interval, IntervalColumns}
import me.kosik.interwalled.spark.join._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}

import scala.io.StdIn


object Main extends App {
  private val Array(sparkMaster, databasePath, queryPath, joinAlgorithmName, driverMemory) = args

  private val joinAlgorithm: IntervalJoin = joinAlgorithmName match {
    case "BroadcastAIList" => BroadcastAIListIntervalJoin
    case "BroadcastPartitionedAIList" => new BroadcastPartitionedAIListIntervalJoin(10_000)
    case "BroadcastPartitionedMinMaxAIList" => BroadcastPartitionedMinMaxAIListIntervalJoin
    case "PartitionedAIList" => PartitionedAIListIntervalJoin
    case "SparkNaive" => SparkNativeIntervalJoin
  }

  private val spark: SparkSession = SparkSession.builder()
    .appName("InterwalledBenchmark")
    .config("spark.driver.memory", driverMemory)
    .master(sparkMaster)
    .getOrCreate()


  private def parse(input: DataFrame): Dataset[Interval[String]] = {
    import spark.implicits._

    input
      .withColumn("text", F.split(F.col("value"), "\\s+"))
      .select(
        F.col("text").getItem(0).alias(IntervalColumns.KEY),
        F.col("text").getItem(1).cast(LongType).alias(IntervalColumns.FROM),
        F.col("text").getItem(2).cast(LongType).alias(IntervalColumns.TO),
        F.col("value").alias(IntervalColumns.VALUE)
      )
      .as[Interval[String]]
  }


  private def run(): Unit = {
    val database = spark.read.text(databasePath)
      .transform(parse)

    val query = spark.read.text(queryPath)
      .transform(parse)

    joinAlgorithm
      .join(database, query)
      .foreach (_ => ())
  }


  spark.time {
    run()
  }

  StdIn.readLine()
}