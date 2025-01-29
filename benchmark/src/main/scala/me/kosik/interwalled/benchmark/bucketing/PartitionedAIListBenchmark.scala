package me.kosik.interwalled.benchmark.bucketing

import me.kosik.interwalled.benchmark.{Benchmark, Timer}
import me.kosik.interwalled.domain.Interval
import me.kosik.interwalled.spark.join.SparkNativeIntervalJoin
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

object PartitionedAIListBenchmark extends Benchmark {

  override def run(
    database: Dataset[Interval[String]],
    query: Dataset[Interval[String]],
    spark: SparkSession,
    extraArguments: Array[String]
  ): Unit = {
    val buckets = extraArguments(0).split(",").map(_.toLong)

    val results = buckets map { bucketsCount =>
      val joinImplementation = new SparkNativeIntervalJoin(bucketsCount)
      val timer = Timer.start()

      Try {
        val result = joinImplementation.join(database, query)
        result.foreach(_ => ())

        val msElapsed = timer.millisElapsed()
        s"PartitionedAIList for $bucketsCount buckets took $msElapsed ms."
      } getOrElse {
        val msElapsed = timer.millisElapsed()
        s"PartitionedAIList failed for $bucketsCount buckets in $msElapsed ms."
      }
    }

    println("Results:")
    results.foreach(r => println(s"\t${r}"))
  }
}
