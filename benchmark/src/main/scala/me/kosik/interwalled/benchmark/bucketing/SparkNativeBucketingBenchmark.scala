package me.kosik.interwalled.benchmark.bucketing

import me.kosik.interwalled.benchmark.{Benchmark, Timer}
import me.kosik.interwalled.domain.Interval
import me.kosik.interwalled.spark.join.SparkNativeIntervalJoin
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

object SparkNativeBucketingBenchmark extends Benchmark {

  override def run(
    database: Dataset[Interval[String]],
    query: Dataset[Interval[String]],
    spark: SparkSession,
    extraArguments: Array[String]
  ): Unit = {
    val buckets = extraArguments(0).split(",").map(_.toLong)

    buckets foreach { bucketsCount =>
      val joinImplementation = new SparkNativeIntervalJoin(bucketsCount)
      val timer = Timer.start()

      Try {
        val result = joinImplementation.join(database, query)
        result.foreach(_ => ())

        val msElapsed = timer.millisElapsed()
        println(s"SparkNative for $bucketsCount buckets took $msElapsed ms.")
      } getOrElse {
        val msElapsed = timer.millisElapsed()
        println(s"SparkNative failed for $bucketsCount buckets in $msElapsed ms.")
      }

    }

  }
}
