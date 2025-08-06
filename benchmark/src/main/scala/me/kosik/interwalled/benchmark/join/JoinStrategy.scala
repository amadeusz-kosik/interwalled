package me.kosik.interwalled.benchmark.join

import me.kosik.interwalled.benchmark.join.algorithms._
import me.kosik.interwalled.benchmark.utils.BenchmarkCallback

import scala.util.Try


object JoinStrategy {
  def getByName(name: String, arguments: Array[String]): Try[BenchmarkCallback] =
    Try(_byName.apply(name, arguments))


  private val _byName: PartialFunction[(String, Array[String]), BenchmarkCallback] = {
    case ("bucketed-cached-native-ailist", Array(maximumComponentsCount, bucketScale)) =>
      new CachedNativeAIListBenchmark(maximumComponentsCount.toInt, Some(bucketScale.toLong)).prepareBenchmark

    case ("bucketed-checkpointed-native-ailist", Array(maximumComponentsCount, bucketScale)) =>
      new CachedNativeAIListBenchmark(maximumComponentsCount.toInt, Some(bucketScale.toLong)).prepareBenchmark

    case ("bucketed-rdd-ailist", Array(bucketScale)) =>
      new RDDAIListBenchmark(Some(bucketScale.toLong)).prepareBenchmark

    case ("bucketed-spark-native", Array(bucketScale)) =>
      new SparkNativeBenchmark(Some(bucketScale.toLong)).prepareBenchmark

    case ("cached-native-ailist", Array(maximumComponentsCount)) =>
      new CachedNativeAIListBenchmark(maximumComponentsCount.toInt, None).prepareBenchmark

    case ("checkpointed-native-ailist", Array(maximumComponentsCount)) =>
      new CheckpointedNativeAIListBenchmark(maximumComponentsCount.toInt, None).prepareBenchmark

    case ("driver-ailist", arguments) if arguments.isEmpty =>
      DriverAIListBenchmark.prepareBenchmark

    case ("spark-native", arguments) if arguments.isEmpty =>
      new SparkNativeBenchmark(None).prepareBenchmark
  }

}
