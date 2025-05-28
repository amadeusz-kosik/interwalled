package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.join._
import me.kosik.interwalled.benchmark.utils.{BenchmarkCallback, BenchmarkRunner, CSV}
import me.kosik.interwalled.main.MainEnv
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{File, FileOutputStream, PrintWriter, Writer}
import java.nio.file.{Files, Path}


object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)
  private val env = MainEnv.build()

  logger.info(f"Running environment: $env.")
  logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

  val Array(dataSuite, clustersCount, clustersSize, benchmarkName) = args.take(4)
  val benchmarkArgs = args.drop(4)

  private val benchmark: BenchmarkCallback = benchmarkName match {
    case "broadcast-ailist" =>
      BroadcastAIListBenchmark.prepareBenchmark

    case "native-ailist" =>
      NativeAIListBenchmark.prepareBenchmark

    case "partitioned-ailist" =>
      new PartitionedAIListBenchmark(benchmarkArgs(0).toInt).prepareBenchmark

    case "partitioned-native-ailist-benchmark" =>
      new PartitionedNativeAIListBenchmark(benchmarkArgs(0).toInt, benchmarkArgs(1).toInt).prepareBenchmark

    case "spark-native-bucketing" =>
      new SparkNativeBucketingBenchmark(benchmarkArgs(0).toInt).prepareBenchmark
  }

  // --------------------------------------------------------------------

  private implicit val spark: SparkSession =
    env.buildSparkSession(s"InterwalledBenchmark - ${benchmark.description} - ${dataSuite} - ${clustersCount} - ${clustersSize}")

  private val csvWriter: Writer = {
    val csvPathStr = f"./jupyter-lab/data/${benchmark.description}-${dataSuite}-${clustersCount}-${clustersSize}.csv"
    val csvPath = Path.of(csvPathStr)

    if(! Files.exists(csvPath)) {
      val writer = new PrintWriter(csvPathStr)
      writer.write(CSV.header)
      writer.flush()

      writer
    } else {
      val stream = new FileOutputStream(new File(csvPathStr), true)
      val writer = new PrintWriter(stream)

      writer
    }
  }

  BenchmarkRunner.run(
    benchmark,
    env.dataDirectory,
    clustersCount.toInt,
    clustersSize.toLong,
    dataSuite,
    csvWriter,
    env.timeoutAfter
  )

  csvWriter.close()
}

