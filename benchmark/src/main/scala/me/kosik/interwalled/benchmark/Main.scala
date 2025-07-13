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

  val Array(dataSuite, outputCSVPath, benchmarkName) = args.take(3)
  val joinArguments = args.drop(3)

  private val benchmark: BenchmarkCallback = benchmarkName match {
    case "bucketed-cached-native-ailist" =>
      new CachedNativeAIListBenchmark(joinArguments(0).toInt, Some(joinArguments(1).toLong)).prepareBenchmark

    case "bucketed-checkpointed-native-ailist" =>
      new CachedNativeAIListBenchmark(joinArguments(0).toInt, Some(joinArguments(1).toLong)).prepareBenchmark

    case "bucketed-rdd-ailist" =>
      new RDDAIListBenchmark(Some(joinArguments(0).toLong)).prepareBenchmark

    case "bucketed-spark-native" =>
      new SparkNativeBenchmark(Some(joinArguments(0).toLong)).prepareBenchmark

    case "driver-ailist" =>
      DriverAIListBenchmark.prepareBenchmark

    case "cached-native-ailist" =>
      new CachedNativeAIListBenchmark(joinArguments(0).toInt, None).prepareBenchmark

    case "checkpointed-native-ailist" =>
      new CheckpointedNativeAIListBenchmark(joinArguments(0).toInt, None).prepareBenchmark

    case "spark-native" =>
      new SparkNativeBenchmark(None).prepareBenchmark
  }

  // --------------------------------------------------------------------

  private implicit val spark: SparkSession =
    env.buildSparkSession(s"InterwalledBenchmark - ${benchmark.description} - $dataSuite")

  private val csvWriter: Writer = {
    if(! Files.exists(Path.of(outputCSVPath))) {
      val writer = new PrintWriter(outputCSVPath)
      writer.write(CSV.header)
      writer.flush()

      writer
    } else {
      val stream = new FileOutputStream(new File(outputCSVPath), true)
      val writer = new PrintWriter(stream)

      writer
    }
  }

  val testData = TestData.load(
    env.dataDirectory,
    dataSuite
  )

  BenchmarkRunner.run(
    testData,
    benchmark,
    csvWriter,
    env.timeoutAfter
  )

  csvWriter.close()
}

