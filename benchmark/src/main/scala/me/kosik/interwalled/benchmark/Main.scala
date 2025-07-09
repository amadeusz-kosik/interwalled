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

  val Array(dataSuite, benchmarkName, outputCSVPath) = args.take(3)

  private val benchmark: BenchmarkCallback = benchmarkName match {
    case "bucketed-native-ailist-1000-1000" =>
      new NativeAIListBenchmark(1000, Some(1000)).prepareBenchmark

    case "bucketed-rdd-ailist-1000" =>
      new RDDAIListBenchmark(Some(1000)).prepareBenchmark

    case "bucketed-spark-native-1000" =>
      new SparkNativeBenchmark(Some(1000)).prepareBenchmark

    case "driver-ailist" =>
      DriverAIListBenchmark.prepareBenchmark

    case "native-ailist-1000" =>
      new NativeAIListBenchmark(1000, None).prepareBenchmark

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

