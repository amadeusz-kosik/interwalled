package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.data.{TestData, TestDataLoader}
import me.kosik.interwalled.benchmark.join._
import me.kosik.interwalled.benchmark.utils.csv.CSVWriter
import me.kosik.interwalled.benchmark.utils.{BenchmarkCallback, BenchmarkRunner}
import me.kosik.interwalled.main.MainEnv
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)
  private val env = MainEnv.build()

  logger.info(f"Running environment: $env.")
  logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

  private val Array(dataSuite, databasePath, queryPath, outputCSVPath, joinStrategy) = args.take(5)
  private val joinArguments: Array[String] = args.drop(5)

  private val benchmark: BenchmarkCallback =
    JoinStrategy
      .getByName(joinStrategy, joinArguments)
      .getOrElse(sys.exit(4))

  // --------------------------------------------------------------------

  private implicit val spark: SparkSession =
    env.buildSparkSession(s"Interwalled benchmark - ${benchmark.description} - $dataSuite")

  private val csvWriter: CSVWriter =
    CSVWriter.open(outputCSVPath)

  private val testData: TestData =
    TestDataLoader.load(dataSuite, f"${env.dataDirectory}/$databasePath", f"${env.dataDirectory}/$queryPath")

  BenchmarkRunner
    .run(testData, benchmark, csvWriter, env.timeoutAfter)

  csvWriter.close()
}

