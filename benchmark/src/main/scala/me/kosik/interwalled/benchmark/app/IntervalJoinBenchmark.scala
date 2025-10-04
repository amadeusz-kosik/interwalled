package me.kosik.interwalled.benchmark.app

import me.kosik.interwalled.benchmark.join._
import me.kosik.interwalled.benchmark.test.suite.TestDataSuites
import me.kosik.interwalled.benchmark.utils.csv.{BenchmarkCSVFormatter, CSVWriter}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}


class IntervalJoinBenchmark(args: Array[String], env: MainEnv) extends BenchmarkApp {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = args match {
    case Array() =>
      System.err.println(s"Available join implementations:")
      JoinStrategies.values.keys.toArray.sorted.foreach { join => System.err.println(s""""$join"""") }

      System.err.println("Available data suites:")
      TestDataSuites.values.keys.toArray.sorted.foreach(suite => System.err.println(s""""$suite""""))

      System.exit(1)

    case Array(dataSuite, joinStrategy) =>
      val joinImplementation = JoinStrategies.values
        .getOrElse(joinStrategy, {
          System.err.println(
            s"Unknown join implementation of $joinStrategy. " +
            s"Available ones:"
          )
          JoinStrategies.values.keys.toList.sorted.map("\"" + _ + "\"").foreach(System.err.println)
          sys.exit(2)
        })

      val testDataSuite = TestDataSuites.values
        .getOrElse(dataSuite, {
          System.err.println(
            s"Unknown data suite of $dataSuite. " +
              s"Available ones:"
          )
          TestDataSuites.values.keys.toList.sorted.map("\"" + _ + "\"").foreach(System.err.println)
          sys.exit(3)
        })

      // --------------------------------------------------------------------

      val csvWriter: CSVWriter[BenchmarkResult] =
        CSVWriter.forPath[BenchmarkResult](BenchmarkCSVFormatter)(env.csvDirectory)

      logger.info(s"Running case $joinStrategy.")
      logger.info(s"Running on $testDataSuite.")

      val benchmarkRequest = BenchmarkRequest(testDataSuite, joinImplementation)
      val benchmarkResult  = BenchmarkRunner.run(benchmarkRequest, env)

      csvWriter.write(benchmarkResult)
      csvWriter.close()

      env.sparkSession.stop()

      benchmarkResult.result match {
        case Success(_) =>
          System.exit(0)

        case Failure(exception) =>
          logger.error("Benchmarking failed", exception)
          System.exit(4)
      }
  }
}

