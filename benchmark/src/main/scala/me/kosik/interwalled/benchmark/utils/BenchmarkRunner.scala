package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.Writer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object BenchmarkRunner { // FIXME this object is a mess
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    testData: TestData,
    benchmarkCallback: BenchmarkCallback,
    outputWriter: Writer,
    timeoutAfter: Duration
  )(implicit sparkSession: SparkSession): Unit = {

    val appName = f"${benchmarkCallback.description} on $testData"

    logger.info(s"Running benchmark - $appName")
    implicit val executionContext: ExecutionContext =
      scala.concurrent.ExecutionContext.Implicits.global

    import scala.concurrent._
    val benchmarkTimer = Timer.start()

    val doContinueBenchmark: Boolean = Await.result(
      Future {
        val resultDataset = benchmarkCallback.fn(testData)
        // Force Spark to compute the data
        // Discard the result to not pollute benchmarks with output I/O
        resultDataset.foreach(_ => ())
      } transform {
        case fail @ Failure(_: TimeoutException) =>
          Success((fail, false))

        case fail @ Failure(_: Exception) =>
          Success((fail, false))

        case success @ Success(_) =>
          Success((success, true))

      } transform { case Success((result, doContinue)) =>
        val elapsedTime = benchmarkTimer.millisElapsed()

        // Second phase: statistics
        val statistics = benchmarkCallback.statistics(testData)

        val benchmarkResult = BenchmarkResult(
          testData.suite,
          benchmarkCallback.description,
          elapsedTime,
          result,
          statistics
        )

        outputWriter.write(CSV.row(benchmarkResult))
        outputWriter.flush()

        Success(doContinue)
      }, timeoutAfter)

      if(! doContinueBenchmark) {
        logger.info("No point in carry out timed out benchmark for larger dataset, aborting.")
        testData.sparkSession.stop()
        sys.exit(4)
      }
  }
}