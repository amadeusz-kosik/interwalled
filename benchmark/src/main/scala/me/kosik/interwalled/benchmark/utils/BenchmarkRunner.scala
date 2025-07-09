package me.kosik.interwalled.benchmark.utils

import me.kosik.interwalled.benchmark.{TestData, Timer}
import org.slf4j.LoggerFactory

import java.io.Writer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


object BenchmarkRunner {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    testData: TestData,
    benchmarkCallback: BenchmarkCallback,
    outputWriter: Writer,
    timeoutAfter: Duration
  ): Unit = {

    val appName = f"${benchmarkCallback.description} on $testData"

    logger.info(s"Running benchmark - $appName")
    implicit val executionContext: ExecutionContext =
      scala.concurrent.ExecutionContext.Implicits.global

    import scala.concurrent._
    val benchmarkTimer = Timer.start()

    val resultPromise = Future {
      // Force Spark to compute the data.
      // Discard the result to not pollute benchmarks with output I/O.
      // Interrupt if job takes too long to complete.

      val resultDataset = benchmarkCallback.fn(testData)
      resultDataset.foreach(_ => ())
    }

    val benchmarkResult = Try { Await.result(resultPromise, timeoutAfter) } match {
      case Success(_) =>
        val elapsedTime = benchmarkTimer.millisElapsed()
        val statistics  = benchmarkCallback.statistics(testData)

        BenchmarkResult(
          testData.suite,
          benchmarkCallback.description,
          Success(elapsedTime),
          statistics
        )

      case Failure(fail) =>
        BenchmarkResult(
          testData.suite,
          benchmarkCallback.description,
          Failure(fail),
          None
        )
    }

    outputWriter.write(CSV.row(benchmarkResult))
    outputWriter.flush()

    if(benchmarkResult.result.isFailure) {
      logger.info("No point in carry out timed out benchmark for larger dataset, aborting.")

      testData.sparkSession.stop()
      sys.exit(4)
    }
  }
}