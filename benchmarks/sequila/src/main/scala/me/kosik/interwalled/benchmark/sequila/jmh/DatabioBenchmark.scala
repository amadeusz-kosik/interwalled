package me.kosik.interwalled.benchmark.sequila.jmh

import me.kosik.interwalled.benchmark.common.test.data.{TestDataSuiteLoader, TestDataSuiteMetadata}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Param, Scope, Setup, State, Warmup}

import java.util.concurrent.TimeUnit


@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
class DatabioBenchmark {

  @Param(Array(
    "databio-s-1-2"
  ))
  var testDataSuiteName: String = _

  private var sparkSession: SparkSession = _
  private var sequilaSession: SequilaSession = _

  @Setup
  def setup(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("Sequila benchmark.")
      .getOrCreate()
    sequilaSession = SequilaSession(sparkSession)
  }

  @Benchmark
  def load() = {
    implicit val sparkSession:   SparkSession   = this.sparkSession
    implicit val sequilaSession: SequilaSession = this.sequilaSession

    val testDataSuiteMetadata = TestDataSuiteMetadata.databioSuites
      .find(_.suite == testDataSuiteName)
      .getOrElse(throw new IllegalArgumentException(s"Test data suite '$testDataSuiteName' not found."))

    // FIXME
    val testDataSuite = TestDataSuiteLoader.load("data/", testDataSuiteMetadata)
    val joinedData = {
      val database = testDataSuite.database
      val query    = testDataSuite.query
      database
        .join(query,
          (database.col("start") <= query.col("end")) &&
            (database.col("pos_end") >= query.col("start")) &&
            (database.col("key") === query.col("key"))
        )
        .select(
          database.col("start").alias("lhs_start"),
          database.col("end").alias("lhs_end"),
          query   .col("start").alias("rhs_start"),
          query   .col("end").alias("rhs_end"),
          database.col("key")
        )
    }

    joinedData.foreach(_ => ())
  }

}
