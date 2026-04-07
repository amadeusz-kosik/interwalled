package me.kosik.interwalled.benchmark.sequila

import me.kosik.interwalled.benchmark.common.test.data.TestDataSuiteMetadata
import me.kosik.interwalled.benchmark.common.timer.{Timer, TimerResult}
import me.kosik.interwalled.benchmark.sequila.data.TestDataSuiteLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SequilaSession
import org.slf4j.LoggerFactory


object Benchmark {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def runBenchmark(testDataPath: String, testDataSuites: Array[TestDataSuiteMetadata]): Map[TestDataSuiteMetadata, TimerResult] = {
    val spark          = SparkSession.builder().appName("Sequila benchmark").getOrCreate()
    val sequilaSession = SequilaSession(spark)

    val results = testDataSuites map { testDataSuiteMetadata =>
      logger.info(s"Running test data suite: $testDataSuiteMetadata.")

      val testData = TestDataSuiteLoader.load(testDataPath, testDataSuiteMetadata)(spark)

      val database = testData.database.alias("database")
      val query    = testData.query.alias("query")

      val timerResult = Timer.timed {
        val joined = database.join(query,
          (database.col("from") <= query.col("to")) &&
            (database.col("to")   >= query.col("from")) &&
            (database.col("key") === query.col("key"))
        )

        joined.foreach(_ => ())
      }

      logger.info(s"Test data suite completed in $timerResult ms.")

      (testDataSuiteMetadata, timerResult)
    }

    results.toMap
  }
}
