package me.kosik.interwalled.benchmark.sequila

import me.kosik.interwalled.benchmark.common.test.data.{TestDataSuiteLoader, TestDataSuites}
import me.kosik.interwalled.benchmark.common.timer.Timer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SequilaSession
import org.slf4j.LoggerFactory


object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  private val spark          = SparkSession.builder().appName("Sequila benchmark").getOrCreate()
  private val sequilaSession = SequilaSession(spark)

  private val testDataSuites = TestDataSuites.databioSuites // FIXME

  testDataSuites foreach { testDataSuiteMetadata =>
    logger.info(s"Running test data suite: $testDataSuiteMetadata.")

    val testData = TestDataSuiteLoader.load("/mnt/data", testDataSuiteMetadata)(spark)

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
  }
}
