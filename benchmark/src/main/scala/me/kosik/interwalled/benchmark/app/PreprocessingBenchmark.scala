package me.kosik.interwalled.benchmark.app

import me.kosik.interwalled.benchmark.preprocessing.{PreprocessingRequest, PreprocessingResult, PreprocessingRunner, PreprocessingStrategies}
import me.kosik.interwalled.benchmark.test.suite.TestDataSuites
import me.kosik.interwalled.benchmark.utils.csv.{CSVWriter, PreprocessingCSVFormatter}
import org.slf4j.LoggerFactory


class PreprocessingBenchmark(args: Array[String], env: ApplicationEnv) extends BenchmarkApp {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = args match {
    case Array() =>
      System.err.println(s"Available partitioning implementations:")
      PreprocessingStrategies.values.keys.toArray.sorted.foreach { join => System.err.println(s""""$join"""") }

      System.err.println("Available data suites:")
      TestDataSuites.values.keys.toArray.sorted.foreach(suite => System.err.println(s""""$suite""""))

      System.exit(1)

    case Array(dataSuiteName, preprocessing) =>
      val preprocessor = PreprocessingStrategies.values
        .getOrElse(preprocessing, {
          System.err.println(
            s"Unknown join implementation of $preprocessing. " +
              s"Available ones:"
          )
          PreprocessingStrategies.values.keys.toList.sorted.map("\"" + _ + "\"").foreach(System.err.println)
          sys.exit(2)
        })

      val dataSuite = TestDataSuites.values
        .getOrElse(dataSuiteName, {
          System.err.println(
            s"Unknown data suite of $dataSuiteName. " +
              s"Available ones:"
          )
          TestDataSuites.values.keys.toList.sorted.map("\"" + _ + "\"").foreach(System.err.println)
          sys.exit(3)
        })

      // --------------------------------------------------------------------

      val csvWriter: CSVWriter[PreprocessingResult] =
        CSVWriter.forPath(PreprocessingCSVFormatter)(env.csvDirectory)

      logger.info(s"Running case $preprocessing.")
      logger.info(s"Running on $dataSuite.")

      val preprocessingRequest = PreprocessingRequest(dataSuite, preprocessor)
      val preprocessingResult = PreprocessingRunner.run(preprocessingRequest, env)

      csvWriter.write(preprocessingResult)
      csvWriter.close()

      env.sparkSession.stop()
  }
}
