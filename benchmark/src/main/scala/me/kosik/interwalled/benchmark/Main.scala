package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.app.{Benchmark, MainEnv, TestDataGenerator}
import org.slf4j.LoggerFactory

object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)

  args.headOption match {
    case Some("benchmark") =>
      val env = MainEnv.build("Interwalled - benchmark")

      logger.info(f"Running environment: $env.")
      logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

      Benchmark.run(args.tail, env)

    case Some("test-data-generator") =>
      val env = MainEnv.build("Interwalled - test data generator")

      logger.info(f"Running environment: $env.")
      logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

      TestDataGenerator.run(env)

    case None =>
      System.err.println("This app requires at least one argument.")
      System.exit(1)

    case unknown =>
      System.err.println(s"Unknown argument: $unknown. Use one of: benchmark, test-data-generator.")
      System.exit(1)
  }
}
