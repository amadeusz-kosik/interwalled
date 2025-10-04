package me.kosik.interwalled.benchmark

import me.kosik.interwalled.benchmark.app.{IntervalJoinBenchmark, MainEnv, PreprocessingBenchmark, TestDataGenerator}
import org.slf4j.LoggerFactory


object Main extends App {

  val logger = LoggerFactory.getLogger(getClass)
  val env = MainEnv.build(args.mkString("Array(", ", ", ")"))

  val app = args.headOption match {
    case Some("interval-join-benchmark") =>
      new IntervalJoinBenchmark(args.tail, env)

    case Some("partitioning-benchmark") =>
      new PreprocessingBenchmark(args.tail, env)

    case Some("test-data-generator") =>
      new TestDataGenerator(env)

    case Some(anythingElse) =>
      throw new IllegalArgumentException(s"Unknown benchmark mode: $anythingElse")

    case None =>
      throw new IllegalArgumentException("This app requires at least one argument.")
  }

  logger.info(f"Running benchmark: ${app.getClass.getName}")
  logger.info(f"Running environment: $env.")
  logger.info(f"Running arguments: ${args.mkString("Array(", ", ", ")")}.")

  app.run()
}
