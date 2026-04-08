package me.kosik.interwalled.benchmark.common.env

import java.nio.file.Path

case class ApplicationEnv(
  csvDirectory: Path,
  dataDirectory: Path
)


object ApplicationEnv {

  def buildMain(): ApplicationEnv =
    ApplicationEnv.build(sys.env)

  def buildTest(): ApplicationEnv = {
    val testDirectories = Map(
      ("INTERWALLED_CSV_DIRECTORY",  sys.env.getOrElse("INTERWALLED_CSV_DIRECTORY",  "/dev/null")),
      ("INTERWALLED_DATA_DIRECTORY", sys.env.getOrElse("INTERWALLED_DATA_DIRECTORY", "data/"))
    )

    ApplicationEnv.build(sys.env ++ testDirectories)
  }

  private def build(envVariables: Map[String, String]): ApplicationEnv = {
    val csvDirectory  = envVariables.getOrElse("INTERWALLED_CSV_DIRECTORY",  "/mnt/results/benchmark/default.csv")
    val dataDirectory = envVariables.getOrElse("INTERWALLED_DATA_DIRECTORY", "/mnt/data")

    ApplicationEnv(
      Path.of(csvDirectory),
      Path.of(dataDirectory)
    )
  }
}
