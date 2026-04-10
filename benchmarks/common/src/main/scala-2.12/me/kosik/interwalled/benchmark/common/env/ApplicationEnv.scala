package me.kosik.interwalled.benchmark.common.env

import java.nio.file.Path

case class ApplicationEnv(
  csvDirectory: Path,
  dataDirectory: Path
)


object ApplicationEnv {

  def buildMain(): ApplicationEnv =
    ApplicationEnv.build(sys.env)

  private def build(envVariables: Map[String, String]): ApplicationEnv = {
    val csvDirectory  = envVariables.getOrElse("IW_CSV_DIRECTORY",  "/mnt/results/benchmark/default.csv")
    val dataDirectory = envVariables.getOrElse("IW_DATA_DIRECTORY", "/mnt/data")

    ApplicationEnv(
      Path.of(csvDirectory),
      Path.of(dataDirectory)
    )
  }
}
