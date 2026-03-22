package me.kosik.interwalled.benchmark.test.suite.unit

import me.kosik.interwalled.ailist.model.{Interval, IntervalsPair}
import me.kosik.interwalled.benchmark.app.ApplicationEnv
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe._


case class UnitTestDataSuite(
  name:           String,
  databasePath:   String,
  queryPath:      String,
  resultPath:     String
) {

  private def loadInput[T <: Product: TypeTag](path: String, env: ApplicationEnv): Dataset[T] = {
    import env.sparkSession.implicits.newProductEncoder

    env.sparkSession.read
      .parquet(f"${env.dataDirectory}/$path")
      .as[T]
  }

  def loadDatabase(env: ApplicationEnv): Dataset[Interval] =
    loadInput[Interval](f"unit-test-data/$databasePath.parquet", env)

  def loadQuery(env: ApplicationEnv): Dataset[Interval] =
    loadInput[Interval](f"unit-test-data/$queryPath.parquet", env)

  def loadResults(env: ApplicationEnv): Dataset[IntervalsPair] = {
    import env.sparkSession.implicits.newProductEncoder
    import org.apache.spark.sql.functions.col

    env.sparkSession.read
      .parquet(f"${env.dataDirectory}/unit-test-data-results/$resultPath.parquet")
      .select(
        col("key"),
        col("lhs.from") .as("lhsFrom"),
        col("lhs.to")   .as("lhsTo"),
        col("lhs.value").as("lhsValue"),
        col("rhs.from") .as("rhsFrom"),
        col("rhs.to")   .as("rhsTo"),
        col("rhs.value").as("rhsValue")
      )
      .as[IntervalsPair]
  }
}

object UnitTestDataSuite {

  val ALL_SUITES = Array(
    UnitTestDataSuite("single-point-to-single-point", "single-point", "single-point", "single-point-single-point-result")
  )
}