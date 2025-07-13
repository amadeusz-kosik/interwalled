package me.kosik.interwalled.test.data.generator.test.cases

import me.kosik.interwalled.domain.IntervalColumns
import me.kosik.interwalled.domain.test.{TestDataRow, TestResultRow}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.{functions => F}


case class TestContinuous(clustersCount: Int, rowsPerCluster: Long, rowsPerJoin: Int) extends TestCase {

  override def testCaseName: String = s"continuous-$rowsPerJoin"

  override def generateLHS(implicit spark: SparkSession): Dataset[TestDataRow] = {
    import spark.implicits._

    TestDataGenerator
      .generateLinear(clustersCount, rowsPerCluster / rowsPerJoin)
      .select(
        ((F.col("from") - F.lit(1)) * F.lit(rowsPerJoin)).as("from"),
        (F.col("to") * F.lit(rowsPerJoin)).as("to"),
        F.col("key"),
        F.concat(
          F.col("key"),
          F.lit("-"),
          (F.col("from") - F.lit(1)) * F.lit(rowsPerJoin),
          F.lit("-"),
          F.col("to") * F.lit(rowsPerJoin)
        ).as("value")
      )
      .as[TestDataRow]
  }

  override def generateRHS(implicit spark: SparkSession): Dataset[TestDataRow] =
    TestDataGenerator.generateLinear(clustersCount, rowsPerCluster)

  override def generateResult(implicit spark: SparkSession): Option[Dataset[TestResultRow]] = Some {
    import spark.implicits._
    import IntervalColumns._

    val lhs = generateLHS
    val rhs = generateRHS

    (lhs join rhs)
      .filter((lhs.col("from") <= rhs.col("to")) and (rhs.col("from") <= lhs.col("to")))
      .select(
        lhs.col(KEY)     .alias(KEY),

        F.struct(
          lhs.col(KEY)   .alias(KEY),
          lhs.col(FROM)  .alias(f"${FROM}"),
          lhs.col(TO)    .alias(f"${TO}"),
          lhs.col(VALUE) .alias(f"${VALUE}")
        ).alias("lhs"),

        F.struct(
          rhs.col(KEY)   .alias(KEY),
          rhs.col(FROM)  .alias(f"${FROM}"),
          rhs.col(TO)    .alias(f"${TO}"),
          rhs.col(VALUE) .alias(f"${VALUE}")
        ).alias("rhs")
      )
      .as[TestResultRow]
  }
}
