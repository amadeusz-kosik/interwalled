package me.kosik.interwalled.spark.join

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import org.apache.log4j.Level
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{functions => f}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


abstract class AbstractIntervalJoinTestSuite extends AnyFunSuite with DataFrameSuiteBase with Matchers with BeforeAndAfterEach {

  def inputSizes: Array[Long] = Array(100L)

  def inputSuites: Array[String] = Array("one-to-all", "one-to-many", "one-to-one")

  def intervalJoin: IntervalJoin

  // ---------------------------------------------------------------------------------------------------------------- //

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setLogLevel(Level.WARN.toString)
  }

  inputSizes foreach { inputSize => inputSuites.foreach { inputSuite =>
    test(s"$inputSize rows, $inputSuite") {
      import spark.implicits._

      def loadInput(datasetName: String): Dataset[Interval[String]] =
        spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
          .select(
            $"chromosome".as("key"),
            $"from",
            $"to",
            f.lit("CONST_VALUE").as("value")
          )
          .as[Interval[String]]

      def loadResult(datasetName: String): Dataset[IntervalsPair[String]] =
        spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
          .select(
            $"chromosome".as("key"),
            f.struct(
              $"chromosome".as("key"),
              $"fromLHS".as("from"),
              $"toLHS".as("to"),
              f.lit("CONST_VALUE").as("value")
            ).as("lhs"),
            f.struct(
              $"chromosome".as("key"),
              $"fromRHS".as("from"),
              $"toRHS".as("to"),
              f.lit("CONST_VALUE").as("value")
            ).as("rhs")
          )
          .as[IntervalsPair[String]]

      val lhs = loadInput("in-lhs").as[Interval[String]]
      val rhs = loadInput("in-rhs").as[Interval[String]]

      val expected = loadResult("out-result")
      val actual = intervalJoin.join(lhs, rhs)

      assertDataFrameDataEquals(expected = expected.toDF(), result = actual.toDF())
    }
  }}
}
