package me.kosik.interwalled.spark.join

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.kosik.interwalled.domain.{Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.{DataFrame, Dataset, functions => f}
import org.scalatest.funsuite.AnyFunSuite


abstract class AbstractIntervalJoinTestSuite extends AnyFunSuite with DataFrameSuiteBase {

  def inputSizes: Array[Long] = {
    if(sys.env.getOrElse("INTERWALLED_RUN_100K", "FALSE") != "FALSE")
      Array(100L, 1_000L, 10_000L, 100_000L)
    else if(sys.env.getOrElse("INTERWALLED_RUN_10K", "FALSE") != "FALSE")
      Array(100L, 1_000L, 10_000L)
    else
      Array(100L, 1_000L)
  }

  def inputPartitions: Array[(Int, Int)] = Array((1, 1), (4, 4))

  def inputSuites: Array[String] = Array("one-to-all", "one-to-many", "one-to-one")

  def intervalJoin: IntervalJoin

  // ---------------------------------------------------------------------------------------------------------------- //

  def assertDataEqual(expected: Dataset[IntervalsPair[String]], actual: Dataset[IntervalsPair[String]]): Unit = {
    import expected.sparkSession.implicits._

    def prepareResult(data: Dataset[IntervalsPair[String]]): DataFrame = data
      .toDF()
      .select(
        $"lhs.from" .as("lhs_from"),
        $"lhs.to"   .as("lhs_to"),
        $"lhs.key"  .as("lhs_key"),
        $"lhs.value".as("lhs_value"),
        $"rhs.from" .as("rhs_from"),
        $"rhs.to"   .as("rhs_to"),
        $"rhs.key"  .as("rhs_key"),
        $"rhs.value".as("rhs_value"),
      )

    assertDataFrameDataEquals(expected = prepareResult(expected), result = prepareResult(actual))
  }

  // ---------------------------------------------------------------------------------------------------------------- //

  inputSizes foreach { inputSize => inputSuites.foreach { inputSuite =>
    inputPartitions foreach { case (lhsPartitions, rhsPartitions) =>

      test(s"$inputSize rows, $inputSuite, $lhsPartitions x $rhsPartitions partitions") {
        import spark.implicits._

        def loadInput(datasetName: String): Dataset[Interval[String]] =
          spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
            .as[Interval[String]]

        def loadResult(datasetName: String): Dataset[IntervalsPair[String]] =
          spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
            .as[IntervalsPair[String]]

        val lhs = loadInput("in-lhs").as[Interval[String]].repartition(lhsPartitions)
        val rhs = loadInput("in-rhs").as[Interval[String]].repartition(rhsPartitions)

        val expected = loadResult("out-result")
        val actual = intervalJoin.join(lhs, rhs)

        assertDataEqual(expected = expected, actual = actual)
      }

      test(s"Multiple keys, ${inputSize * 4} rows, $inputSuite, $lhsPartitions x $rhsPartitions partitions") {
        import spark.implicits._

        def loadInput(datasetName: String): Dataset[Interval[String]] =
          spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
            .drop(IntervalColumns.KEY)
            .withColumn(IntervalColumns.KEY, f.explode(f.array(f.lit("CH-1"), f.lit("CH-2"), f.lit("CH-3"), f.lit("CH-4"))))
            .as[Interval[String]]


        def loadResult(datasetName: String): Dataset[IntervalsPair[String]] =
          spark.read.parquet(s"data/$inputSuite/$inputSize/$datasetName.parquet")
            .as[IntervalsPair[String]]
            .flatMap(pair => Array("CH-1", "CH-2", "CH-3", "CH-4").map {
              key => pair.copy(key = key, lhs = pair.lhs.copy(key = key), rhs = pair.rhs.copy(key = key))
            })

        val lhs = loadInput("in-lhs").as[Interval[String]].repartition(lhsPartitions)
        val rhs = loadInput("in-rhs").as[Interval[String]].repartition(rhsPartitions)

        val expected = loadResult("out-result")
        val actual = intervalJoin.join(lhs, rhs)

        assertDataEqual(expected = expected, actual = actual)
      }
    }
  }}
}
