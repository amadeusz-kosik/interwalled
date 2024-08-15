
package me.kosik.interwalled.acceptance

import me.kosik.interwalled.spark.strategy.AIListIntervalJoinStrategy
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TestIntervalJoinCorrectness extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfter with Matchers {

  val lhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  val rhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  def createTempView(range: Long, name: String, schema: StructType, mapping: Long => Row): Unit = {
    val rdd = spark.sparkContext
      .parallelize(1L to range)
      .map(mapping)

    val df = spark
      .createDataFrame(rdd, schema)

    df.createOrReplaceTempView(name)
  }

  before {
    createTempView(10L, "view_lhs", lhsSchema, i => Row("ch-01", i * 2, i * 2 + 1))
    createTempView(10L, "view_rhs", rhsSchema, i => Row("ch-01", i * 2, i * 2 + 1))

    spark.experimental.extraStrategies = new AIListIntervalJoinStrategy(spark) :: Nil
  }

  private val SQL_STRING_SIMPLE = """ |
    | SELECT * FROM view_lhs lhs
    |   INNER JOIN view_rhs rhs ON (
    |     lhs.start <= rhs.end AND
    |     rhs.start <= lhs.end AND
    |     lhs.chromosome = rhs.chromosome
    |   )""".stripMargin


  test("BroadcastAIListIntervalJoinPlan should return valid results") {
    val actual = spark.sqlContext
      .sql(SQL_STRING_SIMPLE)

    actual.count() shouldEqual 10
    actual.show()
  }
}
