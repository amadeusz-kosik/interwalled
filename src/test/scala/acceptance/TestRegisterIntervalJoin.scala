package com.eternalsh.interwalled
package acceptance

import com.eternalsh.interwalled.spark.IntervalJoinStrategy
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.Seq
import scala.util.Random

class TestRegisterIntervalJoin extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfter {

  val lhsSchema: StructType = StructType(Array(
    StructField("start", LongType),
    StructField("end",   LongType)
  ))

  val rhsSchema: StructType = StructType(Array(
    StructField("start", LongType),
    StructField("end", LongType)
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
    createTempView(1000000L, "view_lhs", lhsSchema, i => Row(i, i + 1))
    createTempView(1000000L, "view_rhs", rhsSchema, i => Row(i, i + 1))
  }

  test("Without registering IntervalJoinStrategy") {
    spark.sqlContext
      .sql("""SELECT * FROM view_lhs lhs INNER JOIN view_rhs rhs ON (lhs.start <= rhs.end AND rhs.start <= lhs.end)""")
      .explain()
  }

  test("Register IntervalJoinStrategy") {
    spark.experimental.extraStrategies = new IntervalJoinStrategy(spark) :: Nil

    spark.sqlContext
      .sql("""SELECT * FROM view_lhs lhs INNER JOIN view_rhs rhs ON (lhs.start <= rhs.end AND rhs.start <= lhs.end)""")
      .explain()
  }
}
