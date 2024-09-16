package me.kosik.interwalled.acceptance

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait CommonDFSuiteBase extends AnyFunSuite with DataFrameSuiteBase with Matchers {

  lazy val lhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  lazy val rhsSchema: StructType = StructType(Array(
    StructField("chromosome", StringType),
    StructField("start",      LongType),
    StructField("end",        LongType)
  ))

  protected def createDF(range: Long, schema: StructType, mapping: Long => Row): DataFrame = {
    val rdd = spark.sparkContext
      .parallelize(1L to range)
      .map(mapping)

    spark.createDataFrame(rdd, schema)
  }

  protected def getJoinPredicate(lhsDF: DataFrame, rhsDF: DataFrame) =
    (lhsDF("chromosome") === rhsDF("chromosome")) && (lhsDF("start") <= rhsDF("end")) && (rhsDF("start") <= lhsDF("end"))


  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setLogLevel(Level.WARN.toString)
  }
}
