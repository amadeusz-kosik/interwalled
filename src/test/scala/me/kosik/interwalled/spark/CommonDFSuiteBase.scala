package me.kosik.interwalled.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait CommonDFSuiteBase extends AnyFunSuite with DataFrameSuiteBase with Matchers {

  protected def getJoinPredicate(lhsDF: DataFrame, rhsDF: DataFrame): Column =
    (lhsDF("chromosome") === rhsDF("chromosome")) && (lhsDF("from") <= rhsDF("to")) && (rhsDF("from") <= lhsDF("to"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.setLogLevel(Level.WARN.toString)
  }
}
