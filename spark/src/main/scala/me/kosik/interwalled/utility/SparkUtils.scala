package me.kosik.interwalled.utility

import org.apache.spark.sql.SparkSession


object SparkUtils {

  def withJobName[T, U](jobName: String)(f: => U)(implicit sparkSession: SparkSession): U = {
    sparkSession.sparkContext.setJobDescription(jobName)
    val result = f
    sparkSession.sparkContext.setJobDescription("")
    result
  }
}
