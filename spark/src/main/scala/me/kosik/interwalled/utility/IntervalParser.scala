package me.kosik.interwalled.utility

import me.kosik.interwalled.ailist.{Interval, IntervalColumns, IntervalsPair}
import org.apache.spark.sql.Row

object IntervalParser {
  import IntervalColumns._

  def rowToInterval(key: String, row: Row): Interval =
    Interval(key, row.getAs[Long](FROM), row.getAs[Long](TO), row.getAs(VALUE))

  def rowToIntervalsPair(key: String, lhs: Row, rhs: Row): IntervalsPair =
    IntervalsPair(key, rowToInterval(key, lhs), rowToInterval(key, rhs))
}
