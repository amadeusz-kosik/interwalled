package me.kosik.interwalled.spark.join

import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.reflect.runtime.universe._

trait IntervalJoin {
  def join[T : TypeTag](lhsInput: Dataset[Interval[T]], rhsInput: Dataset[Interval[T]]): Dataset[IntervalsPair[T]]
}
