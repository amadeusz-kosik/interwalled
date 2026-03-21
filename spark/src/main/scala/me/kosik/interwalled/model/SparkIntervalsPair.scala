package me.kosik.interwalled.model

import me.kosik.interwalled.ailist.model.Interval

case class SparkIntervalsPair(
  key:    String,

  leftFrom:   Long,
  leftTo:     Long,
  leftValue:  String,

  rightFrom:  Long,
  rightTo:    Long,
  rightValue: String
)

object SparkIntervalsPair {

  def apply(lhs: Interval[String], rhs: Interval[String]): SparkIntervalsPair = {
    assert(lhs.key == rhs.key, "Both intervals must have the same key.")

    SparkIntervalsPair(
      lhs.key,

      lhs.from,
      lhs.to,
      lhs.value,

      rhs.from,
      rhs.to,
      rhs.value
    )
  }

  def apply(lhs: SparkInterval, rhs: SparkInterval): SparkIntervalsPair = {
    assert(lhs.key == rhs.key, "Both intervals must have the same key.")

    SparkIntervalsPair(
      lhs.key,

      lhs.from,
      lhs.to,
      lhs.value,

      rhs.from,
      rhs.to,
      rhs.value
    )
  }
}