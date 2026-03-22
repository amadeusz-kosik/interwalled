package me.kosik.interwalled.ailist.model

case class Interval(
  key:    String,
  from:   Long,
  to:     Long,
  value:  String
)

object Interval {
  def overlaps(lhs: Interval, rhs: Interval): Boolean =
    lhs.key ==  rhs.key && lhs.to >= rhs.from && rhs.to >= lhs.from
}
