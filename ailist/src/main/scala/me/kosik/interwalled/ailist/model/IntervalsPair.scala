package me.kosik.interwalled.ailist.model


case class IntervalsPair(
  key:        String,

  lhsFrom:    Long,
  lhsTo:      Long,
  lhsValue:   String,

  rhsFrom:    Long,
  rhsTo:      Long,
  rhsValue:   String
)

object IntervalsPair {
  def apply(lhs: Interval, rhs: Interval): IntervalsPair = {
    assert(lhs.key == rhs.key, "Both intervals must have the same key.")

    IntervalsPair(
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