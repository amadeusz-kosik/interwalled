package me.kosik.interwalled.domain

case class IntervalsPair[T](
  key: String,
  lhs: Interval[T],
  rhs: Interval[T]
)
