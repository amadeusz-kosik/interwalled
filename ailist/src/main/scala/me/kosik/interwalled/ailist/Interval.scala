package me.kosik.interwalled.ailist

case class Interval[T](
  start: Long,
  end: Long,
  value: T
)
