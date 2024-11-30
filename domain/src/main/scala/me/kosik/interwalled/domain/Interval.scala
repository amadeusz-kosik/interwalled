package me.kosik.interwalled.domain

case class Interval[T](
  key: String,
  from: Long,
  to: Long,
  value: T
)
