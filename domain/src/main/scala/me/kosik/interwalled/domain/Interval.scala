package me.kosik.interwalled.domain

case class Interval[T](
  key: String,
  from: Long,
  to: Long,
  value: T
)

object IntervalColumns {
  val KEY   : String = "key"
  val FROM  : String = "from"
  val TO    : String = "to"
  val VALUE : String = "value"
}