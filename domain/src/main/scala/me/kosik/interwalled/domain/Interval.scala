package me.kosik.interwalled.domain

case class Interval[T](
  key: String,
  from: Long,
  to: Long,
  value: T
)


case class BucketedInterval[T](
  _bucket: Long,
  key: String,
  from: Long,
  to: Long,
  value: T
) {

  def toInterval: Interval[T] =
    Interval(key, from, to, value)
}

object IntervalColumns {
  val BUCKET : String = "_bucket"
  val KEY    : String = "key"
  val FROM   : String = "from"
  val TO     : String = "to"
  val VALUE  : String = "value"
}