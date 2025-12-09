package me.kosik.interwalled.ailist

case class BucketedInterval(
  bucket:   String,
  key:      String,
  from:     Long,
  to:       Long,
  value:    String
) {

  def toInterval: Interval =
    Interval(key, from, to, value)
}

object IntervalColumns {
  val BUCKET : String = "bucket"
  val KEY    : String = "key"
  val FROM   : String = "from"
  val TO     : String = "to"
  val VALUE  : String = "value"

  // Additional, temporary columns
  val _COMPONENT  : String = "_ailist_component"
  val _MAX_E      : String = "_ailist_max_end"
}