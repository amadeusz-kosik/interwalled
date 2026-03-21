package me.kosik.interwalled.model

import me.kosik.interwalled.ailist.model.Interval

case class SparkInterval(
  key:    String,
  from:   Long,
  to:     Long,
  value:  String
) {

  def toAIListInterval: Interval[String] =
    new Interval(key, from, to, value)
}
