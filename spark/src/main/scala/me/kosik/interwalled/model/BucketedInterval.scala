package me.kosik.interwalled.model

import me.kosik.interwalled.ailist.model.Interval

case class BucketedInterval(
  bucket:   String,
  key:      String,
  from:     Long,
  to:       Long,
  value:    String
) {

  def withoutBucketing: Interval =
    new Interval(key, from, to, value)
}