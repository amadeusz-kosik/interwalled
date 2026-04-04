package me.kosik.interwalled.spark.join.implementation.model

import me.kosik.interwalled.ailist.model.Interval

case class RDDAIListIntervalRow(
  bucket:   String,
  salt:     Int,
  key:      String,
  from:     Long,
  to:       Long,
  value:    String
) {

  def withoutBucketing: Interval =
    new Interval(key, from, to, value)
}