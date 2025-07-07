package me.kosik.interwalled.spark.join.config

case class AIListConfig(
  bucketSize:                         Long = 10000L,
  maximumComponentsCount:             Int  = 10,
  intervalsCountToCheckLookahead:     Int  = 20,
  intervalsCountToTriggerExtraction:  Int  = 10
)
