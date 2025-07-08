package me.kosik.interwalled.spark.join.config

case class AIListConfig(
  maximumComponentsCount:             Int  = 10,
  intervalsCountToCheckLookahead:     Int  = 20,
  intervalsCountToTriggerExtraction:  Int  = 10
)
