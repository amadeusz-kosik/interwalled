package me.kosik.interwalled.spark.join.config
import me.kosik.interwalled.ailist.{AIListConfig => JAIListConfig}

case class AIListConfig(
  maximumComponentsCount:             Int  = 10,
  intervalsCountToCheckLookahead:     Int  = 20,
  intervalsCountToTriggerExtraction:  Int  = 10,
  minimumComponentSize:               Int  = 64
) {
  def toShortString: String =
    f"$maximumComponentsCount-$intervalsCountToCheckLookahead-$intervalsCountToTriggerExtraction-$minimumComponentSize"

  def toJava: JAIListConfig = new JAIListConfig(
    maximumComponentsCount,
    intervalsCountToCheckLookahead,
    intervalsCountToTriggerExtraction,
    minimumComponentSize
  )
}
