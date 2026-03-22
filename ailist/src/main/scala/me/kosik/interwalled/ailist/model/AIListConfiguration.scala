package me.kosik.interwalled.ailist.model

case class AIListConfiguration(
  maximumComponentsCount: Int,
  intervalsCountToCheckLookahead: Int,
  intervalsCountToTriggerExtraction: Int,
  maximumComponentSize: Int,
  checkLookbehindCoverage: Boolean,
  isInputDataSorted: Boolean
)

object AIListConfiguration {
  def apply: AIListConfiguration = AIListConfiguration(
    maximumComponentsCount = 32,
    intervalsCountToCheckLookahead = 24,
    intervalsCountToTriggerExtraction = 16,
    maximumComponentSize = 64,
    checkLookbehindCoverage = false,
    isInputDataSorted = false
  )
}