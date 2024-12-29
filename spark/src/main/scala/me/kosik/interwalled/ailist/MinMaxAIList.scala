package me.kosik.interwalled.ailist


case class MinMaxAIList[T](
  aiList: AIList[T],
  leftBound: Option[Long],
  rightBound: Option[Long]
)
