package me.kosik.interwalled.ailist

import me.kosik.interwalled.domain.Interval

class MinMaxAIListBuilder[T] {

  private val builder = new AIListBuilder[T]()
  private var minValue: Option[Long] = None
  private var maxValue: Option[Long] = None

  def build(): MinMaxAIList[T] = {
    val aiList = builder.build()
    MinMaxAIList(aiList, minValue, maxValue)
  }

  def put(interval: Interval[T]): Unit = {
    builder.put(interval)

    minValue = handleNone[Long](Math.min)(minValue, interval.from)
    maxValue = handleNone[Long](Math.max)(maxValue, interval.to)
  }

  private def handleNone[T1](f: (T1, T1) => T1)(lhs: Option[T1], rhs: T1): Option[T1] =
    Some(f(lhs.getOrElse(rhs), rhs))
}
