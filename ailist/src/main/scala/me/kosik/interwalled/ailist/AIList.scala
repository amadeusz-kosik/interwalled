package me.kosik.interwalled.ailist

import me.kosik.interwalled.ailist.model.{Interval, Intervals}


case class AIList(
  intervals: Intervals,
  componentsCount: Int,
  componentsLengths: Array[Int],
  componentsStartIndexes: Array[Int],
  componentsMaxEnds: Array[Long]
) {
  def overlapping(interval: Interval) =
    new AIListIterator(interval.from, interval.to, this)

  def length: Int =
    intervals.length

  override def toString: String = {
    val componentsStr = componentsLengths.mkString(", ")
    f"AIList { Components: $componentsCount { $componentsStr }}"
  }

  /* OverlapIterator interface. */
  // FIXME: Remove those methods, use immutable fields instead.

  private[ailist] def getComponentStartIndex(componentIndex: Int) =
    componentsStartIndexes(componentIndex)

  private[ailist] def getComponentLength(componentIndex: Int) =
    componentsLengths(componentIndex)

  private[ailist] def getComponentMaxEnd(componentIndex: Int) = {
    val componentStartIndex = getComponentStartIndex(componentIndex)
    val componentLength = getComponentLength(componentIndex)
    val componentEndIndex = componentStartIndex + componentLength - 1
    componentsMaxEnds(componentEndIndex)
  }

  private[ailist] def getIntervalMaxEnd(intervalIndex: Int) =
    componentsMaxEnds(intervalIndex)

  private[ailist] def getComponentsCount =
    componentsCount

  private[ailist] def getInterval(index: Int) =
    intervals.get(index)

  private[ailist] def getIntervals =
    intervals
}


