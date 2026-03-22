package me.kosik.interwalled.ailist.utils

import me.kosik.interwalled.ailist.model.Intervals

import scala.annotation.tailrec

object SearchUtils {

  def findRightmost(intervals: Intervals, leftBound: Int, rightBound: Int, queryEnd: Long): Int = {
    // EDGE CASE:
    // All elements are less than the {queryEnd}:
    if (intervals.get(rightBound).to < queryEnd)
      return rightBound

    // EDGE CASE:
    // All elements are greater than the {queryEnd}:
    if (intervals.get(leftBound).from > queryEnd)
      return -1

    findRightmostBinary(intervals, leftBound, rightBound, queryEnd)
  }

  @tailrec
  private def findRightmostBinary(intervals: Intervals, leftBound: Int, rightBound: Int, queryEnd: Long): Int = {
    if(rightBound - leftBound > 15) {
      val middleIndex = (leftBound + rightBound) / 2

      if(intervals.get(middleIndex).from >= queryEnd) {
        // Intervals' left edge is further right than the query's right edge.
        //  The middleIndex is too far right (will not find anything there) and the right side of the array
        //  does not contain any valid queries, skip.
        findRightmostBinary(intervals, leftBound, middleIndex, queryEnd)
      } else {
        findRightmostBinary(intervals, middleIndex, rightBound, queryEnd)
      }

    } else {
      findRightmostLinear(intervals, leftBound, rightBound, queryEnd)
    }
  }

  private def findRightmostLinear(intervals: Intervals, leftBound: Int, rightBound: Int, queryEnd: Long): Int = {
    for (index <- rightBound to leftBound by -1) {
      if (intervals.get(index).from <= queryEnd)
        return index
    }

    -1
  }
}
