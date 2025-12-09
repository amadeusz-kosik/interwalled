package me.kosik.interwalled.utility

import org.apache.spark.sql.Row
import scala.annotation.tailrec


object BinarySearch {
  private val MAX_E = "_ailist_max_end"

  // FIXME: operate on indexes
  private val LINEAR_TH = 4

  def findFirstGreaterEqual(needle: Long, haystack: Array[Row]): Option[Int] =
    findFirstGreaterEqual(needle: Long, haystack: Array[Row], 0, haystack.length - 1)

  @tailrec
  private def findFirstGreaterEqual(needle: Long, haystack: Array[Row], leftBound: Int, rightBound: Int): Option[Int] = {
    if(haystack(leftBound).getAs[Long](MAX_E) >= needle)
      Some(leftBound)
    else if(haystack(rightBound).getAs[Long](MAX_E) < needle)
      None
    else if(rightBound - leftBound <= LINEAR_TH)
      findFirstGreaterEqualLinear(needle, haystack, leftBound, rightBound)
    else {
      val middle = (leftBound + rightBound) / 2
      val middleElement = haystack(middle).getAs[Long](MAX_E)

      if(middleElement < needle)
        findFirstGreaterEqual(needle, haystack, middle, rightBound)
      else
        findFirstGreaterEqual(needle, haystack, leftBound, middle)
    }
  }

  private def findFirstGreaterEqualLinear(needle: Long, haystack: Array[Row], leftBound: Int, rightBound: Int): Option[Int] =
    (leftBound to rightBound).find(index => haystack(index).getAs[Long](MAX_E) >= needle)
}
