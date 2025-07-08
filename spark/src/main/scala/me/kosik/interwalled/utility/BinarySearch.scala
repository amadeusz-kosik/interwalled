package me.kosik.interwalled.utility

import org.apache.spark.sql.Row

import scala.annotation.tailrec

object BinarySearch {
  import me.kosik.interwalled.domain.IntervalColumns.{FROM, TO}
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

//  def findRightmost(needle: Long, haystack: Array[Row]): Option[Int] = {
//    binary(needle, haystack, 0, haystack.length - 1)
//  }
//
//  private def binary(needle: Long, haystack: Array[Row], from: Int, to: Int): Option[Long] = {
//    // EDGE CASE:
//    // All elements are less than the {queryEnd}:
//    if (haystack(to).getAs[Long](FROM) < needle) {
//      Some(to)
//    }
//
//    // EDGE CASE:
//    // All elements are greater than the {queryEnd}:
//    else if (haystack(from).getAs[Long](FROM) > needle) {
//      None
//    }
//
//    // Binary search:
//    else {
//      val middle = (to - from) / 2
//
//      if (haystack(middle).getAs[Long](FROM) >= haystack)
//        binary(needle, haystack, from, middle)
//      else
//        binary(needle, haystack, middle, to)
//    }
//  }
//
//  private def linear(needle: Long, haystack: Array[Row], from: Int, to: Int): Option[Int] = {
//    haystack.foldRight(Option.empty[Int]){
//      case (row, accumulator) if row.getAs[Long](FROM) > haystack =>
//        Some(accumulator)
//
//      case (_, Some(result)) =>
//        Some(result)
//
//      case (row, None) =>
//        Some(row.getAs[Long])
//
//    }
//
//    for(int index = rightBound; index >= leftBound; -- index) {
//      if(intervals.get(index).from() <= queryEnd)
//        return index;
//    }
//
//    return -1;
//  }
}
