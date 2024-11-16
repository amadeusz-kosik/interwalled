package me.kosik.interwalled.ailist

import org.scalatest.funsuite.AnyFunSuite

class ListSplittingTest extends AnyFunSuite {

//  test("Splitting list into components test - splitting intervals") {
//    val intervals = Array(
//      Interval(  1,  11, "NIL"),
//      Interval(  1,  12, "NIL"),
//      Interval(  2,  13, "NIL"),
//      Interval(  2,  14, "NIL"),
//      Interval(  3,  15, "NIL"),
//
//      Interval(  4,  64, "NIL"),
//
//      Interval(  4,  12, "NIL"),
//      Interval(  5,  10, "NIL"),
//      Interval(  6,  12, "NIL"),
//      Interval(  6,  14, "NIL"),
//      Interval(  7,  18, "NIL"),
//
//      Interval(  8,  64, "NIL"),
//      Interval(  8,  62, "NIL"),
//      Interval(  9,  63, "NIL"),
//      Interval(  9,  61, "NIL"),
//      Interval( 10,  60, "NIL"),
//
//      Interval( 10,  33, "NIL"),
//      Interval( 11,  32, "NIL"),
//      Interval( 12,  33, "NIL"),
//      Interval( 12,  32, "NIL"),
//      Interval( 15,  33, "NIL"),
//    )
//
//    val aiList = {
//      val aiListBuilder = new AIListBuilder[String](10, 5, 5, 3)
//      intervals.foreach(aiListBuilder.put)
//      aiListBuilder.build()
//    }
//
//    assert(21 == aiList.size,               "All 21 Intervals should be persisted.")
//    assert( 2 == aiList.getComponentsCount, "Intervals ending with > 60 should be placed in a separate component.")
//  }
//
//  test("Splitting list into components test - no splitting intervals due to minimum component length") {
//    val intervals = Array(
//      Interval(  1,  11, "NIL"),
//      Interval(  1,  12, "NIL"),
//      Interval(  2,  13, "NIL"),
//      Interval(  2,  14, "NIL"),
//      Interval(  3,  15, "NIL"),
//
//      Interval(  4,  64, "NIL"),
//
//      Interval(  4,  12, "NIL"),
//      Interval(  5,  10, "NIL"),
//      Interval(  6,  12, "NIL"),
//      Interval(  6,  14, "NIL"),
//      Interval(  7,  18, "NIL"),
//
//      Interval(  8,  64, "NIL"),
//      Interval(  8,  62, "NIL"),
//      Interval(  9,  63, "NIL"),
//      Interval(  9,  61, "NIL"),
//      Interval( 10,  60, "NIL"),
//
//      Interval( 10,  33, "NIL"),
//      Interval( 11,  32, "NIL"),
//      Interval( 12,  33, "NIL"),
//      Interval( 12,  32, "NIL"),
//      Interval( 15,  33, "NIL"),
//    )
//
//    val aiList = {
//      val aiListBuilder = new AIListBuilder[String](10, 5, 5, 25)
//      intervals.foreach(aiListBuilder.put)
//      aiListBuilder.build()
//    }
//
//    assert(21 == aiList.size,               "All 21 Intervals should be persisted.")
//    assert( 1 == aiList.getComponentsCount, "All intervals should be placed in a separate component.")
//  }
//
//  test("Splitting list into components test - no splitting intervals due to maximum component count") {
//    val intervals = Array(
//      Interval(  1,  11, "NIL"),
//      Interval(  1,  12, "NIL"),
//      Interval(  2,  13, "NIL"),
//      Interval(  2,  14, "NIL"),
//      Interval(  3,  15, "NIL"),
//
//      Interval(  4,  64, "NIL"),
//
//      Interval(  4,  12, "NIL"),
//      Interval(  5,  10, "NIL"),
//      Interval(  6,  12, "NIL"),
//      Interval(  6,  14, "NIL"),
//      Interval(  7,  18, "NIL"),
//
//      Interval(  8,  64, "NIL"),
//      Interval(  8,  62, "NIL"),
//      Interval(  9,  63, "NIL"),
//      Interval(  9,  61, "NIL"),
//      Interval( 10,  60, "NIL"),
//
//      Interval( 10,  33, "NIL"),
//      Interval( 11,  32, "NIL"),
//      Interval( 12,  33, "NIL"),
//      Interval( 12,  32, "NIL"),
//      Interval( 15,  33, "NIL"),
//    )
//
//    val aiList = {
//      val aiListBuilder = new AIListBuilder[String](1, 5, 5, 3)
//      intervals.foreach(aiListBuilder.put)
//      aiListBuilder.build()
//    }
//
//    assert(21 == aiList.size,               "All 21 Intervals should be persisted.")
//    assert( 1 == aiList.getComponentsCount, "All intervals should be placed in a single component.")
//  }
}
