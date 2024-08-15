package me.kosik.interwalled.algorithm.ailist

import me.kosik.interwalled.algorithm.Interval
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class BasicCorrectnessTest extends AnyFunSuite {

  test("Correctness test - no overlapping intervals") {
    val lhs = (1 to 1000) map { i => Interval(i + 0, i + 2, i)}
    val rhs = (1 to 1000) map { i => Interval(i + 3, i + 6, i)}

    val aiList = {
      val aiListBuilder = new AIListBuilder[Int](10, 20, 10, 64)
      lhs.foreach(aiListBuilder.put)
      aiListBuilder.build()
    }

    val result = rhs.map(aiList.overlapping).flatMap(_.asScala)
    assert(result.isEmpty, "No overlapping intervals should produce empty results set.")
  }

  test("Correctness test - 1 right interval matching all left intervals.") {
    val lhs = (1 to 10000) map { i => Interval(i, i, "L")}
    val rhs = Interval(0, 10000, "R")

    val aiList = {
      val aiListBuilder = new AIListBuilder[String](10, 20, 10, 64)
      lhs.foreach(aiListBuilder.put)
      aiListBuilder.build()
    }

    val result = aiList.overlapping(rhs).asScala.map(_ -> rhs).toList

    assert(result.map(_._2).toSet == Set(rhs), s"All RHS in the results set should be equal to the $rhs.")
    assert(result.map(_._1).toSet == lhs.toSet, "All LHS should be present in the results set.")
    assert(result.length == lhs.length, "Each LHS should be exactly once in the results set.")
  }

  test("Correctness test - 1 left interval matching all right intervals.") {
    val lhs = Interval(0, 10000, "L")
    val rhs = (1 to 10000) map { i => Interval(i, i, "R")}

    val aiList = {
      val aiListBuilder = new AIListBuilder[String](10, 20, 10, 64)
      aiListBuilder.put(lhs)
      aiListBuilder.build()
    }

    val result = rhs.flatMap(interval => aiList.overlapping(interval).asScala.map(_ -> interval))

    assert(result.map(_._1).toSet == Set(lhs), s"All LHS in the results set should be equal to the $lhs.")
    assert(result.map(_._2).toSet == rhs.toSet, "All RHS should be present in the results set.")
    assert(result.length == rhs.length, "Each RHS should be exactly once in the results set.")
  }
}
