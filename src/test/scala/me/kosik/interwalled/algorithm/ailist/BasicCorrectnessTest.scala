package me.kosik.interwalled.algorithm.ailist

import me.kosik.interwalled.algorithm.Interval
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.jdk.CollectionConverters._

class BasicCorrectnessTest extends AnyFunSuite {

  test("Correctness test - no overlapping intervals") {
    val lhs = (1 to 1000) map { i => Interval(i +    0, i +    0, i)}
    val rhs = (1 to 1000) map { i => Interval(i + 1003, i + 1003, i)}

    val aiList = {
      val aiListBuilder = new AIListBuilder[Int](10, 20, 10, 64)
      lhs.foreach(aiListBuilder.put)
      aiListBuilder.build()
    }

    val result = rhs.map(aiList.overlapping).flatMap(_.asScala)
    assert(result.isEmpty, "No overlapping intervals should produce empty results set.")
  }

  test("Correctness test - 1:1 matching, single list component.") {
    val lhs = (1 to 100) map { i => Interval(i, i, "L")}
    val rhs = (1 to 100) map { i => Interval(i, i, "R")}

    val aiList = {
      val aiListBuilder = new AIListBuilder[String](10, 20, 10, 64)
      lhs.foreach(aiListBuilder.put)
      aiListBuilder.build()
    }

    val expected = (1 to 100) map { i => Interval(i, i, "L") -> Interval(i, i, "R")}
    val actual = rhs.flatMap(interval => aiList.overlapping(interval).asScala.map(_ -> interval))

    assert(actual.toArray sameElements expected, "All LHS should be present in the results set.")
    assert(actual.length == lhs.length, "Each LHS should be exactly once in the results set.")
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

  test("Correctness test - 1 right interval matching all left intervals, multiple list components.") {

    val lhs = ((1 to 100) map { i =>
      Interval(i, i + 1, "L")
    }) ++ ((1 to 100) map { i =>
      Interval(i, i + 50, "L")
    })

    val rhs = List(Interval(0, 10000, "R"))

    val aiList = {
      val aiListBuilder = new AIListBuilder[String](20, 10, 4, 5)
      lhs.foreach(aiListBuilder.put)
      aiListBuilder.build()
    }

    val expected = lhs.map(i => i -> Interval(0, 10000, "R"))
    val actual = rhs.flatMap(interval => aiList.overlapping(interval).asScala.map(_ -> interval))

    assert(aiList.getComponentsCount == 2, "This test requires AIList with two components.")
    actual.toSet should contain theSameElementsAs expected
  }

  test("All to all matching.") {
    val lhs = (1 to 100) map { i => Interval(i, 1000 + i, "L")}
    val rhs = (1 to 100) map { i => Interval(i, 1000 + i, "R")}

    val aiList = {
      val aiListBuilder = new AIListBuilder[String](10, 20, 10, 64)
      lhs.foreach(aiListBuilder.put)
      aiListBuilder.build()
    }

    val expected = (for {
      i <- (1 to 100)
      j <- (1 to 100)
    } yield Interval(i, i + 1000, "L") -> Interval(j, j + 1000, "R")).toSet.toList
    val actual = rhs.flatMap(interval => aiList.overlapping(interval).asScala.map(_ -> interval)).toSet.toList

    assert(actual equals expected, "All expected elements should be present in the results set.")
  }
}
