package me.kosik.interwalled

import me.kosik.interwalled.domain.Interval
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._


package object ailist {

  object AIListTestHelper extends Matchers {

    def buildList(intervals: Seq[Interval], aiListConfig: AIListConfig = AIListConfig.DEFAULT): AIList = {
      val listBuilder = new AIListBuilder(aiListConfig)

      intervals.foreach(interval => listBuilder.put(interval))
      listBuilder.build()
    }

    def buildResult(aiList: AIList, otherList: Seq[Interval]): JoinResult = {
      val intervalsPairs = otherList.flatMap { rightInterval =>
        aiList.overlapping(rightInterval).asScala.map(leftInterval => IntervalsPair(leftInterval, rightInterval))
      }
      JoinResult(intervalsPairs)
    }

    def buildExpected(leftIntervals: Seq[Interval], rightIntervals: Seq[Interval]): JoinResult = {
      def isOverlapping(lhs: Interval, rhs: Interval): Boolean = lhs.to >= rhs.from && rhs.to >= lhs.from

      JoinResult(leftIntervals.flatMap { leftInterval =>
        rightIntervals
          .filter { rightInterval => isOverlapping(leftInterval, rightInterval)}
          .map(rightInterval => IntervalsPair(leftInterval, rightInterval))
      })
    }

    def assertEqual(expected: JoinResult, actual: JoinResult): Assertion = {
      actual.intervalPairs.length shouldEqual expected.intervalPairs.length
      actual.intervalPairs.toSet should contain theSameElementsAs expected.intervalPairs.toSet
    }
  }

  case class IntervalsPair(leftInterval: Interval, rightInterval: Interval)

  case class JoinResult(intervalPairs: Seq[IntervalsPair])
}
