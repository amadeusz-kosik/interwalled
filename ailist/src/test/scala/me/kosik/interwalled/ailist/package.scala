package me.kosik.interwalled

import me.kosik.interwalled.domain.Interval
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers


package object ailist {

  object AIListTestHelper extends Matchers {

    def buildList[T](intervals: Seq[Interval[T]], aiListConfig: AIListConfig = AIListConfig.DEFAULT): AIList[T] = {
      val listBuilder = new AIListBuilder[T](aiListConfig)

      intervals.foreach(interval => listBuilder.put(interval))
      listBuilder.build()
    }

    def buildResult[T](aiList: AIList[T], otherList: Seq[Interval[T]]): JoinResult[T] = {
      import scala.jdk.CollectionConverters._

      val intervalsPairs = otherList.flatMap { rightInterval =>
        aiList.overlapping(rightInterval).asScala.map(leftInterval => IntervalsPair(leftInterval, rightInterval))
      }
      JoinResult(intervalsPairs)
    }

    def buildExpected[T](leftIntervals: Seq[Interval[T]], rightIntervals: Seq[Interval[T]]): JoinResult[T] = {
      def isOverlapping(lhs: Interval[T], rhs: Interval[T]): Boolean = lhs.to >= rhs.from && rhs.to >= lhs.from

      JoinResult(leftIntervals.flatMap { leftInterval =>
        rightIntervals
          .filter { rightInterval => isOverlapping(leftInterval, rightInterval)}
          .map(rightInterval => IntervalsPair(leftInterval, rightInterval))
      })
    }

    def assertEqual[T](expected: JoinResult[T], actual: JoinResult[T]): Assertion = {
      actual.intervalPairs.length shouldEqual expected.intervalPairs.length
      actual.intervalPairs.toSet should contain theSameElementsAs expected.intervalPairs.toSet
    }
  }

  case class IntervalsPair[T](leftInterval: Interval[T], rightInterval: Interval[T])

  case class JoinResult[T](intervalPairs: Seq[IntervalsPair[T]])
}
