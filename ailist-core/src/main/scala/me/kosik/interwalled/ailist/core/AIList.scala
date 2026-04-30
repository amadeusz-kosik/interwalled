package me.kosik.interwalled.ailist.core

import me.kosik.interwalled.ailist.core.model.Interval
import java.util.ArrayList
import java.util.List


case class AIList(intervals: Array[Interval], maxE: Array[Long]) {
  
    def size(): Integer = {
        intervals.length
    }

    def overlapping(query: Interval): Array[Interval] = {
        intervals.iterator
            .zip(maxE.iterator)
            .dropWhile { case (interval, maxE) => maxE < query.from }
            .takeWhile { case (interval, maxE) => interval.from <= query.to }
            .map { case (interval, maxE) => interval }
            .filter(interval => Interval.overlaps(interval, query))
            .toArray
    }

    def overlappingJ(query: Interval): List[Interval] = {
        import scala.collection.JavaConverters._
        overlapping(query).toBuffer.asJava
    }
}

object AIList {
    // Java adapter
    def apply(intervals: ArrayList[Interval]): AIList = {
        import scala.collection.JavaConverters._
        AIList.apply(intervals.asScala.toArray)
    }

    def apply(intervals: Array[Interval]): AIList = {
        val maxE = {
            if(! intervals.isEmpty)
                intervals.scanLeft(Long.MinValue)(_ max _.to).drop(1)
            else
                Array.empty[Long]
        }

        AIList(intervals, maxE)
    }
}