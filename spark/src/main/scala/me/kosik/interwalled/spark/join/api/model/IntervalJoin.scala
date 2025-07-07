package me.kosik.interwalled.spark.join.api.model

import me.kosik.interwalled.domain.{Interval, IntervalsPair}
import org.apache.spark.sql.Dataset


object IntervalJoin {
  case class Input[T](
    lhsData: Dataset[Interval[T]],
    rhsData: Dataset[Interval[T]]
  )

  case class Result[T](
    data: Dataset[IntervalsPair[T]],
    statistics: Option[IntervalStatistics]
  )
}
