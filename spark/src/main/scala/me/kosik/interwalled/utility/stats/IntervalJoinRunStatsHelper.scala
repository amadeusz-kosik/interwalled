package me.kosik.interwalled.utility.stats

import me.kosik.interwalled.utility.stats.model.IntervalJoinRunStats
import org.apache.spark.sql.Dataset

object IntervalJoinRunStatsHelper {

  def gatherStats(result: Dataset[_]): IntervalJoinRunStats =
    IntervalJoinRunStats(resultRowsCount = result.count())
}
