package me.kosik.interwalled.spark.join

import me.kosik.interwalled.spark.join.api.IntervalJoin
import me.kosik.interwalled.spark.join.implementation.driver.DriverAIListIntervalJoin


class DriverAIListIntervalJoinTestSuite extends AbstractIntervalJoinTestSuite {

  override def intervalJoin: IntervalJoin = DriverAIListIntervalJoin
}
