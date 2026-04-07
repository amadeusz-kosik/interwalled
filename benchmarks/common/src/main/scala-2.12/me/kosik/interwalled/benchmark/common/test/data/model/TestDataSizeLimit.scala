package me.kosik.interwalled.benchmark.common.test.data.model

case class TestDataSizeLimit(limit: Option[Long])

object TestDataSizeLimit {
  def apply(): TestDataSizeLimit =
    TestDataSizeLimit(None)

  def apply(limit: Long): TestDataSizeLimit =
    TestDataSizeLimit(Some(limit))
}
