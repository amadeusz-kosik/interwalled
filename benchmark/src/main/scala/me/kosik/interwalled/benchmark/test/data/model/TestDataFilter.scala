package me.kosik.interwalled.benchmark.test.data.model

case class TestDataFilter(fn: RawTestDataRow => Boolean)

object TestDataFilter {
  def default: TestDataFilter = TestDataFilter(_ => true)
}