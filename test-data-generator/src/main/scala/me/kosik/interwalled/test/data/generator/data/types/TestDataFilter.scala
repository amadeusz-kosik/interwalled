package me.kosik.interwalled.test.data.generator.data.types

case class TestDataFilter(fn: RawTestDataRow => Boolean)

object TestDataFilter {
  def default: TestDataFilter = TestDataFilter(_ => true)
}