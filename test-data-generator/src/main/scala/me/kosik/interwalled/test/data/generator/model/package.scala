package me.kosik.interwalled.test.data.generator

package object model {
  case class TestDataRow(from: Long, to: Long, chromosome: String)

  case class TestResultRow(fromLHS: Long, toLHS: Long, fromRHS: Long, toRHS: Long, chromosome: String)
}
