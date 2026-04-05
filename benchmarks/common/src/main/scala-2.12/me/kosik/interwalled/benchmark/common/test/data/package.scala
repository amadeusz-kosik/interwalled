package me.kosik.interwalled.benchmark.common.test

package object data {

  trait DataPaths {
    def paths: Array[String]
  }

  case class DatabasePaths(override val paths: Array[String]) extends DataPaths {
    override def toString: String = paths.mkString("DatabasePaths(", ", ", ")")
  }

  object DatabasePaths {
    def apply(path: String): DatabasePaths = DatabasePaths(Array(path))
  }

  case class QueryPaths(override val paths: Array[String]) extends DataPaths {
    override def toString: String = paths.mkString("QueryPaths(", ", ", ")")
  }

  object QueryPaths {
    def apply(path: String): QueryPaths = QueryPaths(Array(path))
  }

}
