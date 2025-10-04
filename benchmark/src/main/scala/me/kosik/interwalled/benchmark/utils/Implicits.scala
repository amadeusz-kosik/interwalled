package me.kosik.interwalled.benchmark.utils


object Implicits {

  implicit class LongExtensions(longValue: Long) {
    implicit def K: Long =
      longValue * 1000L

    implicit def M: Long =
      longValue * 1000L * 1000L

    implicit def B: Long =
      longValue * 1000L * 1000L * 1000L
  }
}
