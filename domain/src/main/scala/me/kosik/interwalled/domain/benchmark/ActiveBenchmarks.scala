package me.kosik.interwalled.domain.benchmark

object ActiveBenchmarks {

  object TestDataSizes {
    val baseline: Array[(Int, Long)] = Array(
      16 ->        100L,
      16 ->        250L,
      16 ->        500L,
      16 ->       1000L,
      16 ->       2500L,
      16 ->       5000L,
      16 ->  10 * 1000L,
      16 ->  25 * 1000L,
      16 ->  50 * 1000L,
      16 -> 100 * 1000L,
      16 -> 250 * 1000L,
      16 -> 500 * 1000L
    )

    val extended: Array[(Int, Long)] = baseline ++ Array(
      16 ->  1000 * 1000L,
      16 ->  2500 * 1000L,
      16 ->  5000 * 1000L,
//      16 -> 10000 * 1000L,
//      16 -> 25000 * 1000L,
//      16 -> 50000 * 1000L
    )
  }
}
