package me.kosik.interwalled.domain.benchmark

object ActiveBenchmarks {

  object TestDataSizes {
    val baseline: Array[(Int, Long)] = Array(
      1 ->  10 * 1000L,
      1 ->  25 * 1000L,
      1 ->  50 * 1000L,
      1 ->  75 * 1000L,
      1 -> 100 * 1000L,
      1 -> 250 * 1000L,
      1 -> 500 * 1000L,
      1 -> 750 * 1000L
    )

    val extended: Array[(Int, Long)] = baseline ++ Array(
      1 ->  1000 * 1000L,
      1 ->  2500 * 1000L,
      1 ->  5000 * 1000L,
      1 ->  7500 * 1000L,
      1 -> 10000 * 1000L,
      1 -> 25000 * 1000L,
      1 -> 50000 * 1000L,
      1 -> 75000 * 1000L
    )
  }
}
