package me.kosik.interwalled.benchmark.utils.timer

import java.util.concurrent.TimeUnit.NANOSECONDS


object Timer {
  def start(): Timer =
    new Timer(System.nanoTime())
}

class Timer(private val startTime: Long) {
  def millisElapsed(): TimerResult =
    TimerResult(NANOSECONDS.toMillis(System.nanoTime() - startTime))
}
