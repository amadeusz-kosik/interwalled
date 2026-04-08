package me.kosik.interwalled.benchmark.common.timer

import java.util.concurrent.TimeUnit.NANOSECONDS


object Timer {
  def start(): Timer =
    new Timer(System.nanoTime())

  def timed[T](block: => T): (TimerResult, T) = {
    val timer = start()
    val result = block
    val timerResult = timer.millisElapsed()

    (timerResult, result)
  }
}

class Timer(private val startTime: Long) {
  def millisElapsed(): TimerResult =
    TimerResult(NANOSECONDS.toMillis(System.nanoTime() - startTime))
}
