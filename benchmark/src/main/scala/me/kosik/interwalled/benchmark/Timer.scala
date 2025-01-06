package me.kosik.interwalled.benchmark

import java.util.concurrent.TimeUnit.NANOSECONDS

object Timer {
  def start(): Timer =
    new Timer(System.nanoTime())
}

class Timer(startTime: Long) {
  def millisElapsed(): Long =
    NANOSECONDS.toMillis(System.nanoTime() - startTime)
}