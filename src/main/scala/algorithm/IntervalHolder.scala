package com.eternalsh.interwalled
package algorithm

trait IntervalHolder[T] extends Serializable {
  def overlapping(interval: Interval[T]): java.util.Iterator[Interval[T]]
}

trait IntervalHolderBuilder[T, A >: IntervalHolder[T]] extends Serializable {
  def build(): A
  def put(interval: Interval[T]): Unit
}