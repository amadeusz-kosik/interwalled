package me.kosik.interwalled.algorithm

trait IntervalHolder[T] extends Serializable {
  def overlapping(interval: Interval[T]): OverlapIterator[T]
}

trait IntervalHolderBuilder[T, A >: IntervalHolder[T]] extends Serializable {
  def build(): A
  def put(interval: Interval[T]): Unit
}