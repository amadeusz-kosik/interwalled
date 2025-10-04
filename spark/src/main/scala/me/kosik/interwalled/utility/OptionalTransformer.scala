package me.kosik.interwalled.utility


sealed trait OptionalTransformer[T] {
  def apply[U](input: U)(fn: T => (U => U)): U
}

object OptionalTransformer {
  def apply[T](maybeTransformer: Option[T]): OptionalTransformer[T] = maybeTransformer match {

    case Some(transformer) =>
      SomeTransformer[T](transformer)

    case None =>
      NoneTransformer[T]()
  }
}

case class SomeTransformer[T](transformer: T) extends OptionalTransformer[T] {
  override def apply[U](input: U)(fn: T => (U => U)): U = fn(transformer)(input)
}

case class NoneTransformer[T]() extends OptionalTransformer[T] {
  override def apply[U](input: U)(fn: T => (U => U)): U = input
}