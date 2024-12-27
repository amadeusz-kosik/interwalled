package me.kosik.interwalled.utility

object FPUtils {

  def flipOption[T1, T2](lhs: Option[T1], rhs: Option[T2]): Option[(T1, T2)] = (lhs, rhs) match {
    case (Some(l), Some(r)) => Some(l -> r)
    case _ => None
  }
}
