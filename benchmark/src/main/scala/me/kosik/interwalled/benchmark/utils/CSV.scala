package me.kosik.interwalled.benchmark.utils

import scala.reflect.runtime.universe._


object CSV {

  private val DELIMITER = ","

  def toCSV[T <: Product: TypeTag](rows: Seq[T]): Seq[String] = {
    val caseClassFields = typeOf[T].members
      .collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }
      .toSeq
      .reverse

    val header = caseClassFields.mkString(DELIMITER)
    val body   = rows.map(_.productIterator.mkString(DELIMITER))

    Seq(header) ++ body
  }
}
