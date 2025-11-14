package me.kosik.interwalled.spark.join.preprocessor.deoutlier

case class DeoutlierConfig(percentile: Int) {
  override def toString: String = s"deoutlier-$percentile"
}
