package me.kosik.interwalled.spark.join.preprocessor.salter


case class SalterConfig(perRows: Long) {
  override def toString: String = s"salt-per-$perRows"

  def calculateScale(datasetCount: => Long): Long = math.max(1L, datasetCount / perRows)
}
