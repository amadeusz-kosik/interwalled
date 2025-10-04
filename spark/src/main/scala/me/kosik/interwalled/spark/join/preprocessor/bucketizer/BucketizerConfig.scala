package me.kosik.interwalled.spark.join.preprocessor.bucketizer


case class BucketizerConfig(perRows: Long) {
  override def toString: String = s"bucket-per-$perRows"

  def calculateScale(datasetCount: => Long): Long = math.max(1L, datasetCount / perRows)
}
