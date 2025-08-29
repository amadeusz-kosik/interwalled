package me.kosik.interwalled.utility.bucketizer


sealed trait BucketingConfig {
  def bucketScale(datasetCount: => Long): Long
}

case class BucketScale(scale: Long) extends BucketingConfig {
  override def toString: String = s"scale-$scale"
  override def bucketScale(datasetCount: => Long): Long = scale
}

case class BucketCount(count: Long) extends BucketingConfig {
  override def toString: String = s"count-$count"
  override def bucketScale(datasetCount: => Long): Long = math.max(1L, datasetCount / count)
}
