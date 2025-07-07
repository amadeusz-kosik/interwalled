package me.kosik.interwalled.utility.bucketizer

import org.apache.spark.sql.Dataset

sealed trait BucketingConfig {
  def bucketScale: Long
}

case class BucketScale(scale: Long) extends BucketingConfig {
  override def bucketScale: Long = scale
}

case class BucketCount(count: Long, referenceDataset: Dataset[_]) extends BucketingConfig {
  override def bucketScale: Long = referenceDataset.count() / count
}
