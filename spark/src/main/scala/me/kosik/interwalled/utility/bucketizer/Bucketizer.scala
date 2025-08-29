package me.kosik.interwalled.utility.bucketizer

import me.kosik.interwalled.domain.{BucketedInterval, Interval, IntervalsPair}
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe._


trait Bucketizer {
  def bucketize[T : TypeTag](lhs: Dataset[Interval[T]], rhs: Dataset[Interval[T]]): (Dataset[BucketedInterval[T]], Dataset[BucketedInterval[T]])
  def deduplicate[T : TypeTag](input: Dataset[IntervalsPair[T]]): Dataset[IntervalsPair[T]]
}

object Bucketizer {
  def apply(config: Option[BucketingConfig]): Bucketizer = config match {
    case Some(c) => new SimpleBucketizer(c)
    case None    =>     DummyBucketizer
  }

  def salting(config: Option[BucketingConfig]): Bucketizer = config match {
    case Some(c) => new SaltingBucketizer(c)
    case None    =>     DummyBucketizer
  }
}