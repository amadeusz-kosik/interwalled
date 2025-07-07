package me.kosik.interwalled.spark.join.api

import me.kosik.interwalled.domain.IntervalsPair
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import me.kosik.interwalled.utility.bucketizer.{BucketingConfig, Bucketizer}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe._


trait BucketizedJoin extends IntervalJoin {

  def bucketingConfig: Option[BucketingConfig]

  private val bucketizer = new Bucketizer(bucketingConfig)

  override protected def prepareInput[T : TypeTag](input: Input[T]): PreparedInput[T] = {
    (bucketizer.bucketize(input.lhsData), bucketizer.bucketize(input.rhsData))
  }

}
