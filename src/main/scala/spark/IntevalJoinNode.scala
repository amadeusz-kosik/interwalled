package com.eternalsh.interwalled
package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

import scala.collection.Seq

case class IntevalJoinNode(left: SparkPlan,
                           right: SparkPlan,
                           condition: Seq[Expression],
                           context: SparkSession
                          ) extends BinaryExecNode {

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)

  override protected def doExecute(): RDD[InternalRow] = {

  }

  override def output: scala.Seq[Attribute] = left.output ++ right.output
}
