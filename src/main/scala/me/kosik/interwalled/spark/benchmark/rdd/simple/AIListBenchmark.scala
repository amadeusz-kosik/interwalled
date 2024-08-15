
package me.kosik.interwalled.spark.benchmark.rdd.simple

import me.kosik.interwalled.algorithm.Interval
import me.kosik.interwalled.algorithm.ailist.AIListBuilder
import org.apache.spark.rdd.RDD



class AIListBenchmark {

  def performJoin(left: RDD[SimpleInterval], right: RDD[SimpleInterval]): RDD[(SimpleInterval, SimpleInterval)] = {
    val database = left.mapPartitions { dataIterator =>
      val aiListBuilder = new AIListBuilder[String](10, 20, 10, 64)
      dataIterator.foreach(i => aiListBuilder.put(Interval(i.from, i.to, i.value)))
      List(aiListBuilder.build()).iterator
    }

    val query = database.cartesian(right)
    ???
  }
}
