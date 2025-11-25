package me.kosik.interwalled.spark.join.preprocessor.repartitioner

case class RepartitionerConfig(doRepartition: Boolean) {
  override def toString: String = {
    if(doRepartition)
      "repartition"
    else
      "skip-repartition"
  }
}


