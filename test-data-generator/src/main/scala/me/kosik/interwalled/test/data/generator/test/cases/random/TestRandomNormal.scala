package me.kosik.interwalled.test.data.generator.test.cases.random

import me.kosik.interwalled.test.data.generator.data.types.{IntervalLength, RawTestDataRow}
import me.kosik.interwalled.test.data.generator.test.cases.TestCase
import org.apache.spark.mllib.random.{PoissonGenerator, StandardNormalGenerator}
import org.apache.spark.sql.{Dataset, SparkSession}


class TestRandomNormal(
  length: IntervalLength,
  totalRowsCount: Long
) extends TestCase with Serializable {

  private lazy val seed = 25503

  private lazy val generator = {
    val _generator = new StandardNormalGenerator()
    _generator.setSeed(seed)

    _generator
  }

  override def testCaseName: String = s"TestRandomNormal-$length"

  override def _generate()(implicit sparkSession: SparkSession): Dataset[RawTestDataRow] = {
    import sparkSession.implicits._

    sparkSession.sparkContext
      .range(0L, totalRowsCount)
      .map(_ => ((1.0 + generator.nextValue()) * totalRowsCount / 2).toLong)
      .filter(_ >= 0L)
      .map(from => RawTestDataRow(from, from + length.value))
      .toDS()
      .as[RawTestDataRow]
  }
}
