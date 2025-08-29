package me.kosik.interwalled.benchmark.test.data.datasets.random

import me.kosik.interwalled.benchmark.test.data.model.{IntervalLength, RawTestDataRow}
import me.kosik.interwalled.benchmark.test.data.datasets.TestCase
import org.apache.spark.mllib.random.UniformGenerator
import org.apache.spark.sql.{Dataset, SparkSession}


class TestRandomUniform(
  name: String,
  length: IntervalLength,
  totalRowsCount: Long
) extends TestCase with Serializable {

  private lazy val seed = 25503

  private lazy val generator = {
    val _generator = new UniformGenerator()
    _generator.setSeed(seed)

    _generator
  }

  override def testCaseName: String = name

  override def _generate()(implicit sparkSession: SparkSession): Dataset[RawTestDataRow] = {
    import sparkSession.implicits._

    sparkSession.sparkContext
      .range(0L, totalRowsCount)
      .map(_ => (generator.nextValue() * totalRowsCount).toLong)
      .map(from => RawTestDataRow(from, from + length.value))
      .toDS()
      .as[RawTestDataRow]
  }
}
