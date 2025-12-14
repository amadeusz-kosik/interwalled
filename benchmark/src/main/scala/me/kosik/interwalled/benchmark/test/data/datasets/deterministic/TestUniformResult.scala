package me.kosik.interwalled.benchmark.test.data.datasets.deterministic

import me.kosik.interwalled.ailist.{Interval, IntervalsPair}
import me.kosik.interwalled.spark.join.api.model.IntervalJoin.Input
import me.kosik.interwalled.spark.join.implementation.SparkNativeIntervalJoin
import me.kosik.interwalled.spark.join.preprocessor.generic.Preprocessor.PreprocessorConfig
import org.apache.spark.sql.{Dataset, SparkSession}


class TestUniformResult(leftSource: TestUniform, rightSource: TestUniform) {

  def testCaseName: String = f"${leftSource.testCaseName}-${rightSource.testCaseName}-result"

  def generate()(implicit sparkSession: SparkSession): Dataset[IntervalsPair] = {
    import SparkNativeIntervalJoin.Config
    import sparkSession.implicits._

    val input = {
      val getSource: TestUniform => Dataset[Interval] = { testCase =>
        testCase.generate().as[Interval]
      }

      Input(getSource(leftSource), getSource(rightSource))
    }

    val data = {
      new SparkNativeIntervalJoin(Config(PreprocessorConfig.empty))
        .join(input, gatherStatistics = false)
        .data
        .coalesce(1)
    }

    data
  }

}

