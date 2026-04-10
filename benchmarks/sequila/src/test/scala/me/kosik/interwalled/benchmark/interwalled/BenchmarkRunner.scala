package me.kosik.interwalled.benchmark.interwalled

import com.holdenkarau.spark.testing.DatasetSuiteBase
import me.kosik.interwalled.benchmark.common.test.data.TestDataSuites
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Path


class BenchmarkRunner extends AnyFunSuite with DatasetSuiteBase {

  private val testDataPath  = Path.of("data/")
  private val testDataSuites = TestDataSuites.databioSuites.filter(_.suite.startsWith("databio-s")).take(1)

  test(s"Testing small databio data sets: ${testDataSuites.mkString("Array(", ", ", ")")}.") {
    Benchmark.runBenchmark(testDataPath, testDataSuites, _ => ())(spark)
  }
}