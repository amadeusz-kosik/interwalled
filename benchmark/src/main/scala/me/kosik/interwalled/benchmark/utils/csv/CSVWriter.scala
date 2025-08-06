package me.kosik.interwalled.benchmark.utils.csv

import me.kosik.interwalled.benchmark.utils.BenchmarkResult

import java.io.{File, FileOutputStream, PrintWriter, Writer}
import java.nio.file.{Files, Path}


class CSVWriter(fileWriter: Writer) {

  def write(row: BenchmarkResult): Unit = {
    fileWriter.write(CSVFormatter.row(row))
    fileWriter.flush()
  }

  def close(): Unit = {
    fileWriter.close()
  }
}

object CSVWriter {

  def open(csvFilePath: String): CSVWriter = open(
    if(! Files.exists(Path.of(csvFilePath))) {
      val writer = new PrintWriter(csvFilePath)
      writer.write(CSVFormatter.header)
      writer.flush()

      writer
    } else {
      val stream = new FileOutputStream(new File(csvFilePath), true)
      val writer = new PrintWriter(stream)

      writer
    }
  )

  def open(writer: Writer): CSVWriter = new CSVWriter(writer)
}
