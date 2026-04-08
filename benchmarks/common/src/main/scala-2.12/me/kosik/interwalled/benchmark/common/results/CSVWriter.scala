package me.kosik.interwalled.benchmark.common.results

import java.io.{File, FileOutputStream, PrintWriter, Writer}
import java.nio.file.{Files, Path}


class CSVWriter[T](formatter: CSVFormatter[T], fileWriter: Writer) {

  def write(row: T): Unit = {
    fileWriter.write(formatter.row(row))
    fileWriter.flush()
  }

  def close(): Unit = {
    fileWriter.close()
  }
}

object CSVWriter {

  def forPath[T](formatter: CSVFormatter[T])(csvFilePath: Path): CSVWriter[T] = forWriter[T](formatter) {
    if(! Files.exists(csvFilePath)) {
      val writer = new PrintWriter(csvFilePath.toString)
      writer.write(formatter.header)
      writer.flush()

      writer
    } else {
      val stream = new FileOutputStream(new File(csvFilePath.toString), true)
      val writer = new PrintWriter(stream)

      writer
    }
  }

  def forWriter[T](formatter: CSVFormatter[T])(writer: Writer): CSVWriter[T] = new CSVWriter(formatter, writer)
}
