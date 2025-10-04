package me.kosik.interwalled.benchmark.utils.csv

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

  def forPath[T](formatter: CSVFormatter[T])(csvFilePath: String): CSVWriter[T] = forWritter[T](formatter) {
    val absolutePath = Path.of(csvFilePath).toAbsolutePath

    if(! Files.exists(absolutePath)) {
      val writer = new PrintWriter(absolutePath.toString)
      writer.write(formatter.header)
      writer.flush()

      writer
    } else {
      val stream = new FileOutputStream(new File(absolutePath.toString), true)
      val writer = new PrintWriter(stream)

      writer
    }
  }

  def forWritter[T](formatter: CSVFormatter[T])(writer: Writer): CSVWriter[T] = new CSVWriter(formatter, writer)
}
