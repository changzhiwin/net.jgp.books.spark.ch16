package net.jgp.books.spark.ch10.stream_seed

import com.typesafe.scalalogging.Logger
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File

object RecordWriterUtils {

  val log = Logger(getClass.getName)

  def write(fileName: String, records: String): Unit = {
    write(fileName, records, StreamingUtils.inputDirectory)
  }

  def write(fileName: String, records: String, directory: String): Unit = {

    val fullFileName = s"${directory}${fileName}" 
    log.info(s"Writing in: ${fullFileName}")

    val bw = new BufferedWriter(new FileWriter(new File(fullFileName)))
    bw.write(records)
    bw.close()
  }

}