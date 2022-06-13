package net.jgp.books.spark.ch16.lab100_cache_checkpoint

import net.jgp.books.spark.ch10.stream_seed._

object GeneratorBatchData {
  def createBooks(count: Int): String = {
    val rs = new RecordStructure(s"book_${count}_${RecordGeneratorUtils.getLastName}").
      add("name", FieldType.NAME).
      add("title", FieldType.TITLE).
      add("rating", FieldType.RATING).
      add("year", FieldType.CONTEMPORARY_YEAR).
      add("lang", FieldType.LANG)

    val data = rs.getRecords(count, true)

    val fileName = s"${rs.recordName}.csv"

    RecordWriterUtils.write(fileName, data)

    StreamingUtils.inputDirectory + fileName
  }
}