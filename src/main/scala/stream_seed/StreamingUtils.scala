package net.jgp.books.spark.ch10.stream_seed

import java.io.File

object StreamingUtils {

  lazy val inputDirectory = getInputDirectory()

  lazy val inputSubDirectory1 = getInputSubDirectory(1)

  lazy val inputSubDirectory2 = getInputSubDirectory(2)


  private def getInputSubDirectory(order: Int): String = {
    val path = inputDirectory + s"s${order}" + File.separator
    createInputDirectory(path)
    path
  }

  private def getInputDirectory(): String = {
    val path = s"${getSystemTempDirectory()}streaming${File.separator}in${File.separator}"
    createInputDirectory(path)
    path
  }

  private def getSystemTempDirectory(): String = {
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
      "C:\\TEMP\\"
    } else {
      System.getProperty("java.io.tmpdir")
    }
  }

  private def createInputDirectory(directory: String): Boolean = {
    val d = new File(directory)
    d.mkdirs()
  }
}