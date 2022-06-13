package net.jgp.books.spark.ch16.lab100_cache_checkpoint

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions.col

import net.jgp.books.spark.basic.Basic

object CacheMode extends Enumeration {

  type CacheMode = Value

  val NoCacheNoCheckPoint, CacheOnly, CheckPointEger, CheckPointNonEger = Value
}
import CacheMode._

object CacheCheckpoint extends Basic {
  def run(num: String): Unit = {
    val spark = getSession("Lab around cache and checkpoint")
    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp")

    val recordCount = num.toInt
    val t0 = processDataframe(spark, recordCount, NoCacheNoCheckPoint)
    val t1 = processDataframe(spark, recordCount, CacheOnly)
    val t2 = processDataframe(spark, recordCount, CheckPointEger)
    val t3 = processDataframe(spark, recordCount, CheckPointNonEger)

    println("\nProcessing times")
    println("Without cache ............... " + t0 + " ms")
    println("With cache .................. " + t1 + " ms")
    println("With checkpoint ............. " + t2 + " ms")
    println("With non-eager checkpoint ... " + t3 + " ms")
  }

  private def processDataframe(spark: SparkSession, cnt: Int, mode: CacheMode): Long = {
    val df = createDataframe(spark, cnt)

    val t0 = System.currentTimeMillis
    val filterDF = df.filter("rating = 5")

    val cachedDF = mode match {
      case CacheOnly          => filterDF.cache()
      case CheckPointEger     => filterDF.checkpoint()
      case CheckPointNonEger  => filterDF.checkpoint(false)
      case _                  => filterDF
    }

    val langDF = cachedDF.groupBy("lang").
      count.
      orderBy("lang").
      collect

    println("--------------groupBy('lang')----------------------")
    langDF.foreach(r => println(r))

    val yearDF = cachedDF.groupBy("year").
      count.
      orderBy(col("year").desc).
      collect
    
    println("--------------groupBy('year')----------------------")
    yearDF.foreach(r => println(r))

    val t1 = System.currentTimeMillis
    t1 - t0
  }

  private def createDataframe(spark: SparkSession, cnt: Int): DataFrame = {
    val path = GeneratorBatchData.createBooks(cnt)

    spark.read.format("csv").
      option("header", true).
      option("inferSchema", true).
      load(path)
  }
}