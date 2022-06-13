package net.jgp.books.spark.basic

import org.apache.spark.sql.SparkSession

trait Basic {
  def getSession(appName: String): SparkSession = {
    SparkSession.builder().
      appName(appName).
      config("spark.executor.memory", "4g").
      config("spark.memory.offHeap.enabled", true).
      config("spark.memory.offHeap.size", "500M").
      getOrCreate()
  }
}