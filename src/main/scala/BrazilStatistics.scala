package net.jgp.books.spark.ch16.lab200_brazil_stats

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions.{ col, when, regexp_replace, first, sum, expr }

import net.jgp.books.spark.basic.Basic
import net.jgp.books.spark.ch16.lab100_cache_checkpoint.CacheMode._

object BrazilStatistics extends Basic {

  def run(): Unit = {
    val spark = getSession("Brazil economy")
    spark.sparkContext.setCheckpointDir("/tmp")

    val df = spark.read.format("csv").
      option("header", true).
      option("sep", ";").
      option("enforceSchema", true).
      option("nullValue", "null").
      option("inferSchema", true).
      load("data/brazil/BRAZIL_CITIES.csv")
    df.printSchema
    df.show(20, true)
    
    val t0 = process(df, NoCacheNoCheckPoint)
    val t1 = process(df, CacheOnly)
    val t2 = process(df, CheckPointEger)
    val t3 = process(df, CheckPointNonEger)

    println("\n***** Processing times (excluding purification)")
    println("Without cache ............... " + t0 + " ms")
    println("With cache .................. " + t1 + " ms")
    println("With checkpoint ............. " + t2 + " ms")
    println("With non-eager checkpoint ... " + t3 + " ms")
  }

  private def process(df: DataFrame, mode: CacheMode): Long = {

    val t0 = System.currentTimeMillis()

    val filterDF = df.orderBy(col("CAPITAL").desc).
      withColumn("WAL-MART",
        when(col("WAL-MART").isNull, 0).
        otherwise(col("WAL-MART"))
      ).
      withColumn("MAC",
        when(col("MAC").isNull, 0).
        otherwise(col("MAC"))
      ).
      withColumn("GDP", regexp_replace(col("GDP"), ",", ".").cast("float")).
      withColumn("area", regexp_replace(col("area"), ",", "").cast("float")).
      groupBy("STATE").
      agg(
        first("CITY").alias("capital"),
        sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
        sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
        sum("POP_GDP").alias("pop_2016"), 
        sum("GDP").alias("gdp_2016"), 
        sum("POST_OFFICES").alias("post_offices_ct"), 
        sum("WAL-MART").alias("wal_mart_ct"), 
        sum("MAC").alias("mc_donalds_ct"), 
        sum("Cars").alias("cars_ct"), 
        sum("Motorcycles").alias("moto_ct"), 
        sum("AREA").alias("area"), 
        sum("IBGE_PLANTED_AREA").alias("agr_area"), 
        sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"), 
        sum("HOTELS").alias("hotels_ct"), 
        sum("BEDS").alias("beds_ct")
      ).
      withColumn("agr_area", expr("agr_area / 100")).
      orderBy("STATE").
      withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"))

    val cachedDF = mode match {
      case CacheOnly          => filterDF.cache()
      case CheckPointEger     => filterDF.checkpoint()
      case CheckPointNonEger  => filterDF.checkpoint(false)
      case _                  => filterDF
    }

    println("**********  Pure data");
    cachedDF.printSchema;
    val t1 = System.currentTimeMillis()
    println("Aggregation (ms) .................. " + (t1 - t0))

    val popDf = cachedDF.
      drop(
        "area", "pop_brazil", "pop_foreign", "post_offices_ct",
        "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
        "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "gdp_2016"
      ).
      orderBy(col("pop_2016").desc)
    val t2 = System.currentTimeMillis()
    popDf.show(10)

    val walmartPopDf = cachedDF.
      withColumn("walmart_1m_inh", expr("int(wal_mart_ct / pop_2016 * 100000000) / 100")).
      drop(
        "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
        "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
        "hotels_ct", "beds_ct", "gdp_capita", "gdp_2016"
      ).
      orderBy(col("walmart_1m_inh").desc)
    val t3 = System.currentTimeMillis()
    walmartPopDf.show(10)

    val postOfficeDf = cachedDF.
      withColumn("post_office_1m_inh", expr("int(post_offices_ct / pop_2016 * 100000000) / 100")).
      withColumn("post_office_100k_km2", expr("int(post_offices_ct / area * 10000000) / 100")).
      drop(
        "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
        "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct",
        "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil"
      ).
      orderBy(col("post_office_1m_inh").desc)
    
    val postOfficeCachedDf = mode match {
      case CacheOnly          => postOfficeDf.cache()
      case CheckPointEger     => postOfficeDf.checkpoint()
      case CheckPointNonEger  => postOfficeDf.checkpoint(false)
      case _                  => postOfficeDf
    }

    val postOfficePopDf = postOfficeCachedDf.
      drop("post_office_100k_km2", "area").
      orderBy(col("post_office_1m_inh").desc)
    println("**** Per 1 million inhabitants")
    postOfficePopDf.show(10)

    val postOfficeAreaDf = postOfficeCachedDf.
      drop("post_office_1m_inh", "pop_2016").
      orderBy(col("post_office_100k_km2").desc)
    println("**** per 100000 km2")
    postOfficeAreaDf.show(10)

    val t4 = System.currentTimeMillis()
    t4 - t0
  }
}