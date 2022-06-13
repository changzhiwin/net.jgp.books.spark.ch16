# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch16

# Environment
- Java 8
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch16_2.13-1.0.jar` (the same as name property in sbt file)


## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit    \
  --class net.jgp.books.spark.MainApp  \
  --master "local[*]"   \
  --packages com.typesafe.scala-logging:scala-logging_2.13:3.9.4 \
  target/scala-2.13/main-scala-ch16_2.13-1.0.jar
```

## 3, print

### Case: CacheCheckpoint
On my laptop(4core, 8g)
```
// record = 1000
--------------groupBy('lang')----------------------
[de,44]
[en,59]
[es,69]
[fr,53]
[it,47]
[pt,72]

--------------groupBy('year')----------------------
[2022,24]
[2021,37]
[2020,37]
[2019,28]
[2018,27]
[2017,38]
[2016,35]
[2015,46]
[2014,43]
[2013,29]

Processing times
Without cache ............... 3632 ms
With cache .................. 1924 ms
With checkpoint ............. 1839 ms
With non-eager checkpoint ... 1418 ms

// record = 10000
--------------groupBy('lang')----------------------
[de,561]
[en,526]
[es,558]
[fr,579]
[it,571]
[pt,587]

--------------groupBy('year')----------------------
[2022,350]
[2021,326]
[2020,329]
[2019,385]
[2018,335]
[2017,336]
[2016,325]
[2015,348]
[2014,331]
[2013,317]

Processing times
Without cache ............... 3313 ms
With cache .................. 2130 ms
With checkpoint ............. 1741 ms
With non-eager checkpoint ... 1264 ms
```

### Case: BrazilStatistics
```
+-----+--------------+--------+
|STATE|       capital|pop_2016|
+-----+--------------+--------+
|   SP|     São Paulo|44749699|
|   MG|Belo Horizonte|20997560|
|   RJ|Rio De Janeiro|16635996|
|   BA|      Salvador|15276566|
|   RS|  Porto Alegre|11286500|
|   PR|      Curitiba|11242720|
|   PE|        Recife| 9410336|
|   CE|     Fortaleza| 8963663|
|   PA|         Belém| 8272724|
|   MA|      São Luís| 6954036|
+-----+--------------+--------+

+-----+-------------+--------+-----------+--------------+
|STATE|      capital|pop_2016|wal_mart_ct|walmart_1m_inh|
+-----+-------------+--------+-----------+--------------+
|   RS| Porto Alegre|11286500|         52|           4.6|
|   PE|       Recife| 9410336|         22|          2.33|
|   SE|      Aracaju| 2265779|          5|           2.2|
|   BA|     Salvador|15276566|         32|          2.09|
|   AL|       Maceió| 3358963|          6|          1.78|
|   PR|     Curitiba|11242720|         20|          1.77|
|   PB|  João Pessoa| 3999415|          6|           1.5|
|   RN|        Natal| 3474998|          4|          1.15|
|   SC|Florianópolis| 6910553|          8|          1.15|
|   PI|     Teresina| 3212180|          3|          0.93|
+-----+-------------+--------+-----------+--------------+

+-----+--------------+--------+---------------+------------------+
|STATE|       capital|pop_2016|post_offices_ct|post_office_1m_inh|
+-----+--------------+--------+---------------+------------------+
|   TO|        Palmas| 1532902|            151|              98.5|
|   MG|Belo Horizonte|20997560|           1925|             91.67|
|   RS|  Porto Alegre|11286500|            972|             86.12|
|   CE|     Fortaleza| 8963663|            745|             83.11|
|   MT|        Cuiabá| 3305531|            274|             82.89|
|   PR|      Curitiba|11242720|            924|             82.18|
|   ES|       Vitória| 3973697|            308|              77.5|
|   RN|         Natal| 3474998|            263|             75.68|
|   PI|      Teresina| 3212180|            243|             75.64|
|   SC| Florianópolis| 6910553|            512|             74.08|
+-----+--------------+--------+---------------+------------------+

+-----+--------------+---------------+------------------+--------------------+
|STATE|       capital|post_offices_ct|              area|post_office_100k_km2|
+-----+--------------+---------------+------------------+--------------------+
|   RJ|Rio De Janeiro|            544| 43750.46017074585|             1243.41|
|   DF|      Brasília|             60|  5760.77978515625|             1041.52|
|   ES|       Vitória|            308| 46074.50023651123|              668.48|
|   SP|     São Paulo|           1447|248219.94022870064|              582.95|
|   SC| Florianópolis|            512| 95731.03971099854|              534.83|
|   CE|     Fortaleza|            745|148894.85010147095|              500.35|
|   RN|         Natal|            263|52809.720039367676|              498.01|
|   AL|        Maceió|            136|27843.360107421875|              488.44|
|   SE|       Aracaju|            104| 21926.91986465454|               474.3|
|   PR|      Curitiba|            924|199305.38004684448|              463.61|
+-----+--------------+---------------+------------------+--------------------+

***** Processing times (excluding purification)
Without cache ............... 8573 ms
With cache .................. 11088 ms
With checkpoint ............. 3248 ms
With non-eager checkpoint ... 2711 ms
```

## 4, Some diffcult case

### checkpoint eager or not
```
checkpoint()        // eager default

checkpoint(false)   // lazy
```
