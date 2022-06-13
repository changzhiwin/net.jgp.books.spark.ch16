package net.jgp.books.spark

import net.jgp.books.spark.ch16.lab100_cache_checkpoint.CacheCheckpoint
import net.jgp.books.spark.ch16.lab200_brazil_stats.BrazilStatistics

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "1000")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("", "1000")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "BR"       => BrazilStatistics.run()
      case _          => CacheCheckpoint.run(otherArg)
    }
  }
}