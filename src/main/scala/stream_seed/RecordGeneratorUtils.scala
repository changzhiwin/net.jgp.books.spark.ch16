package net.jgp.books.spark.ch10.stream_seed

import java.io.File
import scala.io.Source
import scala.util.Random

object RecordGeneratorUtils {

  val seed = new Random(42)

  def getName: String = s"${getFirstName} ${getLastName}"

  def getRating: Int = getRandomInt(3) + 3

  def getLang: String = language(getRandomInt(language.length))

  // recent 10 years, base 2022
  def getRecentYears: Int = 2022 - getRandomInt(10)

  def getTitle: String = s"${articles(getRandomInt(articles.length))} ${adjectives(getRandomInt(adjectives.length))} ${nouns(getRandomInt(nouns.length))}}"

  def getFirstName: String = {
    val rand = getRandomInt(2)
    rand match {
      case 0  => femaleFirstNames(getRandomInt(femaleFirstNamesCount))
      case 1  => maleFirstNames(getRandomInt(maleFirstNamesCount))
    }
  }

  def getLastName: String = {
    lastNames(getRandomInt(lastNamesCount))
  }

  def getRandomSSN: String = "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + 
      "-" + getRandomInt(10) + getRandomInt(10) +
      "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)

  def getRandomInt(upper: Int): Int = {
    seed.nextInt(upper)
  }

  lazy val femaleFirstNames = Source.fromFile("data/female_first_names.txt").getLines.filter(l => !l.startsWith("#")).toArray

  val femaleFirstNamesCount = femaleFirstNames.length

  lazy val maleFirstNames   = Source.fromFile("data/male_first_names.txt").getLines.filter(l => !l.startsWith("#")).toArray

  val maleFirstNamesCount = maleFirstNames.length

  lazy val lastNames        = Source.fromFile("data/last_names.txt").getLines.filter(l => !l.startsWith("#")).toArray

  val lastNamesCount = lastNames.length

  // not support
  val articles = Array("The", "My", "A", "Your", "Their", "Our")

  val adjectives = Array("", "Great", "Beautiful", "Better", "Terrific", "Fantastic", 
      "Nebulous", "Colorful", "Terrible", "Natural", "Wild", "Worse", "Gorgeous" )

  val nouns = Array("Life", "Trip", "Experience", "Work", "Job", "Beach", "Sky")

  // val daysInMonth = Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  val language = Array("fr", "en", "es", "de", "it", "pt")
}