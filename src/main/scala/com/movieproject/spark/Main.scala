package com.movieproject.spark

import java.nio.charset.CodingErrorAction
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.io.Codec
import scala.io.Source
import scala.util.Try

object Main {

  def loadMovieTags(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieTags: Map[Int, String] = Map()

    val lines = Source.fromFile("src/main/resources/ml-latest/genome-tags.csv").getLines()
    for (line <- lines) {
      var fields = line.split(",")
      if (fields.length > 1) {
        movieTags += (fields(0).toInt -> fields(1))
      }
    }

    return movieTags
  }

  def parseLine(line : String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val dollarAmount = fields(2).toFloat
    (customerId, dollarAmount)
  }

  // user id | item id | rating | timestamp
  case class Rating(ratingId: Integer, itemId: Integer, rating: Double, timestamp: Timestamp)

  case class GenomeTag(tagId: Integer, tagName: String, relevance: Double)

  case class MovieTag(tagName: String, userId: Integer, movieId: Integer, timestamp: Timestamp)

  case class Movie(movieId: Integer, movieTitle: String, releaseYear: Int, genres: List[String])

  def main(args: Array[String]): Unit = {

//    val movieTags = loadMovieTags()
    val sc = new SparkContext("local[*]", "MovieAnalyser")

    val input = sc.textFile("src/main/resources/ml-latest/movies.csv")

    val spark = SparkSession
      .builder()
      .appName("Spark movie analyser")
      .getOrCreate()

    // ---- Clean movie entries ----

    import spark.implicits._
    val movieFile = spark.read.textFile("src/main/resources/ml-latest/movies.csv")
    val header = movieFile.first()
    val yearMatcher = "\\(\\d{4}\\)".r
    val movieDataset: Dataset[Movie] = movieFile
      .filter(line => line != header)
      .filter(line => !yearMatcher.pattern.matcher(line.split(",")(1)).matches)
      .filter(line => Try(line(2)).isSuccess)
      .filter(line => Try(line.split(",")(2).nonEmpty).isSuccess)
      .map(line => {
        val columns = line.split(""",(?=([^\"]*\"[^\"]*\")*[^\"]*$)""")
        val pattern = """(?<=\()\d{4}(?=\))""".r
        val releaseYear = pattern.findFirstIn(columns(1)).getOrElse("9999") // If value is non existent, defaults to 9999
        Movie(columns(0).trim.toInt, columns(1).replaceAll("\\(\\d{4}\\)", "").trim,
          releaseYear.toInt, columns(2).split("\\|").toList)
      })

    // ---- Clean ratings entries -----

    val ratingFile = spark.read.textFile("src/main/resources/ml-latest/ratings.csv")
    val ratingsHeader = ratingFile.first()
    val ratingsDataset: Dataset[Rating] = ratingFile
        .filter(line => line != ratingsHeader)
        .map(line => {
          val columns = line.split(",")
          Rating(columns(0).toInt, columns(1).toInt, columns(2).toDouble, new Timestamp(Instant.ofEpochSecond(columns(3).trim.toLong).toEpochMilli))
        })

    // --- Clean tags entries -------
    //case class MovieTag(tagName: String, userId: Integer, movieId: Integer, timestamp: Timestamp)
    //userId,movieId,tag,timestamp
    val tagFile = spark.read.textFile("src/main/resources/ml-latest/tags.csv")
    val tagHeader = tagFile.first()
    val tagsDataset: Dataset[MovieTag] = tagFile
        .filter(line => line != tagHeader)
        .map(line => {
          val columns = line.split(""",(?=([^\"]*\"[^\"]*\")*[^\"]*$)""")
          columns(2).replaceAll("[^A-Za-z0-9]", " ")
          MovieTag(columns(2).toLowerCase.trim, columns(0).toInt, columns(1).toInt, new Timestamp(Instant.ofEpochSecond(columns(3).trim.toLong).toEpochMilli))
        })


    tagsDataset.show()

//    // Show user dataset
//    usersDataset.show()
//    val mappedPairs = input.map(parseLine)
//
////    val secondMap = mappedPairs.map(x => (x._2, x._1))
//
//    val idCounts = mappedPairs.reduceByKey( (x,y) => x + y )
//
////    val sortedVals = idCounts.sortBy(_._2)
////    val sortedVals = idCounts.map(x => (x._2, x._1)).sortByKey(ascending = false)
//
//    val sortedVals2 = idCounts.sortBy((_._2), ascending = false)
//    val results = sortedVals2.collect()
//
//
//
//    for (result <- results) {
//      println(result)
//    }
//    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
  }
}
