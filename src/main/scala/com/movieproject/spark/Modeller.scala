package com.movieproject

import com.movieproject.spark.Main.{GenomeTag, Rating}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable

object Modeller {

  private def isElementContainedInColumn(element: String => Boolean):
  UserDefinedFunction = udf((column: mutable.WrappedArray[String])  => column.contains(element))

  def getLatestTimestamp: Timestamp ={
    val today:java.util.Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val last:String = timeFormat.format("2018-09-26 07:59:09")
    val re = java.sql.Timestamp.valueOf("2018-09-26 07:59:09")
    re
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MovieAnalyser")
    val spark = SparkSession
      .builder
      .appName("Spark movie analyser")
      .master("local[*]")
      .getOrCreate()

    val movies = spark.read.parquet("src/main/resources/movies.parquet")
    val ratings = spark.read.parquet("src/main/resources/ratings.parquet")
    val tags = spark.read.parquet("src/main/resources/tags.parquet")
    val genome = spark.read.parquet("src/main/resources/genome.parquet")


    //  case class Rating(ratingId: Integer, itemId: Integer, rating: Double, timestamp: Timestamp)
    //
    //  case class GenomeTag(tagId: Integer, movieId: Integer, tagName: String, relevance: Double)
    //
    //  case class MovieTag(tagName: String, userId: Integer, movieId: Integer, timestamp: Timestamp)
    //
    //  case class Movie(movieId: Integer, movieTitle: String, releaseYear: Int, genres: List[String])

    import spark.implicits._

    val genreList = List("Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
      "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller",
      "War", "Western").map(s => movies
      .filter(array_contains($"genres", s))
      .select("movieId").map(r => r.getInt(0)).collect.toList)

    println(genreList(0).length)
    val genresDF = movies
      .filter(array_contains($"genres", "Adventure"))


    println(genresDF.count())
//    movies.show()




//    finalJoin.select("tagName", "movieId", "weightedAvg").groupBy("tagName").agg(avg("weightedAvg")).orderBy(desc("avg(weightedAvg)")).show()


    // TODO: Movies by genre grouping

  }
}
