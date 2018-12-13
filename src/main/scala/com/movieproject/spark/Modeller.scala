package com.movieproject

import com.movieproject.spark.Main.{GenomeTag, Rating}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar






object Modeller {


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




//    println(officialTags.count())

    //  case class Rating(ratingId: Integer, itemId: Integer, rating: Double, timestamp: Timestamp)
    //
    //  case class GenomeTag(tagId: Integer, movieId: Integer, tagName: String, relevance: Double)
    //
    //  case class MovieTag(tagName: String, userId: Integer, movieId: Integer, timestamp: Timestamp)
    //
    //  case class Movie(movieId: Integer, movieTitle: String, releaseYear: Int, genres: List[String])

    import spark.implicits._

    val resAvg = spark.read.parquet("src/main/resources/avgMovieRatings.parquet")


    val tagRatings = spark.read.parquet("src/main/resources/tagRatings.parquet")

    // Movie tag average score with threshold
//    tagRatings.groupBy("tagName").agg(avg($"weightedAvg"))

    ratings.select("timestamp").agg(max("timestamp")).show()


    // Time decay ranking of ratings for movies using reddit formula

    val formattedTimes = ratings
      .withColumn("latest", lit(getLatestTimestamp))
      .select($"rating", $"itemId", datediff($"latest", $"timestamp").as("timeDifference"),
        round(datediff($"latest", $"timestamp").divide(lit(7))).as[Double].as("Weeks"))
      .withColumn("Weeks", when($"Weeks" > 1, $"Weeks" - 1))
      .withColumn("loggedRating", log($"rating"))
//      .filter($"timeDifference" > 1).map(row => $"timeDifference").map(row => row - 1)


    val newDs = formattedTimes.withColumn("weightedTimestampRating", lit($"loggedRating", 1) * exp(lit(-8) * $"Weeks".multiply($"Weeks")))
    newDs.select("*").where($"weightedTimestampRating" !== 0.0).show()









    // TODO: Movies by genre grouping

  }
}
