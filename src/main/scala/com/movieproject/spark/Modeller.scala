package com.movieproject

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object Modeller {
  def main(args: Array[String]): Unit = {

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

    // Movies by tag grouping
//    movies.join(tags, "movieId").select("movieId", "movieTitle", "tagName", "userId").show()
//    tags.join(movies, "movieId").select("tagName", "movieTitle").orderBy(desc("tagName")).show()
//    val taggedMovies = tags.join(movies, "movieId").select("tagName", "movieId", "movieTitle")
//    taggedMovies.join(ratings, taggedMovies("movieId") === ratings("itemId"))
//      .select("movieId", "movieTitle", "rating")
//      .groupBy("movieId").agg(avg("rating")).select("movieTitle")
//      .show()


//    // Average movie rating
    val avgMovieRatings = movies.join(ratings, movies("movieId") === ratings("itemId"))
      .select("movieId", "movieTitle", "rating")
      .groupBy("movieId")
      .agg(avg("rating"))
      .join(movies, "movieId")
      .select("avg(rating)", "movieTitle", "movieId")

    val tempRatingsWithTags = avgMovieRatings.join(tags, "movieId").select("movieTitle", "avg(rating)", "tagName")

    // Example select from temp ratings
    //    tempRatingsWithTags.select("tagName").where($"movieTitle" === "Spawn").show()

    // Get official tags
    val officialTags = genome.as("g").join(tags.as("t"), "tagName").select($"t.userId", $"t.movieId", $"g.tagId", $"g.tagName", $"g.relevance")
    officialTags.show()


    // Movies by genre grouping

  }
}
