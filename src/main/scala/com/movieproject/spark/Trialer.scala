package com.movieproject.spark

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

object Trialer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MovieAnalyserTrial")
    val spark = SparkSession
      .builder
      .appName("Spark movie analyser trial")
      .master("local[*]")
      .getOrCreate()

    val movies = spark.read.parquet("src/main/resources/movies.parquet")
    val ratings = spark.read.parquet("src/main/resources/ratings_real.parquet")
    val tags = spark.read.parquet("src/main/resources/tags_real.parquet")
    val genome = spark.read.parquet("src/main/resources/genome.parquet")
    val tagRatings = spark.read.parquet("src/main/resources/tagRatings.parquet")
    val tagRatingTSWeight = spark.read.parquet("src/main/resources/tagRatingTSWeight.parquet")

//    ratings.show()
//    tagRatingTSWeight.show()
//    tagRatingTSWeight.coalesce(1).write.csv("src/main/resources/weightedMovieRatingTimestamp")

    import spark.implicits._

    // Mapping average ratings per year

    ratings.agg(min("timestamp")).show()
    val timestamp_ds = tags.groupBy(year($"timestamp").alias("year"))
      .agg(count("tagName").alias("count")).orderBy(asc("year"))

    timestamp_ds.show(100)
//    timestamp_ds.coalesce(1).write.csv("src/main/resources/yearlyRatingCount")
//         Average movie rating
    val avg1 = movies.join(ratings, movies("movieId") === ratings("itemId"))
      .select("movieId", "movieTitle", "rating")
      .groupBy("movieId")
      .agg(avg("rating"))
      .join(movies, "movieId")
      .select($"avg(rating)".alias("avgRating"), $"movieTitle", $"movieId")




    val avg2 = movies.join(ratings, movies("movieId") === ratings("itemId"))
      .select("movieId", "movieTitle", "rating")
      .groupBy("movieId")
      .agg(count("rating"))
//      .filter($"count(rating)" > 10)
      .join(movies, "movieId")
      .select($"count(rating)", $"movieTitle", $"movieId")

    val resAvg = avg1.join(avg2, avg1("movieId") === avg2("movieId"))
      .select(avg1("movieId"), avg1("avgRating"), avg1("movieTitle"), avg2("count(rating)").as("countRating"))

//    resAvg.show()
//    resAvg.coalesce(1).write.csv("src/main/resources/movieRatingsSheet.csv")
//    resAvg.write.mode(SaveMode.Overwrite).format("csv").save("src/main/resources/movieRatingsSheet.csv");



    // ---- WEIGHTED

    // Avg rating per tag with threshold
    val tagJoin = genome.as("g")
      .join(resAvg.as("resAvg"), $"g.movieId" === $"resAvg.movieId")
      .select($"g.tagName", $"g.tagId", $"g.movieId", $"g.relevance", $"resAvg.avgRating", $"resAvg.countRating")
      .where($"g.relevance" > 0.5)

    val finalJoin = tagJoin.withColumn("weightedAvg", log("resAvg.countRating") * tagJoin("resAvg.avgRating"))
    finalJoin.show()

    finalJoin.select("tagName", "movieId", "weightedAvg").groupBy("tagName").agg(avg("weightedAvg")).orderBy(desc("avg(weightedAvg)")).show()

//    val finalJoin = tagJoin.withColumn("weightedAvg", log("resAvg.countRating") * tagJoin("resAvg.avgRating"))
//    finalJoin.show()
//    tagRatingTSWeight.coalesce(1).write.csv("src/main/resources/weightedMovieRatings05")
//    tag.write.mode(SaveMode.Overwrite).format("csv").save("src/main/resources/weightedMovieRatings05")












    // Examining the distribution of how people rated movies in total


  }

}
