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

object Finaliser {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MovieAnalyserFinaliser")
    val spark = SparkSession
      .builder
      .appName("Spark movie analyser finaliser")
      .master("local[*]")
      .getOrCreate()

    val movies = spark.read.parquet("src/main/resources/movies.parquet")
    val ratings = spark.read.parquet("src/main/resources/ratings_real.parquet")
    val tags = spark.read.parquet("src/main/resources/tags_real.parquet")
    val genome = spark.read.parquet("src/main/resources/genome.parquet")
    val tagRatings = spark.read.parquet("src/main/resources/tagRatings.parquet")
    val tagRatingTSWeight = spark.read.parquet("src/main/resources/tagRatingTSWeight.parquet")

    ratings.show()
    import spark.implicits._

    // Timestamp weighting = rating / (current_year + 1 - year_of_rating)

    val ratingsWeighted = ratings.withColumn("year", year($"timestamp"))
      .withColumn("tsWeighted", $"rating".divide(lit(2019) - $"year"))


    // Average movie rating
    val avg1 = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
      .select("movieId", "movieTitle", "tsWeighted")
      .groupBy("movieId")
      .agg(avg("tsWeighted"))
      .join(movies, "movieId")
      .select($"avg(tsWeighted)".alias("avgRating"), $"movieTitle", $"movieId")


    val avg2 = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
      .select("movieId", "movieTitle", "tsWeighted")
      .groupBy("movieId")
      .agg(count("tsWeighted"))
      .join(movies, "movieId")
      .select($"count(tsWeighted)", $"movieTitle", $"movieId")

    val resAvg = avg1.join(avg2, avg1("movieId") === avg2("movieId"))
      .select(avg1("movieId"), avg1("avgRating"), avg1("movieTitle"), avg2("count(tsWeighted)").as("countRating"))

    resAvg.show()

    // Avg rating per tag with threshold
    val tagJoin = genome.as("g")
      .join(resAvg.as("resAvg"), $"g.movieId" === $"resAvg.movieId")
      .select($"g.tagName", $"g.tagId", $"g.relevance", $"g.movieId", $"resAvg.avgRating", $"resAvg.countRating")
      .where($"g.relevance" > 0.5)

    val weightedCalc = tagJoin.withColumn("weightedAvg", log("resAvg.countRating") * tagJoin("resAvg.avgRating"))






//    weightedCalc.show()

//    weightedCalc.select("tagName",  "weightedAvg").groupBy("tagName").agg(avg("weightedAvg")).orderBy(desc("avg(weightedAvg)"))

    weightedCalc.select("tagName",  "weightedAvg").groupBy("tagName").agg(avg("weightedAvg")).orderBy(desc("avg(weightedAvg)")).show(false)



  }


}
