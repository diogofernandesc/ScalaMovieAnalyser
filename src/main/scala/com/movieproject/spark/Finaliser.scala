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
    val avgMovieRatings = spark.read.parquet("src/main/resources/avgMovieRatings.parquet")
    val avgMovieRatingsSD = spark.read.parquet("src/main/resources/avgMovieRatingsSD.parquet")

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

    val stdDeviation = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
      .select("movieId", "movieTitle", "tsWeighted")
      .groupBy("movieId")
      .agg(stddev_pop("tsWeighted"))
      .join(movies, "movieId")
      .select($"stddev_pop(tsWeighted)".alias("stdDeviation"), $"movieId")

    val avg2 = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
      .select("movieId", "movieTitle", "tsWeighted")
      .groupBy("movieId")
      .agg(count("tsWeighted"))
      .join(movies, "movieId")
      .select($"count(tsWeighted)", $"movieTitle", $"movieId")

//    val resAvg = avg1.join(avg2, avg1("movieId") === avg2("movieId"))
//      .select(avg1("movieId"), avg1("avgRating"), avg1("movieTitle"), avg2("count(tsWeighted)").as("countRating"))

      val resAvg = avgMovieRatingsSD
//    val finalRes = resAvg.join(stdDeviation, resAvg("movieId") === stdDeviation("movieId")).select(avg1("movieId"), avg1("avgRating"), avg1("movieTitle"), resAvg("countRating"), stdDeviation("stdDeviation")).withColumn("CV", $"stdDeviation".divide($"avgRating"))
//    finalRes.show()
//    finalRes.write.mode(SaveMode.Overwrite).format("parquet").save("src/main/resources/avgMovieRatingsSD.parquet")

    // Avg rating per tag with threshold
    val tagJoin = genome.as("g")
      .join(resAvg.as("resAvg"), $"g.movieId" === $"resAvg.movieId")
      .select($"g.tagName", $"g.tagId", $"g.relevance", $"g.movieId", $"resAvg.avgRating", $"resAvg.countRating")
      .where($"g.relevance" > 0.5)

    val weightedCalc = tagJoin.withColumn("weightedAvg", log("resAvg.countRating") * tagJoin("resAvg.avgRating"))

    val tagAvgRatings = weightedCalc.select("tagId",  "weightedAvg")
      .groupBy("tagId").agg(avg("weightedAvg")).select("tagId", "avg(weightedAvg)")


    // average of standard deviation

    val stdDAvg = weightedCalc.select("tagId", "weightedAvg")
      .groupBy("tagId").agg(stddev_pop("weightedAvg"))
      .select($"tagId", $"stddev_pop(weightedAvg)".alias("stdDeviation"))

    val finalTagRatings = tagAvgRatings.join(stdDAvg, "tagId").select(tagAvgRatings("tagId"), $"avg(weightedAvg)", $"stdDeviation")
      .withColumn("standardised", $"avg(weightedAvg)".divide($"stdDeviation"))
      .where($"standardised" >= 2.5).where($"tagId" =!= lit(1099)) // 2.5 is the threshold here



//    finalTagRatings.coalesce(1).write.csv("src/main/resources/standardisedTagRatings")

//    finalTagRatings.show()

//    val decisionWeighting = tagAvgRatings.select("avg(weightedAvg)", "tagId")
//                            .withColumn("decisionScore", ($"avg(weightedAvg)" - lit(7.0)).divide(lit(7.0)) * lit(100))
//                            .select($"decisionScore", $"avg(weightedAvg)", $"tagId", $"stdDeviation")
////
//    val decisionSum = decisionWeighting.agg(sum("decisionScore")).first().getDouble(0)
////
//    val decisionScore = decisionWeighting.withColumn("allocation", $"decisionScore".divide(lit(decisionSum)) * lit(100))


    // Standardised version:

    val decisionWeighting = finalTagRatings.select("avg(weightedAvg)", "tagId", "standardised")
      .withColumn("decisionScore", ($"standardised" - lit(2.5)).divide(lit(2.5)) * lit(100))
      .select($"decisionScore", $"avg(weightedAvg)", $"tagId", $"standardised")
    //
    val decisionSum = decisionWeighting.agg(sum("decisionScore")).first().getDouble(0)
    //
    val decisionScore = decisionWeighting.withColumn("allocation", $"decisionScore".divide(lit(decisionSum)) * lit(100)).orderBy(desc("allocation"))

    decisionScore.show()
//    val finalTags = decisionScore.select(genome("tagId"), genome("tagName"), decisionScore("allocation"))
//    finalTags.show()
    // Genre allocation

    val genreList = List("Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
      "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller",
      "War", "Western")

    val genreList1 = List("Adventure", "Animation", "Children", "Comedy", "Crime",
      "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller",
      "War", "Western")


    val newList = genreList.map(s => movies
      .filter(array_contains($"genres", s))
      .select("movieId").map(r => r.getInt(0)).collect.toList).zipWithIndex.map(t => genreList(t._2) -> t._1).toMap

    val newList1 = genreList1.map(s => movies
      .filter(array_contains($"genres", s))
      .select("movieId").map(r => r.getInt(0)).collect.toList).zipWithIndex.map(t => genreList1(t._2) -> t._1).toMap

//    for ((k,v) <- newList) avgMovieRatings.select("movieId").where($"movieId".isin(v:_*)).withColumn("genre", lit(k)).join(genreDF, avgMovieRatings("movieId") === genreDF("movieId")).show()


//    for ((k,v) <- newList) avgMovieRatings.select("*").where($"movieId".isin(v:_*)).withColumn("genre", lit(k)).write.mode(SaveMode.Overwrite).format("parquet").save(f"src/main/resources/$k%s.parquet")

//    val genreDf = spark.read.parquet("src/main/resources/Action.parquet")
//      .union(spark.read.parquet("src/main/resources/Adventure.parquet"))
//      .union(spark.read.parquet("src/main/resources/Animation.parquet"))
//      .union(spark.read.parquet("src/main/resources/Children.parquet"))
//      .union(spark.read.parquet("src/main/resources/Comedy.parquet"))
//      .union(spark.read.parquet("src/main/resources/Crime.parquet"))
//      .union(spark.read.parquet("src/main/resources/Documentary.parquet"))
//      .union(spark.read.parquet("src/main/resources/Drama.parquet"))
//      .union(spark.read.parquet("src/main/resources/Fantasy.parquet"))
//      .union(spark.read.parquet("src/main/resources/Film-Noir.parquet"))
//      .union(spark.read.parquet("src/main/resources/Horror.parquet"))
//      .union(spark.read.parquet("src/main/resources/Musical.parquet"))
//      .union(spark.read.parquet("src/main/resources/Mystery.parquet"))
//      .union(spark.read.parquet("src/main/resources/Romance.parquet"))
//      .union(spark.read.parquet("src/main/resources/Sci-Fi.parquet"))
//      .union(spark.read.parquet("src/main/resources/Thriller.parquet"))
//      .union(spark.read.parquet("src/main/resources/War.parquet"))
//      .union(spark.read.parquet("src/main/resources/Western.parquet"))
    val genreDf = spark.read.parquet("src/main/resources/genreData.parquet")
//    genreDf.show()
//    genreDf.select("movieId").where($"genre" === "Action").show
//    genreDf.show()
//    genreDf.coalesce(1).write.csv("src/main/resources/genreDf")

    val genreRatings = genreDf.groupBy("genre").agg(avg("avgRating"))

//    val sumRatings = genreDf.groupBy("genre").agg(sum("movieId"))

    val finalRatings = genreRatings.withColumn("allocation", $"avg(avgRating)".divide(lit(18)) * lit(100))
    finalRatings.orderBy(desc("allocation")).show()

//    val genreScore = genreRatings.withColumn("allocation", )
//    genreRatings.coalesce(1).write.csv("src/main/resources/genreRatings")




//    for ((k,v) <- newList1) = genreDf.union(spark.read.parquet(f"src/main/resources/$k%s.parquet"))

//    genreDf.select("*").where($"genre" === "Western").show()
  }


}
