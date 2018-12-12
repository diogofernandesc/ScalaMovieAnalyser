package com.movieproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}





object Modeller {
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
//    val officialTags = spark.read.parquet("src/main/resources/officialTags.parquet")
    val avgMovieRatings = spark.read.parquet("src/main/resources/avgMovieRatings.parquet")

    println(genome.count())
//    println(officialTags.count())

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


    // Average movie rating
//    val avgMovieRatings = movies.join(ratings, movies("movieId") === ratings("itemId"))
//      .select("movieId", "movieTitle", "rating")
//      .groupBy("movieId")
//      .agg(avg("rating"))
//      .join(movies, "movieId")
//      .select($"avg(rating)".alias("avgRating"), $"movieTitle", $"movieId")




//    val tempRatingsWithTags = avgMovieRatings.as("amr").join(officialTags.as("ot"), "movieId").select($"amr.movieTitle", $"amr.avg(rating)", $"ot.tagName")
//    tempRatingsWithTags.show()

    // Example select from temp ratings
    //    tempRatingsWithTags.select("tagName").where($"movieTitle" === "Spawn").show()


    // Get official tags
//    val officialTags = genome.as("g").join(tags.as("t"), "tagName").select($"t.userId", $"t.movieId", $"g.tagId", $"g.tagName", $"g.relevance")

    genome.createOrReplaceTempView("genome")
    tags.createOrReplaceTempView("tags")

    val result = spark.sql("select t.userId, t.movieId, g.tagId, g.tagName, g.relevance " +
      "from genome g " +
      "inner join tags t on g.tagName <> t.tagName")

    result.show()
    println(result.count())

//    println(genome.count())
//    println(tags.count())
    // Official tags reworked
//    val officialTags = genome.as("g").join(tags.as("t"), $"g.tagName" === $"t.tagName", "leftsemi").select($"t.userId", $"t.movieId", $"g.tagId", $"g.tagName", $"g.relevance")
//    officialTags.show()
//    println(officialTags.count())


    // temp join

    val tempTagjoin = genome.as("g").join(tags.as("t"), $"g.tagName" === $"t.tagName", "right").select( $"g.tagId", $"g.tagName", $"g.relevance", $"g.movieId")
    tempTagjoin.show()
    println(tempTagjoin.count())

//    val officialTags = tempTagjoin.join()
    // Average score per tag
//    val avgTag = ratings.join(tags, ratings("itemId") === tags("movieId")).select("movieId", "tagName", "rating").groupBy("tagName").agg(avg("rating"))


    // TODO: Official tag avg scoring

//    val avgTagOfficial = officialTags
//      .select("movieId", "tagName")
//      .join(ratings, officialTags("movieId") === ratings("itemId"))
//      .select("movieId", "tagName", "rating")
//      .groupBy("tagName")
//      .agg(avg("rating"))
//
//    println("now saving to parquet")
//    avgTagOfficial.show(10)
//    avgTagOfficial.write.mode(SaveMode.Overwrite).format("parquet").save("src/main/resources/officialTagScoring.parquet")


    // ------- Average tag scoring threshold -------




//    val figure = RowFigure(2)
//      .plot(0) { // left-hand plot
//        Plot().withScatter(
//          xsLeft, ysLeft,
//          ScatterOptions().mode(ScatterMode.Marker).name("left"))
//      }
//
//
//    draw(figure, "make-subplots", writer.FileOptions(overwrite=true))





    // TODO: Movies by genre grouping

  }
}
