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

  var movieGenres: Map[Int, String] = Map()

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
    val ratings = spark.read.parquet("src/main/resources/ratings_real.parquet")
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
      "War", "Western")

    val genreList1 = List("Action")

    val newList = genreList1.map(s => movies
    .filter(array_contains($"genres", s))
    .select("movieId").map(r => r.getInt(0)).collect.toList).zipWithIndex.map(t => genreList(t._2) -> t._1).toMap


    //TODO Average movie ratings parquet needs to be recomputed otherwise this is fine
    for ((k,v) <- newList) avgMovieRatings.select("*").where($"movieId".isin(v:_*)).write.mode(SaveMode.Overwrite).format("parquet").save(f"src/main/resources/$k%s.parquet")
//    println(genreList(0).length)
//    val genresDF = movies
//      .filter(array_contains($"genres", "Children"))


//    val genresAction = movies
//      .filter(array_contains($"genres", "Action"))
//    genresAction.show()
//
//    val genresAdventure = movies
//      .filter(array_contains($"genres", "Adventure"))
//
//    val genresAnimation = movies
//      .filter(array_contains($"genres", "Animation"))
//
//    val genresChildren = movies
//      .filter(array_contains($"genres", "Children"))
//
//    val genresComedy = movies
//      .filter(array_contains($"genres", "Comedy"))
//
//    val genresCrime = movies
//      .filter(array_contains($"genres", "Crime"))
//
//    val genresDocumentary = movies
//      .filter(array_contains($"genres", "Animation"))
//
//    genresAction.write.mode(SaveMode.Overwrite).format("parquet").save("src/main/resources/ActionMovies.parquet");

//    println(genresDF.count())
//    movies.show()




//    finalJoin.select("tagName", "movieId", "weightedAvg").groupBy("tagName").agg(avg("weightedAvg")).orderBy(desc("avg(weightedAvg)")).show()


    // TODO: Movies by genre grouping

  }
}
