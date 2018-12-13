package com.movieproject
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._
import org.apache.log4j.{Level, Logger}

import scala.util.Random


object Plotter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Spark movie analyser plotter")
      .master("local[*]")
      .getOrCreate()

    val ratings = spark.read.parquet("src/main/resources/ratings.parquet")

    import spark.implicits._

    // TODO: Plotting ratings over time

//    val ratingList = ratings.select("rating").limit(100).orderBy("timestamp").map(r => r.getDouble(0)).collect.toList
//    val timestampList = ratings.select("timestamp").limit(100).orderBy("timestamp").map(r => r.getTimestamp(0).getTime.toDouble).collect.toList
    val ratingList = ratings.select("rating", "timestamp").sample(0.5).limit(10000)
//    val figure = RowFigure(2)
//      .plot(0) { // left-hand plot
//        Plot().withScatter(
//          timestampList, ratingList,
//          ScatterOptions().mode(ScatterMode.Marker).name("left"))
//      }

    Vegas("Sample Scatterplot", width = 600.0, height = 400.0)
      .withDataFrame(ratingList)
//      .withDataFrame(timestampList.toDF())
//      .withDataFrame(timestampList)
      .mark(Area)
      .encodeX("timestamp", Temporal, timeUnit = TimeUnit.Yearmonth,
        axis = Axis(axisWidth = 0.0, format = "%Y", labelAngle = 0.0, tickSize = Some(0.0)),
        scale = Scale(nice = spec.Spec.NiceTimeEnums.Year),
        bin=Bin(maxbins=8.0))
      .encodeY("rating", Quantitative)
      .show

  }

}
