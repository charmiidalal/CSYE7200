package edu.neu.coe.csye7200.asstsmra

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * @author Yanda Yuan
 * Compare different methods on different functions, which will be better on your project?
 * wordCountSpec can be used as an example if you don't know what the result will be like.
 * To help you understand more about the difference among functions: http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html
 */
object movieRating extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieRating")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val movieMetadata: DataFrame  = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/edu/neu/coe/csye7200/asstsmra/movie_metadata.csv")

  val movie_analysis : DataFrame  = movieMetadata.select("movie_title", "imdb_score").groupBy("movie_title").agg(functions.avg("imdb_score"))
  val movie_deviation : DataFrame  = movieMetadata.select("imdb_score").agg(functions.stddev("imdb_score"))

  println(s"Mean of Movie rating by Movie Title")
  println(s"*************************************")
  for(movie_mean <-movie_analysis.collect()){
    println(s"Mean of Rating of ${movie_mean(0)} -  ${movie_mean(1)}")
  }
  println(s"*************************************")
  println(s"Standard Deviation of Movie Rating for All Movies: ${movie_deviation.collect()(0)}")
}