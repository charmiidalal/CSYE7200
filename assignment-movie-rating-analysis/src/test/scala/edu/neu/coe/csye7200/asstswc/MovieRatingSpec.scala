package edu.neu.coe.csye7200.asstswc

import edu.neu.coe.csye7200.asstsmra.MovieRatingObj
import org.apache.spark.sql.{SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfter, flatspec}

import scala.io.Source
import scala.util.Try

/**
 * @author Charmi Dalal
 */
class MovieRatingSpec extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter  {

  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("MovieRating")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  behavior of "Spark"

  it should "return success for movie metadata valid path" taggedAs Slow in {
    val triedPath = Try(Source.fromResource("movie_metadata.csv"))
    triedPath.isSuccess shouldBe true
  }

  it should "return standard deviation of all movies" taggedAs Slow in {
    MovieRatingObj.standardDevMovieRating() should matchPattern {
      case 0.9988071293753289 =>
    }
  }

  it should "return mean rating per movie" taggedAs Slow in {
    val movie_mean =  MovieRatingObj.meanMovieRating().collect()
    movie_mean should matchPattern {
      case Array(_*) =>
    }
    movie_mean(0).get(0).toString.equals("Eraser")
    movie_mean(0).get(1).equals(6.1)
  }

  it should "return mean rating for all movies" taggedAs Slow in {
    MovieRatingObj.meanMovieRatingByAll() should matchPattern {
      case 6.453200745804848 =>
    }
  }
}
