name := "MovieRatingAnalysis"

version := "1.0"

scalaVersion := "2.12.15"

val scalaTestVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.apache.spark" %% "spark-core" % scalaTestVersion,
  "org.apache.spark" %% "spark-sql" % scalaTestVersion
)