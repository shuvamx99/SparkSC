ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "SparkProject"
  )



libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "commons-io" % "commons-io" % "2.11.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test

