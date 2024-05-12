ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark_heuristics",
    idePackagePrefix := Some("org.spark_heuristics"),
    libraryDependencies += "org.playframework" %% "play-json" % "3.0.3",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test
  )
