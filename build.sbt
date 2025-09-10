import Dependencies.library

ThisBuild / organization:= "My organization"
ThisBuild / scalaVersion := "2.13.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.5.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.4"
ThisBuild / version := "LATEST"
scalacOptions ++= Seq("-target:jvm-1.8")

lazy val root = (project in file("."))
  .settings(
    name := "test-project",

    libraryDependencies ++= Seq(
      library.sparkSql
    )
  )