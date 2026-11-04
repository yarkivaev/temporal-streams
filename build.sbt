ThisBuild / organization := "io.yarkivaev"
ThisBuild / scalaVersion := "3.5.2"
ThisBuild / version := "0.1.0-SNAPSHOT"
lazy val root = project.in(file(".")).settings(
  name := "temporal-streams",
  scalacOptions ++= Seq("-Wunused:all", "-deprecation", "-feature", "-unchecked", "-source:future"),
  libraryDependencies ++= Seq("org.typelevel" %% "cats-effect" % "3.5.7", "co.fs2" %% "fs2-core" % "3.11.0", "org.scalameta" %% "munit" % "1.0.3" % Test),
  testFrameworks += new TestFramework("munit.Framework")
)
