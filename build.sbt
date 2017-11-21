name := "http4s exercise"

version := "0.1"

scalaVersion := "2.12.4"

val http4sVersion = "0.17.5"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "0.9.7",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)