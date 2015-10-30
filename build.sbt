import sbt.Keys._

name := "druidka"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.9"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.9"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.6" % "test"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.0.0"

libraryDependencies+= "com.typesafe.akka" %% "akka-persistence" % "2.4.0-RC3"

libraryDependencies+= "com.typesafe.akka" %% "akka-persistence-query-experimental" % "2.4.0-RC3"