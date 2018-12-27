name := "rdbms"

version := "0.1"

scalaVersion := "2.12.7"

scalacOptions += "-Ypartial-unification"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"
libraryDependencies += "org.typelevel" %% "cats-core" % "1.4.0"
libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.1.0"