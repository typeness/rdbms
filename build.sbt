name := "rdbms"

version := "0.1"

scalaVersion := "2.13.2"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Ywarn-unused:locals",              // Warn if a local definition is unused.
  "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates",            // Warn if a private member is unused.
)


libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.2.0-M1"
libraryDependencies += "com.lihaoyi" %% "fastparse" % "2.3.0"
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.9.5"