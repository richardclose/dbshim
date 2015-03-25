name := "dbshim"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq (
  "org.scala-lang"    %  "scala-compiler"  % "2.11.6",
  "org.scalatest"     %% "scalatest"       % "2.2.4"   % "test",
  "com.h2database"    %  "h2"              % "1.4.186" % "test"
)

