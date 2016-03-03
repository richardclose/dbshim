name := "dbshim"

version := "1.0.1"

organization := "org.phasanix"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq (
  "org.scala-lang"    %  "scala-compiler"  % "2.11.7",
  "org.scalatest"     %% "scalatest"       % "2.2.5"   % "test",
  "com.h2database"    %  "h2"              % "1.4.188" % "test"
)

