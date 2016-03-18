name := "dbshim"

version := "1.0.3-SNAPSHOT"

organization := "org.phasanix"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
  "org.scala-lang"    %  "scala-compiler"  % scalaVersion.value,
  "org.scalatest"     %% "scalatest"       % "2.2.6"   % "test",
  "com.h2database"    %  "h2"              % "1.4.191" % "test"
)

