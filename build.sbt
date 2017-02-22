name := "dbshim"

version := "1.0.5-SNAPSHOT"

organization := "org.phasanix"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq (
  "org.scala-lang"    %  "scala-compiler"  % scalaVersion.value,
  "org.scalatest"     %% "scalatest"       % "3.2.0-SNAP4"   % "test",
  "com.h2database"    %  "h2"              % "1.4.193"       % "test"
)

