name := "dbshim"

version := "1.0.6-SNAPSHOT"

organization := "org.phasanix"

scalaVersion := "2.13.5"

libraryDependencies ++= Seq (
  "org.scala-lang"    %  "scala-compiler"  % scalaVersion.value,
  "org.scalatest"     %% "scalatest"       % "3.2.5"   % "test",
  "com.h2database"    %  "h2"              % "1.4.200" % "test"
)

