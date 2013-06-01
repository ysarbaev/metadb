import sbt._
import Keys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++
    org.sbtidea.SbtIdeaPlugin.ideaSettings ++
    Seq(
      organization := "com.sarbaev",
      version := "0.1",
      scalacOptions ++= Seq(),
      scalaVersion := "2.11.0-M3",

      org.sbtidea.SbtIdeaPlugin.addGeneratedClasses := true,
      org.sbtidea.SbtIdeaPlugin.includeScalaFacet := true,
      org.sbtidea.SbtIdeaPlugin.defaultClassifierPolicy := false,
      org.sbtidea.SbtIdeaPlugin.commandName := "idea",

      scalacOptions ++= Seq("-feature"),

      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % "2.11.0-M3",

        "postgresql" % "postgresql" % "9.1-901.jdbc4"
      )
    )
}

object MetadbBuild extends Build {

  import BuildSettings._


  lazy val core: Project = Project(
    "metadb",
    file("."),
    settings = buildSettings
  )
}
