import sbt._
import Keys._

object BuildSettings {
  val buildScalaVersion = "2.11.0-SNAPSHOT"

  val buildSettings = Defaults.defaultSettings ++
    org.sbtidea.SbtIdeaPlugin.ideaSettings ++
    Seq(
      organization := "com.sarbaev",
      version := "0.1",
      scalacOptions ++= Seq(),
      scalaVersion := buildScalaVersion,
      scalaOrganization := "org.scala-lang.macro-paradise",

      org.sbtidea.SbtIdeaPlugin.addGeneratedClasses := true,
      org.sbtidea.SbtIdeaPlugin.includeScalaFacet := true,
      org.sbtidea.SbtIdeaPlugin.defaultClassifierPolicy := false,
      org.sbtidea.SbtIdeaPlugin.commandName := "idea",

      scalacOptions ++= Seq("-feature"),

      libraryDependencies ++= Seq(
        "org.scala-lang.macro-paradise" % "scala-reflect" % buildScalaVersion
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
