import sbt._
import Keys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ org.sbtidea.SbtIdeaPlugin.ideaSettings ++ Seq(
    organization := "com.sarbaev",
    version := "0.1",
    scalacOptions ++= Seq(),
    scalaVersion := "2.10.1",

    org.sbtidea.SbtIdeaPlugin.addGeneratedClasses := true,
    org.sbtidea.SbtIdeaPlugin.includeScalaFacet := true,
    org.sbtidea.SbtIdeaPlugin.defaultClassifierPolicy := false,
    org.sbtidea.SbtIdeaPlugin.commandName := "idea",

    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.10.1",

      "org.scalamock" % "scalamock-scalatest-support_2.10" % "3.0" % "test",

      "org.scalamock" % "scalamock-core_2.10" % "3.0" % "test"
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
