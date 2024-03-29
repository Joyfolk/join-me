lazy val root = (project in file("."))
  .settings(
    organization := "test",
    name := "join-me",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.12.8",
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-language:higherKinds",
      "-language:postfixOps",
      "-language:existentials",
      "-language:implicitConversions",
      "-feature",
      "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
      "-explaintypes",
      "-Xfatal-warnings",
      "-Xcheckinit",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused:implicits",
      "-Ywarn-unused:imports",
      "-Ywarn-unused:locals",
      "-Ywarn-unused:params",
      "-Ywarn-unused:patvars",
      "-Ywarn-unused:privates",
      "-Ywarn-value-discard",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
    ),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
      "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
    )
  )
