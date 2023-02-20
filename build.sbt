// give the user a nice default project!

val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "io.lgos",
      scalaVersion := "2.13.10"
    )),
    name := "du-odd-occurrence-finder",
    version := "0.0.1",

    sparkVersion := "3.3.0",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    Test / parallelExecution := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.2",

      "org.scalatest" %% "scalatest" % "3.2.2" % "test",
      "org.scalacheck" %% "scalacheck" % "1.15.2" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "3.3.0_1.3.0" % "test"
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    Compile / run := Defaults.runTask(Compile / fullClasspath, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

   resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { _ => false },

    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )
