// ============================================================
// build.sbt — Distributed Hadoop Cluster Analytics Platform
// Course: 23AID302 — Big Data Analytics
// Scala 2.12.15 + Spark 3.2.1 (matches bde2020 cluster)
// ============================================================

ThisBuild / organization := "edu.project1"
ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.15"

// ── Core settings ───────────────────────────────────────────
lazy val root = (project in file("."))
  .settings(
    name := "bigdata-analytics",

    // ── Dependencies ──────────────────────────────────────
    libraryDependencies ++= Seq(

      // Spark Core (provided — already in cluster)
      "org.apache.spark" %% "spark-core"      % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-sql"       % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-mllib"     % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.2.1" % "provided",

      // Hadoop client (provided — already in cluster)
      "org.apache.hadoop" % "hadoop-client"   % "3.2.0" % "provided",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.11" % "test"
    ),

    // ── Assembly settings (fat jar) ────────────────────────
    assembly / assemblyJarName := "bigdata-analytics-assembly-1.0.0.jar",

    // Merge strategy to avoid conflicts with Spark provided jars
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => xs match {
        case "MANIFEST.MF" :: Nil     => MergeStrategy.discard
        case "services" :: _          => MergeStrategy.concat
        case _                        => MergeStrategy.discard
      }
      case "reference.conf"            => MergeStrategy.concat
      case x if x.endsWith(".proto")   => MergeStrategy.rename
      case _                           => MergeStrategy.first
    },

    // Exclude Spark/Hadoop from fat jar (they are in cluster classpath)
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { f =>
        f.data.getName.contains("spark") ||
        f.data.getName.contains("hadoop") ||
        f.data.getName.contains("scala-library")
      }
    },

    // ── Compiler options ────────────────────────────────────
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),

    // ── Source directories ──────────────────────────────────────
    // Scala sources are grouped by pipeline stage (numbered folders, see docs/PROJECT_MAP.md).
    // NOT the default src/main/scala/ layout, so list the stage folders explicitly (searched recursively).
    Compile / unmanagedSourceDirectories := Seq(
      baseDirectory.value / "01_preprocessing",
      baseDirectory.value / "02_batch_processing",
      baseDirectory.value / "03_machine_learning",
      baseDirectory.value / "04_anomaly_and_drift",
      baseDirectory.value / "05_streaming",
      baseDirectory.value / "06_benchmark_dag"
    ),

    // ── Test options ────────────────────────────────────────────
    // CMUUnitTests.scala lives at src/test/scala/ (standard SBT layout inside this dir)
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "scala",
    Test / fork             := true,
    Test / parallelExecution := false
  )
