name := "ElevateBackendSystem"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-hive" % "3.2.0", // Add this if you need Hive support
  "org.postgresql" % "postgresql" % "42.7.4"
)
