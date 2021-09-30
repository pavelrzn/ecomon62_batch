name := "ecomon62"

version := "0.1"

scalaVersion := "2.12.10"

//val sparkVersion = "2.4.6"
val sparkVersion = "3.0.1"
val postgresVersion = "42.2.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)