name := "Taxi Project"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.1"
val sqlVersion = "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sqlVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion