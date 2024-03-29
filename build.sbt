name := "Taxi Project"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"
val sqlVersion = "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sqlVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"