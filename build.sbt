name := "custome-streaming-receiver"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.1"