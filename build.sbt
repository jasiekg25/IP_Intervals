name := "spark_project"

version := "0.1"

scalaVersion := "2.12.13"

val SparkVersion = "3.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.apache.spark" %% "spark-core" % SparkVersion

)
