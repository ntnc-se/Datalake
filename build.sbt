
name := "xgaming-2.11"

version := "0.1"

scalaVersion := "2.11.8"
unmanagedBase := baseDirectory.value / "lib"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.4" % "provided"


// https://mvnrepository.com/artifact/org.scalaj/scalaj-http
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.dataformat/jackson-dataformat-yaml
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.7.1"

// https://mvnrepository.com/artifact/commons-httpclient/commons-httpclient
libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1"



mainClass in assembly := Some("com.viettel.datalake.start.RunApplication")

assemblyJarName in assembly := "xgaming_reportv47.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)

// META-INF discarding for the FAT JAR
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}