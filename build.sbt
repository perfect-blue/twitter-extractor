
name := "twitter-feature-extractor"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3",
  "org.apache.spark" %% "spark-tags" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}