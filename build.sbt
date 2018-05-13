import AssemblyKeys._ // put this at the top of the file

name := "Twitterstats"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
	//"org.apache.spark" % "spark-streaming_2.11" % "1.5.2",
	//"org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2",
	
	"org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
	"org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
	"org.apache.tika" % "tika-core" % "1.13",
	"org.apache.tika" % "tika-parsers" % "1.13",
	"org.scalatest" %% "scalatest" % "2.2.0" % "test",
	"com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.9.0" % "test"
)

//resourceDirectory in Compile := baseDirectory.value / "resources"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

//fork in Test := true
//javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

//parallelExecution in Test := false
