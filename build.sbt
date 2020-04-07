name := "Project2"

version := "0.1.0"

scalaVersion := "2.12.8"
val sparkVersion = "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.13",
                            "org.slf4j" % "slf4j-log4j12" % "1.7.13")

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.8.1" % "test"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}