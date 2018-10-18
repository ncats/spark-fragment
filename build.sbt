
lazy val commonDependencies = Seq(
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.1"
//  "com.databricks" % "spark-xml_2.11" % "0.4.1",
//  "com.amazonaws" % "aws-java-sdk" % "1.11.381"
)

lazy val commonSettings = Seq(
  name := """spark-probedb""",
  scalaVersion := "2.11.8",
  version := "0.0.1"
)

lazy val javaBuildOptions = Seq(
  "-encoding", "UTF-8"
    //,"-Xlint:-options"
    //,"-Xlint:deprecation"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= commonDependencies,
    javacOptions ++= javaBuildOptions
  )
