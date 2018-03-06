import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.4",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "kafka-test",
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream-kafka" % "0.19",
        "org.apache.curator" % "curator-framework" % "4.0.1",
        "org.apache.kafka" %% "kafka" % "1.0.0",
        scalaTest % Test,
        "org.apache.curator" % "curator-test" % "4.0.1" % Test,
        "org.apache.kafka" % "kafka-clients" % "1.0.0" % Test//,
        //"ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
        //"org.slf4j" % "slf4j-simple" % "1.7.25" % Test

    )

)
