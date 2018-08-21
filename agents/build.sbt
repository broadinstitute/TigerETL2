name := "Agents"

lazy val commonSettings = Seq(
  version := "1.0",
  organization := "org.broadinstitute",
  scalaVersion := "2.12.4" // "2.11.8"
)


libraryDependencies ++= {
  Seq(
    "org.typelevel" %% "cats-core" % "1.0.0-RC1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
    //"org.apache.spark" % "spark-core_2.11" % "2.0.0",
    //"com.databricks" % "spark-xml_2.11" % "0.4.1",
    //"org.apache.spark" % "spark-sql_2.11" % "2.0.0",
    //"mysql" % "mysql-connector-java" % "6.0.3"
    //"org.scalikejdbc" %% "scalikejdbc"       % "2.2.0",
    //"org.scalikejdbc" %% "scalikejdbc-config"  % "2.2.0"
  )
}
