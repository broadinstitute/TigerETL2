name := "Spark"

libraryDependencies ++= {
  Seq(
    //"org.apache.spark" % "spark-core_2.11" % "2.0.0",
    //"com.databricks" % "spark-xml_2.11" % "0.4.1",
    "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
    "mysql" % "mysql-connector-java" % "6.0.3"
    //"org.scalikejdbc" %% "scalikejdbc"       % "2.2.0",
    //"org.scalikejdbc" %% "scalikejdbc-config"  % "2.2.0"
  )
}
