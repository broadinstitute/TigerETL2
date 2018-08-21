import java.nio.file.Files
import sbt.Keys._

name := "Spark-agents"

libraryDependencies ++= {
  Seq(
    //"org.apache.spark" % "spark-core_2.11" % "2.0.0",
    //"com.databricks" % "spark-xml_2.11" % "0.4.1",
    "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
    //"mysql" % "mysql-connector-java" % "6.0.3"
    //"org.scalikejdbc" %% "scalikejdbc"       % "2.2.0",
    //"org.scalikejdbc" %% "scalikejdbc-config"  % "2.2.0"
  )
}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}

/*
artifact in (Compile, packageBin) := {
  val previous: Artifact = (artifact in (Compile, packageBin)).value
  previous.copy(`type` = "bundle")
}


lazy val deployTask = taskKey[Unit]("deploy")
deployTask <<= packageBin map { file =>
  val destDir = new File("""J:\TigerETL""")
  Files.copy(file.toPath, new File(destDir, file.name).toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
}
*/