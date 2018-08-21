import java.nio.file.Files
import sbt.Keys._
import complete.DefaultParsers._

val parseArgs = (args: Seq[String]) => args.foldLeft((Map.empty[String,String], Seq.empty[String])){ case ((options, ar), it) =>
  it.split("=") match {
    case Array(a, b) => (options.+((a, b)), ar)
    case _ => (options, ar.:+(it))
  }}


lazy val commonSettings = Seq(
  version := "1.0",
  organization := "org.broadinstitute",
  scalaVersion := "2.11.8"
)

lazy val root = Project("tigeretl", file(".")).enablePlugins(SbtTwirl)
  .settings(commonSettings)

lazy val `spark-agents` = (project in file("spark-agents"))
  .settings(commonSettings)
  .dependsOn(root)

lazy val `spark-agents2` = (project in file("spark-agents2"))
  .settings(commonSettings)
  .dependsOn(root)

lazy val agents = (project in file("agents"))
  .settings(commonSettings)
  .dependsOn(root)

name := "TigerETL"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// fork a new JVM for 'run' and 'test:run'
fork := true

javaOptions += "-Dconfig.file=\"C:\\Users\\atanas\\IdeaProjects\\TigerETL\\application.conf\""

// https://github.com/sbt/sbt-assembly/tree/0.11.2
//resolvers += "Atlassian Maven Repository" at "https://maven.atlassian.com/repository/public"

libraryDependencies ++= {
  val sprayVersion = "1.3.3"
  val akkaVersion = "2.3.14"
  Seq(
    "joda-time" % "joda-time" % "2.6",
    "javax.mail" % "mail" % "1.4.7",
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    //"org.scala-lang" % "scala-library" % scalaVersion.value,
    //"org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",

    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    //"com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "io.spray" % "spray-can_2.11" % sprayVersion,
    "io.spray" % "spray-routing_2.11" % sprayVersion,
    "io.spray" % "spray-testkit_2.11" % sprayVersion,
    "io.spray" % "spray-client_2.11" % sprayVersion,
    "io.spray" % "spray-json_2.11" % "1.3.2",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "org.scalikejdbc" %% "scalikejdbc"       % "2.2.0",
    "org.scalikejdbc" %% "scalikejdbc-config"  % "2.2.0",
    "ch.qos.logback"  %  "logback-classic"   % "1.1.2",
    "com.typesafe" % "config" % "1.3.0",
    "net.java.dev.jna" % "jna" % "3.5.1",
    "net.databinder.dispatch" % "dispatch-core_2.11" % "0.11.2",
    "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1",
    "org.typelevel" %% "cats" % "0.7.2",
    "net.virtual-void" % "json-lenses_2.11" % "0.6.2",
    "org.apache.activemq" % "activemq-client" % "5.15.0",
    "net.sourceforge.htmlunit" % "htmlunit" % "2.27"
    //"org.scala-lang" % "scala-swing" % "2.11.0-M7",
    //"jline" % "jline" % "2.14.5"
    //"org.scalafx" % "scalafx_2.11" % "8.0.144-R12"
    //"org.apache.spark" % "spark-sql_2.11" % "2.0.0"
    //"com.twitter" % "util-eval_2.10" % "6.30.0"
    //"com.atlassian.crowd" % "crowd-integration-client-rest" % "2.7.2",
    //"com.atlassian.security" % "atlassian-cookie-tools" % "3.2" jar
  )
}

// Add dependency on JavaFX library based on JAVA_HOME variable
// unmanagedJars in Compile += Attributed.blank(file(System.getenv("JAVA_HOME") + "/jre/lib/ext/jfxrt.jar"))

val deployTigerTask = TaskKey[Unit]("deployTiger", "Copies assembly jar to remote location")
// http://blog.bstpierre.org/writing-simple-sbt-task
// http://blog.byjean.eu/2015/07/10/painless-release-with-sbt.html
// https://eknet.org/main/dev/sbt-create-distribution-zip.html
deployTigerTask <<=  assembly map { asm =>
  val distdir = new File("""R:\target""")
  val remote = distdir / asm.getName
  println(s"Copying assembly into $remote")
  IO.copyFile(asm, remote)
  val srcZipFile = distdir / asm.getName.replace(".jar","-src.zip")
  def entries(f: File):Seq[File] = if (f.isDirectory) IO.listFiles(f).flatMap(entries) else Seq(f)
  val srcFolder = new File(".") / "src/main/scala"
  IO.zip(entries(srcFolder).map(d => (d, d.getAbsolutePath.substring(srcFolder.getAbsolutePath.length+1))), srcZipFile)
}

val restartTiger = inputKey[Unit]("restartTiger")
restartTiger := {
  scala.io.Source.fromURL(s"""http://analytics-etl:8090/shutdown?immediate=${
    spaceDelimited("<arg>").parsed.headOption match {
      case Some("immediate") => "true"
      case _ => "false"
    }
  }""").mkString
}

lazy val deployRegularAgent = inputKey[Unit]("deployRegularAgent")
deployRegularAgent := {
  val (options, args) = parseArgs(spaceDelimited("<arg>").parsed)
  val subfolderStr = "classes/analytics/tiger/agents"
  val destDir = file(options.getOrElse("destination", """J:\""")) / "agents-target" / subfolderStr
  if (!destDir.exists()) throw new RuntimeException(destDir.getPath + " does not exist.")
  val srcDir = file(".") / "agents/target/scala-2.11" / subfolderStr
  if (!srcDir.exists()) throw new RuntimeException(srcDir.getPath + " does not exist.")
  val files = srcDir.listFiles().filter(f =>
    (args.size==1 && args.head=="all") || args.exists(f.getName.startsWith)
  )
  if (files.isEmpty) throw new RuntimeException("No files found.")

  destDir.listFiles().filter(f =>
    (args.size==1 && args.head=="all") || args.exists(f.getName.startsWith)
  ).foreach(_.delete)

  files.foreach(file =>
    Files.copy(file.toPath, new File(destDir, file.name).toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
  )
  println(s"${files.size} files copied to ${destDir.getPath}")
}

lazy val deploySparkAgent = inputKey[Unit]("deploySparkAgent")
deploySparkAgent := {
  import complete.DefaultParsers._
  val (options, args) = parseArgs(spaceDelimited("<arg>").parsed)
  val subfolderStr = "classes/analytics/tiger/agents/spark"
  val destDir = file(options.getOrElse("destination", """R:\""")) / "spark-agents-target" / subfolderStr
  if (!destDir.exists()) throw new RuntimeException(destDir.getPath + " does not exist.")
  val srcDir = file(".") / "spark-agents/target/scala-2.11" / subfolderStr
  if (!srcDir.exists()) throw new RuntimeException(srcDir.getPath + " does not exist.")
  val files = srcDir.listFiles().filter(f =>
    (args.size==1 && args.head=="all") || args.exists(f.getName.startsWith)
  )
  if (files.isEmpty) throw new RuntimeException("No files found.")
  files.foreach(file =>
    Files.copy(file.toPath, new File(destDir, file.name).toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
  )
  println(s"${files.size} files copied to ${destDir.getPath}")
}

