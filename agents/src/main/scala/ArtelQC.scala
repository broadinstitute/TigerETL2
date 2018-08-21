package analytics.tiger.agents

import java.io.File
import java.sql.SQLException
import scalikejdbc._
import analytics.tiger.ETL._
import analytics.tiger._
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}
/**
  * Edited by biasella on 5/19/2017 .
  */
object ArtelQC {

  def extractVolumes(xml: Elem, nodeName: String) = xml \ nodeName \ "Rows" flatMap {
    case <Rows><Row>{name}</Row>{columns@_*}</Rows> => (columns map (node => node.text) zipWithIndex) map (it => (name.text , (it._2+1) , it._1.toDouble) )
  }

  val folder = scala.reflect.io.File.apply(new File(utils.config.getString("artel_folder")))

  def extractData(path: scala.reflect.io.Path) = {
    val xml = XML.load(path.toFile.inputStream())
    val metadata = try { (xml \ "Header3").head match {
      case <Header3>{fields@_*}</Header3> => fields map (f => f.label -> f.text) toMap
    }} catch {
      case e: Exception => throw new RuntimeException("Header3: " + e.getMessage)
    }
    val runstats = try { (xml \ "Run_Statistics").head match {
      case <Run_Statistics>{fields@_*}</Run_Statistics> => fields map (f => f.label -> f.text) toMap
    }} catch {
      case e: Exception => throw new RuntimeException("Run_Statistics: " + e.getMessage)
    }
    val mean_volumes = extractVolumes(xml, "Channel_Mean_Volumes")
    val inaccuracies = extractVolumes(xml, "Channel_Inaccuracies")
    val fileid = (xml \ "FileID").text
    val fname = path.name
    (xml, metadata, runstats, mean_volumes, inaccuracies, fileid, fname)
  }

  val ArtelAgent: etlType[MillisDelta] = delta => session => {
    implicit val sess = session
    val (d1, d2) = delta.unpack
    val toDoList = folder.toDirectory.list.filter { path =>
      path.path.endsWith(".xml") &&
      d1.getMillis <= path.lastModified && path.lastModified < d2.getMillis
    }.toList
    val res = toDoList map { path =>
      path.path ->
      Try(extractData(path)).map{ case (xml, metadata, runstats, mean_volumes, inaccuracies, fileid, fname) =>
        sql"DELETE FROM artel_well_qc WHERE FILE_ID=?".bind(fileid).executeUpdate().apply()
        mean_volumes zip inaccuracies foreach { case ((row, column, mean), (_, _, inaccuracy)) =>
          val params = Seq(fileid, metadata("Device_ID"), metadata("Date"), row, column, mean, inaccuracy, fname, runstats("Mean_Volume"), runstats("Inaccuracy"), runstats("CV"), runstats("Target_Volume"))
          try sql"INSERT INTO artel_well_qc VALUES(?,?,?,?,?,?,?,?,?,?,?,?)".bind(params: _*).executeUpdate().apply()
          catch {
            case e: Exception => throw new RuntimeException(e.getMessage + "\n" + params.mkString(", "))
          }
        }
      }
    }
    val failures = res.filter(_._2.isFailure)
    val successes = res.filter(_._2.isSuccess)
    val str = s"${successes.size} files succeeded: ${successes.map(_._1).mkString("\n")},\n${failures.size} files failed: ${failures.map{ case (path, Failure(e)) =>  s"$path -> ${e.getMessage}"}.mkString("\n")}"
    Seq((delta, if (failures.size == 0) Right(str) else Right(etlMessage(str))))
  }

  /*
  manually define delta

  val mydelta = MillisDelta("2016-jan-01 00:00:00","2016-jan-06 00:00:00")
  val etlPlan = prepareEtl("analytics.tiger.ArtelAgent", mydelta, ArtelAgent)(toCommit = false)
  val res = utils.CognosDB.apply(etlPlan)
  println(res)
  */

  def main(args: Array[String]) = {
    val agentName = utils.objectName(this)
    val etlPlan = MillisDelta.loadFromDb(agentName) flatMap(delta => prepareEtl(agentName, delta, ArtelAgent)())
    val res = utils.CognosDB.apply(etlPlan)
    println(res)
  }
}