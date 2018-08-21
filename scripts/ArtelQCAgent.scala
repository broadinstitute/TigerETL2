import analytics.tiger.ETL._
import analytics.tiger._
import scalikejdbc._
import java.io.File
import scala.xml.{Elem, XML}

def extractVolumes(xml: Elem, nodeName: String) = xml \ nodeName \ "Rows" flatMap {
  case <Rows><Row>{name}</Row>{columns@_*}</Rows> => (columns map (node => node.text) zipWithIndex) map (it => (name.text , (it._2+1) , it._1.toDouble) )
}

val folder = scala.reflect.io.File.apply(new File(utils.config.getString("artel_folder")))

// def ArtelAgent(delta, session)

val ArtelAgent: etlType[MillisDelta] = delta => session => {
  implicit val sess = session
  val (d1, d2) = delta.unpack
  val toDoList = folder.toDirectory.list.filter { path =>
    d1.getMillis <= path.lastModified && path.lastModified < d2.getMillis
  }.toList
  toDoList foreach { path =>
    val xml = XML.load(path.toFile.inputStream())
    val metadata = (xml \ "Header3").head match {
      case <Header3>{fields@_*}</Header3> => fields map (f => f.label -> f.text) toMap
    }
    val mean_volumes = extractVolumes(xml, "Channel_Mean_Volumes")
    val inaccuracies = extractVolumes(xml, "Channel_Inaccuracies")
    val FileID = (xml \ "FileID").text
    sql"DELETE FROM artel_well_qc WHERE FILE_ID=?".bind(FileID).executeUpdate().apply()
    mean_volumes zip inaccuracies foreach { case ((row, column, mean), (_, _, inaccuracy)) =>
      sql"INSERT INTO artel_well_qc VALUES(?,?,?,?,?,?,?)".bind(FileID, metadata("Device_ID"), metadata("Date"), row, column, mean, inaccuracy).executeUpdate().apply()
    }
  }

  Seq((delta, Right(s"${toDoList.size} files processed")))
}

// (toCommit = false) will not apply changes to DB

val etlPlan = MillisDelta.loadFromDb("analytics.tiger.ArtelAgent") flatMap(delta => prepareEtl("analytics.tiger.ArtelAgent", delta, ArtelAgent)())
val res = utils.CognosDB.apply(etlPlan)
println(res)

/*
-manually define delta
val mydelta = MillisDelta("2016-jan-01 00:00:00","2016-jan-06 00:00:00")

val etlPlan = prepareEtl("analytics.tiger.ArtelAgent", mydelta, ArtelAgent)
val res = utils.CognosDB.apply(etlPlan)
println(res)
*/