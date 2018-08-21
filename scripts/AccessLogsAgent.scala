import analytics.tiger._
import java.io.File
import java.util.zip.GZIPInputStream
import analytics.tiger.ETL._
import org.joda.time.{Instant, DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat
import scalikejdbc._
import scala.io.Source

val extrDate = """(.*).(\d\d\d\d-\d\d-\d\d)(.*)?""".r
val dtformatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")

def LogsAgent(application: String, filePrefix: String, logsFolder: String, tableName: String, nColumns: Int, extractLine: String => List[Any]): etlType[MillisDelta] = delta => session => {
  implicit val sess = session
  val (d1, d2) = delta.unpack
  val toDoList = scala.reflect.io.File.apply(new File(logsFolder)).toDirectory.list.filter { path =>
    path.name.startsWith(filePrefix) && d1.getMillis <= path.lastModified && path.lastModified < d2.getMillis
  }.toList

  val stmt = SQL(s"INSERT INTO $tableName VALUES(${0.until(nColumns) map {_ => "?"} mkString(",")})")
  val items = toDoList map { path => {
    val extrDate(_,dateStr,_) = path.name
    val dd = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(dateStr)
    SQL(s"DELETE FROM $tableName WHERE application=? and timestamp>=? and timestamp<?").bind(application, dd, dd.plusDays(1)).executeUpdate().apply()
    val fis = path.toFile.inputStream()
    val item = Source.fromInputStream(if (path.name.endsWith(".gz")) new GZIPInputStream(fis) else fis).getLines().foldLeft((0,0,None:Option[String])) { case ((passed,failed,failure1),line) =>
      try {
        stmt.bind(extractLine(line): _*).executeUpdate().apply()
        (passed+1, failed, failure1)
      } catch {
        case e: Exception => (passed, failed+1, failure1 match { case Some(f) => Some(f) case None => Some(e.getMessage + "\n" + line)})
      }
    }
    (path.name, item._1, item._2, item._3)
  }}
  val res = items.map(it => s"${it._1} => lines processed: ${it._2}, lines failed: ${it._3}, 1st failure: ${it._4}").mkString("\n")
  Seq((delta, Right(if (items.exists(_._3>0)) etlMessage(res) else res)))
}

val extr = """(.*)\s(.*)\s(.*)\s\x5B(.*)\x5D\s\x22(.*)\x22\s(\d*)\s([\d|-]*)(\s(.*))?""".r
val extractLine = (line: String) => line match {
  case extr(remoteIP, _, remoteUser, timestamp, requestedURL, responseCode, bytesSent, remainder, _) =>
    List(remoteIP, remoteUser, dtformatter.parseDateTime(timestamp), requestedURL.substring(0, math.min(requestedURL.size, 4000)), responseCode.toInt, try bytesSent.toInt catch {case _ => None}, try remainder.substring(0, math.min(remainder.size, 4000)) catch {case _ => None})
}

def accessExtractLine(application: String)(line: String) = application :: extractLine(line)

val application = System.getProperty("application")
val cnf = utils.config.getConfig(s"accessLogs.$application")
val (agentName, filePrefix, logsFolder) = (cnf.getString("agentName"), cnf.getString("filePrefix"), cnf.getString("logsFolder"))
val lineExtractor = application match {
  case "squid" => (line: String) =>
  {
    val res = accessExtractLine(application)(line)
    (res take 3) ++ List({
      val d = res(3).asInstanceOf[DateTime]
      val isDaylightOfset = !DateTimeZone.getDefault.isStandardOffset(d.getMillis)
      if (isDaylightOfset) d.minusHours(1) else d
    }) ++ (res drop 4)
  }
  case _ => accessExtractLine(application) _
}
val etlPlan = MillisDelta.loadFromDb(agentName) map{ d =>
  val (d1, d2) = d.unpack
  //val start = d1.withMillisOfDay(0)
  val end = d2.withMillisOfDay(0).plusDays(1)
  new MillisDelta(d1, end.getMillis-d1.getMillis) { override val persist = d.persist }
} flatMap (prepareEtl(agentName, _, LogsAgent(application, filePrefix, logsFolder, "access_log", 8, lineExtractor))())
val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)