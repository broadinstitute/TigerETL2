import analytics.tiger._
import java.io.{FileInputStream, File}
import java.util.zip.GZIPInputStream
import analytics.tiger.ETL._
import org.joda.time.format.DateTimeFormat
import scalikejdbc._
import scala.io.Source

val extrDate = """access_log.(\d\d\d\d-\d\d-\d\d)(.*)?""".r
val dtformatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")

def LogsAgent(filePrefix: String, logsFolder: String, tableName: String, nColumns: Int, extractLine: String => Array[Any]): etlType[MillisDelta] = delta => session => {
  implicit val sess = session
  val (d1, d2) = delta.unpack
  val toDoList = scala.reflect.io.File.apply(new File(logsFolder)).toDirectory.list.filter { path =>
    path.name.startsWith(filePrefix) && d1.getMillis <= path.lastModified && path.lastModified < d2.getMillis
  }.toList

  var count=0
  val stmt = SQL(s"INSERT INTO $tableName VALUES(${0.until(nColumns) map {_ => "?"} mkString(",")})")
  toDoList foreach { path => {
    val extrDate(dateStr,_) = path.name
    val dd = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(dateStr)
    SQL(s"DELETE FROM $tableName WHERE timestamp>=? and timestamp<?").bind(dd, dd.plusDays(1)).executeUpdate().apply()
    val fis = path.toFile.inputStream()
    Source.fromInputStream(if (path.name.endsWith(".gz")) new GZIPInputStream(fis) else fis).getLines() foreach { line =>
      try {
        stmt.bind(extractLine(line): _*).executeUpdate().apply()
        count = count + 1
      } catch {
        case e: Exception => throw new Exception(e.getMessage + "\nfilename=" + path.name + "\n" + line)
    }}
  }}
  Seq((delta, Right(s"${toDoList.size} files processed: ${toDoList.map(_.name).mkString(",")} ; lines:$count")))
}

val extractLine = (line: String) => {
  val extr = """(.*)\s(.*)\s(.*)\s\x5B(.*)\x5D\s\x22(.*)\x22\s(.*)\s(.*)""".r
  val extr(remoteIP, _, remoteUser, timestamp, requestedURL, responseCode, bytesSent) = line
  Array(remoteIP, remoteUser, dtformatter.parseDateTime(timestamp), requestedURL.substring(0, math.min(requestedURL.size, 4000)), responseCode.toInt, try bytesSent.toInt catch {case _ => None})
}

val agentName = "analytics.tiger.MercuryLogsAgent"
val etlPlan = MillisDelta.loadFromDb(agentName) flatMap (prepareEtl(agentName, _, LogsAgent("access_log.", utils.config.getString("mercuryLogsFolder"), "mercury_access_log", 6, extractLine))())
val res = utils.CognosDB.apply(etlPlan)
println(res)
