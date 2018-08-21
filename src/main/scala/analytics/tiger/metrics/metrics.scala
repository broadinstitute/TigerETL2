package analytics.tiger.metrics

import java.sql.{CallableStatement, SQLException, Timestamp}

import analytics.tiger._
import analytics.tiger.api.Boot
import analytics.tiger.metrics.metricType.Ignore
import analytics.tiger.metrics.metricUtils.{Extractor, runItem}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalikejdbc._

import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

/**
 * Created by atanas on 3/16/2015.
 */

trait mTask {
  def run(run: runItem)(implicit session: DBSession): Either[etlMessage, Any]
}

case class importTask(
  tableName: String,
  getData: runItem => (List[String], Stream[Array[String]]),
  mTypes: Seq[metricType.EnumVal]
) extends mTask {
  val extractor = (s: Array[String]) =>
    for (i <- 0.until(mTypes.size) if (mTypes(i) != Ignore) ) yield Try { mTypes(i) match {
      case metricType.String => s(i)
      case metricType.Long => s(i).toLong
      case metricType.Boolean => s(i).toBoolean
      case metricType.Double if (!s(i).toDouble.isNaN) => s(i).toDouble
      case metricType.Timestamp(formatter) => new Timestamp(DateTime.parse(s(i), DateTimeFormat.forPattern(formatter)).getMillis)
      case metricType.TrimmedString(size) => s(i).substring(0, scala.math.min(size, s.size))
    }
  }.toOption

  def run(run: runItem)(implicit session: DBSession) = try {
    val (files, data) = getData(run)
    if (data.isEmpty) Right(etlMessage(s"\n$tableName (Data not found)"))
    else {
      SQL(s"DELETE FROM $tableName WHERE run_name=?").bind(run.runName).update().apply()
      val sql = SQL(s"""INSERT INTO $tableName VALUES(${0.to(mTypes.filterNot(_==Ignore).size).map(_ => "?").mkString(",")})""")
      sql.batch(data.map { Some(run.runName) :: extractor(_).toList }: _*).apply()
      Right(s"\n$tableName (${data.size} records)\n${files.mkString("\n")}")
    }
  } catch {
    case e: Exception => Left(etlMessage(s"\n$tableName => ${e.getMessage}"))
  }

}

case class dbTask(functionName: String) extends mTask {

  def run(run: runItem)(implicit session: DBSession) = {
    var stmt: CallableStatement = null
    try {
      stmt = session.connection.prepareCall(s"{CALL ? := $functionName(?,?)}")
      stmt.registerOutParameter(1, java.sql.Types.VARCHAR)
      stmt.setString(2, run.runName)
      stmt.registerOutParameter(3, java.sql.Types.NUMERIC)
      stmt.execute()
      Right(s"$functionName => ${stmt.getString(1)} (${stmt.getString(3)} records)")
    } catch {
      case e: SQLException => Left(etlMessage(s"$functionName => ${e.toString}"))
    } finally {
      if (stmt!=null) stmt.close()
    }
  }

}

case class sqlTask(name: String, sql: String) extends mTask {

  def run(run: runItem)(implicit session: DBSession) = {
    try {
      val res = SQL(sql).bind(run.runName).update().apply()
      Right(s"$name => ($res records)")
    } catch {
      case e: SQLException => Left(etlMessage(s"$name => ${e.toString}"))
    }
  }

}

object metricType {
  sealed trait EnumVal
  case object String extends EnumVal
  case object Long extends EnumVal
  case object Double extends EnumVal
  case object Boolean extends EnumVal
  // http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html
  case class Timestamp(formatter: String) extends EnumVal
  case class TrimmedString(size: Int) extends EnumVal
  case object Ignore extends EnumVal
}

object metricUtils {
  // https://intranet.broadinstitute.org/bits/service-catalog/compute-resources/file-systems
  //type runDiscreteDelta = discreteDelta[runItem]
  val rootFolder = scala.tools.nsc.io.File(utils.config.getString("seq_illumina_folder"))

  case class Extractor(columnName: String, func: Seq[String] => Try[Any])

  case class runItem(runName: String, instrument: String, cancelled: Boolean) {
    private val pattern = """(.*)_(.*)_(.*)_(.*)""".r
    val pattern(runDate, _, _, flowcell) = runName
  }

  def daysToOrganicRunConvertor(a: DaysDelta)(session: DBSession): DiscreteDelta[runItem] = {
    implicit val sess = session
    val (d1, d2) = a.unpack
    val res = sql"SELECT run_name, instrument FROM slxre2_organic_run a WHERE a.walk_up=1 AND IS_CANCELLED=0 AND a.run_end_date>=${d1} AND a.run_end_date<${d2}".map(it => runItem(it.string(1), it.string(2), false)).list.apply()
    DiscreteDelta(res.toSet)
  }

  def exploreMetrics(file: scala.reflect.io.File, columnSeparator: String = "\t", commentLines: Int = 0) {
    val iter = scala.io.Source.fromInputStream(file.inputStream()).getLines()
    1.to(commentLines).foreach(it => {iter.next() ; it })  // skip comments
    val columns = Seq("RUN_NAME") ++ iter.next().split(columnSeparator)
    println(s"\nCREATE TABLE <TABLE_NAME>${columns.map("\"" + _ + "\" VARCHAR2(100 byte)").mkString("(\n",",\n","\n)\n\n")}")
    println(s"Seq${columns.map("(\"" + _ + "\", metricType.String)").mkString("(\n",",\n","\n)\n\n")}")
  }

  def storedFunctionHandler(functionName: String) = (groupId: Seq[Extractor], session: DBSession) => {
    var stmt: CallableStatement = null
    try {
      stmt = session.connection.prepareCall(s"{CALL ? := $functionName(?)}")
      stmt.registerOutParameter(1, java.sql.Types.VARCHAR)
      groupId.zipWithIndex.foreach{ case (Extractor(_,func), ind) => stmt.setString(ind+2, func(Seq()) match {case Success(v: String) => v}) }
      stmt.execute()
      Right(s"$functionName => ${stmt.getString(1)}")
    } catch {
      case e: SQLException => Left(etlMessage(s"$functionName => ${e.toString}"))
    } finally {
      if (stmt!=null) stmt.close()
    }
  }

  val recordableValue  = (value: String, mt: metricType.EnumVal) => scala.util.Try {
    try mt match {
      case metricType.String    => value
      case metricType.TrimmedString(size) => value.substring(0, scala.math.min(size, value.size))
      case metricType.Long      => value.toLong
      case metricType.Double    => try { val r = value.toDouble ; if (r.isNaN) None else r} catch { case _ => None }
      case metricType.Boolean   => value.toBoolean.toString
      case metricType.Timestamp(formatter) => new Timestamp(DateTime.parse(value, DateTimeFormat.forPattern(formatter)).getMillis)
    } catch { case e: Throwable => throw new RuntimeException(s"Value: $value, ${e.getMessage}") }
  }

  def getExtractors(interestingColumns: Seq[(String, metricType.EnumVal)])(titles: Seq[String]) = {
    val columns = titles.zipWithIndex.toMap
    interestingColumns.map{case (title, metricType) => Extractor(
      title,
      line => recordableValue(line(columns(title)), metricType)
    )}
  }

  case class metricTask(
     metricName: String,
     metricTypes: Seq[(String, metricType.EnumVal)],
     commentLines: Int,
     columnSeparator: String,
     tableName: String,
     fileFilter: Regex,
     getFolder: (Seq[Extractor], DBSession) => Directory
   )

  def deleteStatement(tableName: String, groupId: Seq[Extractor]) =
    SQL(s"DELETE FROM $tableName WHERE ${
      groupId.map(it => s"${it.columnName}=?").mkString(" AND ")
    }").bind(groupId.map(_.func.apply(Seq()) match {case Success(run) => run }): _*).executeUpdate()

  def insertSQL(tableName: String, extractors: Seq[Extractor]) =
    s"INSERT INTO $tableName(\n${extractors.map("\""+_.columnName+"\"").mkString(",")})\nVALUES(${extractors.map(_ => "?").mkString(",")})"

  def taskToHandler(task: metricTask) = {
    (groupId: Seq[Extractor], session: DBSession) =>
      val folder = task.getFolder(groupId, session)
      if (!folder.exists) Right(etlMessage(s"${task.metricName}: Doesn't exist: $folder"))
      else {
        val fileIter = folder.files
        if (fileIter.isEmpty) Right(etlMessage(s"${task.metricName}: Files not found"))
        else {
          deleteStatement(task.tableName, groupId).apply()(session)
          val res = fileIter.foldLeft(List[Either[etlMessage, Any]]())((acc, file) =>
            task.fileFilter.findFirstIn(file.name) match {
              case None => acc
              case Some(x) => {
                val plan = IO {
                  val iter = scala.io.Source.fromInputStream(file.inputStream()).getLines()
                  1.to(task.commentLines).foreach(it => { iter.next(); it }) // skip comments
                  val extractors = groupId ++ getExtractors(task.metricTypes)(iter.next.split(task.columnSeparator))
                  val prep_stmt = SQL(insertSQL(task.tableName, extractors))
                  var count = 0
                  iter.filterNot(_ == "").foreach{ line =>
                    val fields = line.split(task.columnSeparator)
                    val params = extractors.map(_.func.apply(fields) match { case Success(v) => v case Failure(e) => None })
                    prep_stmt.bind(params: _*).executeUpdate().apply()(session)
                    count = count + 1
                  }
                  count
                }
                try Right(s"$x, lines processed: ${plan.run}")
                catch { case e: Exception => Left(etlMessage(e.getMessage)) }
              } :: acc
            }
          )
          ETL.aggregateResults(task.metricName, res.reverse)
        }
      }
  }

  implicit def toExtractors(extractors: Seq[Extractor]) = (_: Iterator[String], _:String) => extractors



  ///////////////

  case class Section(columns: Seq[String], data: Seq[Array[String]]) {
    def select(cols: Seq[String]) = {
      val extr = cols.map(columns.indexOf)
      if (extr.exists(_ == -1)) throw new RuntimeException(s"Unknown columns: ${
        extr.zip(cols).filter(_._1 == -1).map(_._2).mkString(",")
      }")
      Section(cols, data.map(line => extr.map(line.apply).toArray))
    }
  }

  def getReadNames(lines: List[String]) = lines.filter(it => it.startsWith("Read") && it.indexOf(",") == -1)

  def sectionExtractor(lines: List[String], whilePred: Array[String] => Boolean) = {
    val run_name = lines(1)
    (sectionName: String) => {
      val sectionStart = lines.dropWhile(_ != sectionName).drop(1)
      if (sectionStart.isEmpty) Section(Nil, Nil)
      else Section(
        "Run Name" :: "Section Name" :: sectionStart.head.split(",").map(_.trim).toList,
        sectionStart.tail.map(_.split(",").map(_.trim)).takeWhile(whilePred).map(it => (run_name :: sectionName :: it.toList).toArray)
      )
    }
  }

  def getSuperSection(srcFolder: String, sectionNames: String*) = {
    val sections = scala.reflect.io.File.apply(srcFolder).toDirectory.files.filter(_.path.endsWith("summary.csv")).flatMap { case it =>
      val extractor = sectionExtractor(scala.io.Source.fromFile(it.path).getLines().toList, it => scala.util.Try(it(0).toInt).isSuccess)
      sectionNames.map(extractor)
    }
    Section(sections.map(_.columns).toStream.headOption.getOrElse(Nil), sections.flatMap(_.data).toSeq)
  }

  //////////////////

  class metrics(lines: List[String]) {
    val run_name = if (lines.isEmpty) "" else lines(1)
    val readNames = lines.filter(it => it.startsWith("Read") && it.indexOf(",") == -1)

    def sectionExtractor(whilePred: Array[String] => Boolean)(sectionName: String) = {
      val sectionStart = lines.dropWhile(_ != sectionName).tail
      if (sectionStart.isEmpty) Section(Nil, Nil)
      else Section(
        "Section Name" :: sectionStart.head.split(",").map(_.trim).toList,
        sectionStart.tail.map(_.split(",").map(_.trim)).takeWhile(whilePred).map(it => (sectionName :: it.toList).toArray)
      )
    }

    val regularReads = readNames.filterNot(_.contains("(I)")).map(read => sectionExtractor(it => scala.util.Try(it(0).toInt).isSuccess)(read))

  }

  def sectionsUnion(sections: Seq[Section]) = Section(
    sections.map(_.columns).toStream.headOption.getOrElse(Nil),
    sections.flatMap(_.data)
  )

  def runToOrganicRunConvertor(runs: Seq[String], ignoreCancelled: Boolean = true)(session: DBSession): DiscreteDelta[runItem] = {
    implicit val sess = session
    val res = sql"SELECT run_name, instrument, IS_CANCELLED FROM slxre2_organic_run a WHERE a.run_name IN ($runs)".map{ it =>
      runItem(it.string(1), it.string(2), it.int(3) == 1)
    }.list.apply().filter(!ignoreCancelled || !_.cancelled)
    DiscreteDelta(res.toSet)
  }


}