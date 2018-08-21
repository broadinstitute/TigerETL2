package analytics.tiger

import java.io.{File, FileInputStream}
import java.net.{URL, URLEncoder}
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPInputStream

import analytics.tiger.ETL._
import analytics.tiger.metrics.metricType.Timestamp
import cats.Monad
import cats.data.Reader
import org.joda.time.{DateTime, Days, Interval, Minutes}
import org.joda.time.format.DateTimeFormat
import scalikejdbc.{DBSession, SQL}
import scalikejdbc._

import scala.io.Source
import scala.reflect.io.Path
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, Node, XML}

/**
 * Created by atanas on 7/14/2015.
 */

object MercuryLogger {

  import analytics.tiger._
  import java.io.{FileInputStream, File}
  import java.util.zip.GZIPInputStream
  import analytics.tiger.ETL._
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

    var count=0
    val stmt = SQL(s"INSERT INTO $tableName VALUES(${0.until(nColumns) map {_ => "?"} mkString(",")})")
    toDoList foreach { path => {
      val extrDate(_,dateStr,_) = path.name
      val dd = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(dateStr)
      SQL(s"DELETE FROM $tableName WHERE application=? and timestamp>=? and timestamp<?").bind(application, dd, dd.plusDays(1)).executeUpdate().apply()
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
    List(remoteIP, remoteUser, dtformatter.parseDateTime(timestamp), requestedURL.substring(0, math.min(requestedURL.size, 4000)), responseCode.toInt, try bytesSent.toInt catch {case _ => None})
  }

  def accessExtractLine(application: String)(line: String) = application :: extractLine(line)

  def main(args: scala.Array[String]): Unit = {
    val application = "squid" // System.getProperty("application")
    val cnf = utils.config.getConfig(s"accessLogs.$application")
    val (agentName, filePrefix, logsFolder) = (cnf.getString("agentName"), cnf.getString("filePrefix"), cnf.getString("logsFolder"))
    val etlPlan = MillisDelta.loadFromDb(agentName) flatMap (prepareEtl(agentName, _, LogsAgent(application, filePrefix, logsFolder, "access_log", 7, accessExtractLine(application)))())
    val res = utils.CognosDB.apply(etlPlan)
    println(res)
/*
    val agentName = "analytics.tiger.MercuryLogsAgentTEST"
    val etlPlan = prepareEtl(agentName, MillisDelta("2016-jan-20","2016-jun-21"), LogsAgent("access_log.", utils.config.getString("mercuryLogsFolder"), "mercury_access_log", 6, extractLine))(toCommit = false)
    val res = utils.CognosDB.apply(etlPlan)
    println(res)
*/
  }

}


object Workshop {

  type jobsTimes = (DateTime, Int, DateTime) => Seq[DateTime]

  def getTimes(etlAnchor: DateTime, etlWindow: Int)(schAnchor: DateTime, schWindow: Int, moment: DateTime) = {
    def point(anchor: DateTime, window: Int, moment: DateTime, f: Double => Double) = anchor.plusMinutes(f(Minutes.minutesBetween(anchor, moment).getMinutes.toDouble / window).toInt*window)
    val from  = point(etlAnchor, etlWindow, moment, scala.math.ceil)
    val right = point(schAnchor, schWindow, moment, scala.math.ceil)
    val to    = point(etlAnchor, etlWindow, right , d => if (d.isWhole()) d-1 else d.floor)
    0.to(Minutes.minutesBetween(from, to).getMinutes/etlWindow) map (it => from.plusMinutes(it*etlWindow))
  }

  case class etlJob(agent_name: String, times: jobsTimes)


  val todo = Seq( () => Right("OK"), () => Left(""))

  def run(td: Seq[() => Either[String,String]]): List[Either[String,String]] = {
    td match {
      case h :: t => h() match {
        case Right(str) => Right(str) :: run(t)
        case Left(str) => List(Left(str))
      }
      case Nil => List()
    }
  }

  def main(args: scala.Array[String]): Unit = {
    println("Hello AGAIN")

    import analytics.tiger.ETL._
    import analytics.tiger._
    import org.joda.time.DateTime

    val agentName = "analytics.tiger.PdoStar5Agent"

    def pdoPushLeft(cond: => Boolean, size: Long = 365L)(delta: MillisDelta) = {
      if (cond) new MillisDelta(delta.start.minus(size*24*60*60*1000), delta.size + size*24*60*60*1000)
      else delta
    }

    val argsMap = utils.argsToMap(args)

    val etlPlan = for (
      delta <- MillisDelta.loadFromDb(agentName) map pdoPushLeft(
        argsMap.get("1y-refresh").getOrElse("FALSE").toBoolean ||
          (DateTime.now.dayOfWeek().getAsText == "Friday" && DateTime.now.hourOfDay().get < 2)
      );
      plan <- prepareEtl(agentName, delta, sqlScript.etl(relativeFile("resources/pdo_star_etl.sql")))()
    ) yield plan
    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    println(res)


    //MercuryLogger.main(null)


  }

}

object experiments {
  val rt = "радиотеатър"
  def unfold[A, S](z: S)(f: S => Option[(A, S)]): Seq[A] =
    f(z) match {
      case Some((h,s)) => unfold(s)(f).+:(h)
      case None => Seq()
    }

  val testDaysEtlAgent: ETL.etlType[DaysDelta] = delta => session =>
    //  Left(etlError("Test Error2"))
    try {
      implicit val sess = session
      val (d1, d2) = delta.unpack
      if (delta.contains(DaysDelta(ETL.daysFormatter.parseDateTime("2015-Mar-3"),5))) Seq((delta, Left(new etlMessage("TEST ERROR"))))
      else {
        val res = sql"select * from etl_runs WHERE start_time>=${d1} and start_time<${d2}".map(_.string("agent_name")).list.apply()
        //Thread.sleep(5000)
        Seq((delta, Right("OK")))
      }
    } catch {
      case e: Exception => Seq((delta, Left(etlMessage(e.getMessage))))
    }

  case class rlbItem(run: String, lane: String, barcode: String)

  type rlbDiscreteDelta = DiscreteDelta[rlbItem]
  //type fcDiscreteDelta = discreteDelta[String]

  val testRlbEtlAgent: ETL.etlType[rlbDiscreteDelta] = delta => session =>
    //  Left(etlError("Test Error"))
    try {
      implicit val sess = session
      if (delta.contains(new rlbDiscreteDelta(Set(rlbItem("H2JLGCCXX150316","4",null))))) Seq((delta, Left(new etlMessage("TEST RLB ERROR"))))
      else {
        //val res = sql"select * from etl_runs WHERE start_time>=${d1} and start_time<${d2}".map(_.string("agent_name")).list.apply()
        //Thread.sleep(5000)
        Seq((delta, Right("OK")))
      }
    } catch {
      case e: Exception => Seq((delta, Left(etlMessage(e.getMessage))))
    }

  def RghqsFcConvertor(dummy: Delta, session: DBSession)  = {
    implicit val sess = session
    val res = sql"SELECT flowcell_barcode FROM slxre_rghqs_targets WHERE rgmd_timestamp IS NOT NULL AND rghqs_timestamp IS NULL".map(_.string(1)).list.apply()
    new DiscreteDelta(res.toSet)
  }

  val RghqsLoaderAgent: ETL.etlType[DiscreteDelta[String]] = delta => session => {
    SQL(s"BEGIN solexa_2.RGHQS_RELOAD(seq20.varray100varchar100(${delta.elements.map("'" + _ + "'").mkString(",")})); END;").execute().apply()(session)
    Seq((delta, Right("OK")))
  }


  val testMillisEtlAgent: ETL.etlType[MillisDelta] = delta => session =>
    //  Left(etlError("Test Error"))
    try {
      //throw new RuntimeException("TEST")
      implicit val sess = session
      val (d1, d2) = delta.unpack
      val res = sql"select * from etl_runs WHERE start_time>=${d1} and start_time<${d2}".map(_.string("agent_name")).list.apply()
      Seq((delta, Right("OK")))
    } catch {
      case e: Exception => Seq((delta, Left(etlMessage(e.getMessage))))
    }

  def rlbElem(a: DaysDelta, session: DBSession): rlbDiscreteDelta = {
    implicit val sess = session
    val (d1, d2) = a.unpack
    val res = sql"SELECT DISTINCT a.run_barcode,a.lane,'' molecular_indexing_scheme FROM slxre_readgroup_metadata a WHERE a.run_date>=${d1} AND a.run_date<${d2}".map(it => new rlbItem(it.string("run_barcode"), it.string("lane"), it.string("molecular_indexing_scheme"))).list.apply().distinct
    new DiscreteDelta(res.toSet)
  }

  trait Functor[A, F[_]] {
    def map[B](f: A => B): F[B]
  }
  class Fn1Functor[A, B](g: A => B) extends Functor[B, ({type f[?] = A => ?})#f] {
    override def map[C](f: B => C): (A => C) = a => f(g(a))
  }

  sealed trait Maybe[+A] {
    // >>=
    def flatMap[B](f: A => Maybe[B]): Maybe[B] = this match {
      case Just(a) => f(a)
      case MaybeNot => MaybeNot
    }
  }

  case class Just[+A](a: A) extends Maybe[A]
  case object MaybeNot extends Maybe[Nothing]

}

object deltaMonadOps {

  trait Delta {
    def size: Long
  }

  case class MillisDelta(start: DateTime, size: Long) extends Delta

  case class DaysDelta(start: DateTime, size: Long) extends Delta

  trait DeltaHandler[A <: Delta] {
    def handle(value: A): Delta
    def chunkMillis(delta: A, chunkSize: Long): Seq[A]
  }


  implicit val millisHandler = new DeltaHandler[MillisDelta] {

    def handle(value: MillisDelta): Delta = value

    def chunkMillis(delta: MillisDelta, chunkSize: Long): Seq[MillisDelta] = {
      0L.until(math.ceil(delta.size.toDouble/chunkSize).toLong).map(it => MillisDelta(delta.start.plus(it*chunkSize), math.min(chunkSize, delta.size-it*chunkSize)))
    }
  }

  implicit val daysHandler = new DeltaHandler[DaysDelta] {

    def handle(value: DaysDelta): Delta = value

    def chunkMillis(delta: DaysDelta, chunkSize: Long): Seq[DaysDelta] = {
      0.until(math.ceil(delta.size.toDouble/chunkSize).toInt).map(it => DaysDelta(delta.start.plusDays(it*chunkSize.toInt), math.min(chunkSize, delta.size-it*chunkSize)))
    }
  }


//  object Delta {
//    def toDelta[A <: Delta](value: A)(implicit handler: DeltaHandler[A]) = handler.handle(value)
//  }

}
