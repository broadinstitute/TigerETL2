package analytics.tiger

import java.sql._
import analytics.tiger.api.Boot
import oracle.jdbc.{OraclePreparedStatement, OracleConnection}
import org.joda.time.format.{DateTimeFormat}
import org.joda.time.{Days, DateTime}
import scalikejdbc._
import scala.util.{Failure, Success, Try}

/**
 * Created with IntelliJ IDEA.
 * User: atanas
 * Date: 10/28/14
 * Time: 3:39 PM
 * To change this template use File | Settings | File Templates.
 */

case class etlMessage(val msg: String, val recipients: Seq[String] = ETL.errorRecipients)

trait Delta {
  val size: Long
  val persist: DBSession => Int = _ => 0
  def sqlSubs: Map[String, String] = ???
}

case class MillisDelta(start: DateTime, size: Long) extends Delta {

  def contains(that: MillisDelta) = {
    val (d1,d2) = unpack
    val (that1,that2) = that.unpack
    d1.getMillis <= that1.getMillis && that2.getMillis <= d2.getMillis
  }

  def unpack = (start, start.plus(size))
  override def toString = s"[${ETL.millisFormatter.print(start)}, ${ETL.millisFormatter.print(start.plus(size))})"

  override def sqlSubs = {
    val (d1, d2) = unpack
    Map(
      "/*DELTA_START*/" -> s"TO_DATE('${ETL.millisFormatter.print(d1)}','YYYY-MON-DD HH24:MI:SS')",
      "/*DELTA_END*/"   -> s"TO_DATE('${ETL.millisFormatter.print(d2)}','YYYY-MON-DD HH24:MI:SS')"
    )}

}

object MillisDelta {
  def parse(s:String) = Try(ETL.millisFormatter.parseDateTime(s)) recover { case _ => ETL.daysFormatter.parseDateTime(s)}
  def apply(d1: DateTime, d2: DateTime): MillisDelta = MillisDelta(d1, d2.getMillis-d1.getMillis)
  def apply(d1: String, d2: String): MillisDelta = {
    (for (
      m1 <- parse(d1);
      m2 <- parse(d2)
    ) yield apply(m1, m2)) get
  }

  def loadFromDb(agentName: String, cap: Long = Long.MaxValue, propLink: String = "", dbSysdate: DBSession => DateTime = ETL.getDbSysdate()): Reader[DBSession, MillisDelta] =
    Reader(session => {
      val pointer = parse(ETL.getDeltaPointerStr(agentName, propLink)(session)).get
      val size = math.min(dbSysdate(session).getMillis - pointer.getMillis, cap)
      new MillisDelta(pointer, size) {
        override val persist = (session: DBSession) => ETL.persistDB(ETL.millisFormatter.print(pointer.plus(size)), agentName, propLink).get.apply()(session)
      }
    })

  def loadFromDbBackwards(agentName: String, cap: Long, propLink: String = ""): Reader[DBSession, MillisDelta] =
    Reader(session => {
      val pointer = parse(ETL.getDeltaPointerStr(agentName, propLink)(session)).get
      val start = pointer.minus(cap)
      new MillisDelta(start, cap) {
        override val persist = (session: DBSession) => ETL.persistDB(ETL.millisFormatter.print(start), agentName, propLink).get.apply()(session)
      }
    })

  def pushLeft(n: Long) = (d: MillisDelta) => new MillisDelta(d.start.minus(n), d.size+n) { override val persist = d.persist }

}

class DaysDelta(val start: DateTime, val size: Long) extends Delta {
  if (size<0) throw new IllegalArgumentException("DaysDelta size can't be negative.")

  def contains(that: DaysDelta) = {
    val (d1,d2) = unpack
    val (that1,that2) = that.unpack
    d1.getMillis <= that1.getMillis && that2.getMillis <= d2.getMillis
  }
  def unpack = (start, start.plusDays(size.toInt))
  override def toString = s"[${ETL.daysFormatter.print(start)}, ${ETL.daysFormatter.print(start.plusDays(size.toInt))})"
}

object DaysDelta {
  def apply(start: DateTime, size: Long): DaysDelta = new DaysDelta(start.withTimeAtStartOfDay, size)

  def apply(d1: DateTime, d2: DateTime): DaysDelta = {
    val v1 = d1.withTimeAtStartOfDay
    val v2 = d2.withTimeAtStartOfDay
    apply(v1, Days.daysBetween(v1, v2).getDays)
  }
  def apply(d1: String, d2: String): DaysDelta = apply(ETL.daysFormatter.parseDateTime(d1), ETL.daysFormatter.parseDateTime((d2)))

  def loadFromDb(agentName: String, cap: Int = Int.MaxValue, propLink: String = "", dbSysdate: DBSession => DateTime = ETL.getDbSysdate()): Reader[DBSession, DaysDelta] =
    Reader(session => {
      val pointer = ETL.daysFormatter.parseDateTime(ETL.getDeltaPointerStr(agentName, propLink)(session))
      val size0 = math.min(org.joda.time.Days.daysBetween(pointer, dbSysdate(session)).getDays, cap)
      new DaysDelta(pointer, size0) {
        override val persist = (session: DBSession) => ETL.persistDB(ETL.daysFormatter.print(pointer.plusDays(size0)), agentName, propLink).get.apply()(session)
      }
    })

  def loadFromDbBackwards(agentName: String, cap: Int, propLink: String = ""): Reader[DBSession, DaysDelta] =
    Reader(session => {
      val pointer = ETL.daysFormatter.parseDateTime(ETL.getDeltaPointerStr(agentName, propLink)(session))
      val start = pointer.minusDays(cap)
      new DaysDelta(start, cap) {
        override val persist = (session: DBSession) => ETL.persistDB(ETL.daysFormatter.print(start), agentName, propLink).get.apply()(session)
      }
    })

  def pushLeft(n: Int ) = (d: DaysDelta) => new DaysDelta(d.start.minusDays(n), d.size+n) { override val persist = d.persist }

}

case class DiscreteDelta[A](elements: Set[A]) extends Delta {
  val size: Long = elements.size
  def contains(that: DiscreteDelta[A]) = that.elements.forall(elements.contains(_))
  def unpack = elements
  override def sqlSubs = Map("/*DELTA*/" -> elements.map(it => s"'$it'").mkString(","))

  override def toString = {
    if (elements.size <= 50) elements.toString()
    else s"Set(${elements.take(50).mkString("\n")} ...)"
  }

  def binder = elements match {
    case el: Set[String] =>
      new ParameterBinder[Set[String]] {
        override val value = Set[String]()
        override def apply(stmt: PreparedStatement, idx: Int): Unit = {
          val ops = stmt.asInstanceOf[OraclePreparedStatement]
          val oconn = ops.getConnection.asInstanceOf[OracleConnection]
          val arr = oconn.createARRAY("COGNOS.VARCHAR2_TABLE", elements.asInstanceOf[Set[String]].toArray)
          ops.setARRAY(idx, arr)
      }}
    case _ => throw new RuntimeException("Unsuported parameterBinder in discreteDelta.")
  }

  def generateXML = elements.map { elem =>
      elem match {
        case s: String => s"""<item value=\"$s\"/>"""
        case _ =>
          val clazz = elem.getClass
          clazz.getDeclaredFields.map { it =>
            val fieldName = it.getName
            s"""$fieldName=\"${clazz.getDeclaredMethod(fieldName).invoke(elem)}\""""
          }.mkString("<item ", " ", "/>")
      }
    }.mkString("<delta>", "", "</delta>")

}

/*
trait Functor[F[_]] {
  def map[A, B](fa: F[A])(f: A => B): F[B]
}

trait Monad[F[_]] extends Functor[F] {
  def unit[A](a: => A): F[A]
  def flatMap[A, B](ma: F[A])(f: A => F[B]): F[B]
  def map[A, B](fa: F[A])(f: A => B): F[B] = flatMap(fa)(a => unit(f(a)))
}
*/
sealed trait IO[A] { self =>
  def run: A
  def map[B](f: A => B) = flatMap(a => IO(f(a)))
  def flatMap[B](f: A => IO[B]): IO[B] = new IO[B] { def run = f(self.run).run }
}

object IO {
  def apply[A](a: => A): IO[A] = new IO[A] { def run = a }
}

case class Reader[C, A](g: C => A) {
  def apply(c: C) = g(c)
  def map[B](f: A => B): Reader[C, B] = flatMap(a => Reader(c => f(a)) )  // Reader(c => f(g(c)))
  def flatMap[B](f: A => Reader[C, B]): Reader[C, B] = Reader(c => f(g(c))(c))
}

object ETL {

  val daysFormatter   = DateTimeFormat.forPattern("yyyy-MMM-dd")
  val millisFormatter = DateTimeFormat.forPattern("yyyy-MMM-dd HH:mm:ss")

  def persistDB(pointer: String, agentName: String, propLink: String) =
    Some(SQL(s"UPDATE COGNOS.etl_property$propLink SET value=? WHERE key=?").bind(pointer, agentName).update())
/*
  implicit def chunkMillis(delta: MillisDelta, chunkSize: Long): Seq[MillisDelta] =
    0L.until(math.ceil(delta.size.toDouble/chunkSize).toLong).map(it => MillisDelta(delta.start.plus(it*chunkSize), math.min(chunkSize, delta.size-it*chunkSize)))

  implicit def chunkDays(delta: DaysDelta, chunkSize: Long): Seq[DaysDelta] =
    0.until(math.ceil(delta.size.toDouble/chunkSize).toInt).map(it => DaysDelta(delta.start.plusDays(it*chunkSize.toInt), math.min(chunkSize, delta.size-it*chunkSize)))

  implicit def chunkDiscrete[A](delta: DiscreteDelta[A], chunkSize: Long): Seq[DiscreteDelta[A]] =
    delta.elements.grouped(chunkSize.toInt).map(DiscreteDelta(_)).toSeq

  implicit def chunkDummy(delta: DummyDelta, chunkSize: Long): Seq[DummyDelta] = Seq(delta)
*/
  def tupleToSeq[A <: Delta](t: (A,A)) = Seq(t._1, t._2).filter(_.size>0)

  implicit def splitMillis(delta: MillisDelta): Seq[MillisDelta] =
    if (delta.size==1) Seq(delta)
    else {
      val median = delta.size / 2
      Seq(MillisDelta(delta.start, median), MillisDelta(delta.start.plus(median), delta.size - median))
    }

  implicit def splitDays(delta: DaysDelta): Seq[DaysDelta] =
    if (delta.size==1) Seq(delta)
    else {
      val median = delta.size / 2
      Seq(DaysDelta(delta.start, median), DaysDelta(delta.start.plusDays(median.toInt), delta.size - median))
    }

  def splitDiscrete[A](delta: DiscreteDelta[A]): Seq[DiscreteDelta[A]] =
    if (delta.size==1) Seq(delta)
    else {
      val (s1, s2) = delta.elements.splitAt(delta.elements.size / 2)
      Seq(DiscreteDelta(s1), DiscreteDelta(s2))
    }


  trait DeltaHandler[A <: Delta] {
    def chunk(delta: A, chunkSize: Long): Seq[A]
  }

  implicit val millisHandler = new DeltaHandler[MillisDelta] {
    def chunk(delta: MillisDelta, chunkSize: Long): Seq[MillisDelta] = {
      0L.until(math.ceil(delta.size.toDouble/chunkSize).toLong).map(it => MillisDelta(delta.start.plus(it*chunkSize), math.min(chunkSize, delta.size-it*chunkSize)))
    }
  }

  implicit val daysHandler = new DeltaHandler[DaysDelta] {
    def chunk(delta: DaysDelta, chunkSize: Long): Seq[DaysDelta] = {
      0.until(math.ceil(delta.size.toDouble/chunkSize).toInt).map(it => DaysDelta(delta.start.plusDays(it*chunkSize.toInt), math.min(chunkSize, delta.size-it*chunkSize)))
    }
  }

  implicit def discreteHandler[A] = new DeltaHandler[DiscreteDelta[A]] {
    def chunk(delta: DiscreteDelta[A], chunkSize: Long): Seq[DiscreteDelta[A]] = {
      if (chunkSize == Long.MaxValue) Seq(delta)
      else delta.elements.grouped(chunkSize.toInt).map(DiscreteDelta.apply).toSeq
    }
  }

  trait DummyDelta extends Delta

  val dummyDelta = new DummyDelta {
    override def toString() = "dummyDelta"
    val size: Long = 1
    //override def persist: Option[SQLUpdate] = None
    def contains(a: Delta): Boolean = false
  }

  implicit val dummyHandler = new DeltaHandler[DummyDelta] {
    def chunk(delta: DummyDelta, chunkSize: Long): Seq[DummyDelta] = Seq(delta)
  }

  lazy val errorRecipients = utils.config.getString("errorRecipients").split(",").toSeq

  def getDeltaPointerStr(agent_name: String, propLink: String)(implicit session: DBSession) =
    try {
      val pointer = SQL(s"""
        SELECT CASE WHEN semaphor='STOP' THEN 'STOP' ELSE pointer end
        FROM
        (SELECT
            MAX(CASE WHEN key ='SEMAPHOR' then value ELSE NULL end) semaphor,
            MAX(CASE WHEN key!='SEMAPHOR' then value ELSE NULL end) pointer
        FROM COGNOS.etl_property$propLink
        WHERE key='$agent_name' OR key='SEMAPHOR'
        )""").map(_.string(1)).single().apply().get
      pointer match {
        case ""     => throw new RuntimeException(s"Agent $agent_name not found in etl_property.")
        case "STOP" => throw new RuntimeException("STOP semaphor is engaged.")
        case _      => pointer
      }
    } catch {
      case e: Exception => throw new Exception(s"Problem found in 'etl_property', ${e.getMessage}")
    }

  def getDbSysdate(pushBackSeconds: Int = 0)(session: DBSession) = {
    implicit val sess = session
    sql"SELECT sysdate FROM dual".map(_.jodaDateTime(1)).single().apply().get.minusSeconds(pushBackSeconds)
  }
/*
  def minimizingETL[A <: Delta](splitSize: Long)(etl: etlType[A]): etlType[A] = {
    def minimizingETLAux(level: Int): etlType[A] =
      (delta: A) => session => {
        val savePointName = "POINT_" + Math.random().toInt.toString
        SQL.apply("SAVEPOINT " + savePointName).execute().apply()(session)
        val res = etl(delta)(session)
        res match {
          case Seq((delta, Right(msg))) => res
          case Seq((delta, Left(err))) =>
            SQL.apply("ROLLBACK TO " + savePointName).execute().apply()(session)
            if (delta.size <= splitSize || level == 10) res
            else delta.split.flatMap(chunk => minimizingETLAux(level+1)(chunk.asInstanceOf[A], session))
        }
      }
    minimizingETLAux(0)
  }
*/
  def storedFunctionETL[A <: Delta](functionName: String): etlType[A] =
  delta => session => {
    val (d1, d2) = delta match {
      case dlt:MillisDelta => dlt.unpack
      case dlt:DaysDelta   => dlt.unpack
      case _ => throw new RuntimeException("'storedFunctionETL' must be applied only to 'daysDelta' and 'millisDelta' instances.")
    }
    val conn = session.connection
    var stmt: CallableStatement = null
    try {
      stmt = conn.prepareCall(s"{CALL ? := $functionName(?, ?, ?)}")
      stmt.registerOutParameter(1, java.sql.Types.VARCHAR)
      stmt.setTimestamp(2, new Timestamp(d1.toDate.getTime))
      stmt.setTimestamp(3, new Timestamp(d2.toDate.getTime))
      stmt.registerOutParameter(4, java.sql.Types.NUMERIC)
      stmt.execute()
      Seq((delta, Right(s"${stmt.getString(1)}, items_processed: ${stmt.getLong(4)}")))
    } finally {
      if (stmt!=null) stmt.close()
    }
  }

/*
  def storedFunctionDiscreteETL[A](functionName: String): etlType[Discrete[A]] =
  (delta, session) => {
    val conn = session.connection
    var stmt: CallableStatement = null
    try {
      stmt = conn.prepareCall(s"{CALL ? := $functionName(?)}")
      stmt.registerOutParameter(1, java.sql.Types.VARCHAR)
      stmt.setString(2, delta.generateXML)
      stmt.execute()
      Seq((delta, Right(s"${stmt.getString(1)}, items_processed: ${delta.size}")))
    } finally {
      if (stmt!=null) stmt.close()
    }
  }
*/
  def relativeFile(fileName: String) = (utils.user_dir / fileName).toFile

  def executeSteps(tasks: List[() => Either[etlMessage, Any]]): List[Either[etlMessage, Any]] = {
    def executeSteps0(step: Int, tasks0: List[() => Either[etlMessage, Any]]): List[Either[etlMessage, Any]] =
    tasks0 match {
      case List() => List()
      case h::t => h.apply() match {
        case Right(str:String) => List(Right(s"step:$step,$str")) ++ executeSteps0(step+1, t)
        case Right(msg:etlMessage) => List(Right(etlMessage(s"step:$step,${msg.msg}", msg.recipients))) ++ executeSteps0(step+1, t)
        case Left(msg) => List(Left(etlMessage(s"step:$step,${msg.msg}", msg.recipients)))
      }
    }
    executeSteps0(1, tasks)
  }

  object sqlScript {

    abstract class sqlItem(val name: String, val sql: String) {
      def apply(implicit session: DBSession): String
    }

    class plsql(name: String, sql: String) extends sqlItem(name, sql) {
      def apply(implicit session: DBSession): String = { SQL(sql).execute().apply(); "ok" }
    }

    class select(name: String, sql: String) extends sqlItem(name, sql) {
      def apply(implicit session: DBSession): String = SQL(sql).map(_.string(1)).single().apply().get
    }

    class update(name: String, sql: String) extends sqlItem(name, sql) {
      def apply(implicit session: DBSession): String = SQL(sql).executeUpdate().apply().toString
    }

    class agent_function(name: String, function_name: String, delta: MillisDelta) extends sqlItem(name, function_name) {
      def apply(implicit session: DBSession): String =
        storedFunctionETL[MillisDelta](function_name).apply(delta)(session).head._2.toString
    }

    def createSteps(delta: Delta, file: scala.reflect.io.File, subst: Map[String, String]) = {
      val str = scala.io.Source.fromInputStream(file.inputStream()).mkString
      val str2 = (subst++delta.sqlSubs).foldLeft(str)((acc, pair) => acc.replace(pair._1, pair._2))
      str2.split("--STATEMENT").drop(1).map(item => {
        val CRLN1 = item.indexOf("\r\n")
        val CRLN2 = item.indexOf("\r\n", CRLN1+1)
        val name = if (CRLN1>0) item.substring(1, CRLN1) else item.substring(CRLN1+2, CRLN2)
        val sql = item.substring(CRLN1+2)
        if (sql.startsWith("BEGIN") || sql.startsWith("DECLARE")) new plsql(name, sql)
        else if (sql.startsWith("SELECT")) new select(name, sql.substring(0, sql.lastIndexOf(";")))
        else if (sql.startsWith("AGENT_FUNCTION")) new agent_function(name, sql.substring(15, sql.indexOf("\r\n")), delta.asInstanceOf[MillisDelta])
        else new update(name, sql.substring(0, sql.lastIndexOf(";")))
      })
    }

    def etl[A <: Delta](file: scala.reflect.io.File, subst: Map[String, String] = Map()): etlType[A] =
    delta => session => {
      val steps = createSteps(delta, file, subst).map{ item => () => {
        val start = System.currentTimeMillis()
        val res = Try(item.apply(session))
        val duration = (System.currentTimeMillis() - start)/1000
        res match {
          case Success(r) => Right(s"${item.name} [$duration secs]: $r")
          case Failure(e) => Left(etlMessage(s"${item.name} [$duration secs]\n${e.getMessage}\n"))
        }
      }}
      Seq((delta, aggregateResults(file.name, executeSteps(steps.toList))))
    }

  }

  //def printDeltaETL[A <: Delta](): etlType[A] = delta => session => { println(delta) ; Seq((delta, Right())) }

  case class etlEntry(dbName: String, key: String) {

    def select = utils.mkConnProvider(dbName).apply(Reader(implicit session =>
      Try(
        sql"SELECT value FROM COGNOS.etl_property WHERE key=?".bind("analytics.tiger.AnalysisScannerAgent1").map(_.string(1)).single().apply() match {
          case Some(value) => value
          case None => throw new RuntimeException("Key not found.")
        }
      )))

    def insert = utils.mkConnProvider(dbName).apply(Reader(implicit session =>
      Try(
        sql"INSERT INTO COGNOS.etl_property VALUES(?, ?)"
          .bind(key, ETL.millisFormatter.print(new DateTime()))
          .execute().apply()
    )))

    def update(value: String) = utils.mkConnProvider(dbName).apply(Reader(implicit session =>
      Try(
        sql"UPDATE COGNOS.etl_property SET value=? WHERE key=?"
          .bind(value, key)
          .update().apply() match {
          case 1 => Success()
          case _ => throw new RuntimeException(value)
        }
      )))

  }

  def defaultErrorEmailer(subject: String)(res: resultsType) {
    val recipients  = res.flatMap{
        case (_, Left (etlMessage(msg, recipients))) => recipients
        case (_, Right(etlMessage(msg, recipients))) => recipients
        case _ => Seq()
    }.distinct
    recipients.foreach(recipient => {
      val content = res.foldLeft(List[String]()){
          case (acc, (delta, Left (etlMessage(msg, recipients)))) if (recipients.contains(recipient)) => s"ERROR\n$delta => $msg" :: acc
          case (acc, (delta, Right(etlMessage(msg, recipients)))) if (recipients.contains(recipient)) => s"WARNING:\n$delta => $msg" :: acc
          case (acc, _) => acc
      }
      utils.sendEmail(
        "atanas@broadinstitute.org",
        Seq(recipient),
        s"ETL ${if (res.exists(_._2.isLeft)) "Errors" else "Warnings"} , $subject",
        content.mkString("","\n\n","")
      )
    })
  }

  type resultsType = Seq[(Delta, Either[etlMessage, Any])]
  type etlType[A <: Delta] = A => DBSession => resultsType

  def prepareEtl[A <: Delta, B <: Delta]
  (
    agentName: String,
    delta: A = dummyDelta,
    etl: etlType[B]
  )
  (
    command: String ="",
    chunkSize: Long = Long.MaxValue,
    toCommit: resultsType => Boolean = isEverythingOk,
    convertor: A => DBSession => B = (а: A) => (_: DBSession) => а,
    propLink: String = "",
    exceptionToResult: Exception => Either[etlMessage, Any] = e => Left(etlMessage(e.getMessage))
  )
  (implicit handler: DeltaHandler[B])
  =
  Reader[DBSession, resultsType](implicit session => {
    assert(chunkSize>0, "chunkSize must be > 0")
    val id = utils.recordStart(agentName, command, s"trying delta $delta", propLink)(session)
    val convertedDelta = convertor(delta)(session)
    val chunkedDelta = handler.chunk(convertedDelta, chunkSize)
    val res = chunkedDelta flatMap (delta =>
      try etl(delta)(session)
      catch { case e: Exception => Seq((delta, exceptionToResult(e))) }
    )
    val myToCommit = toCommit(res)
    val details =
      (if (delta.persist(session) == 0) "Adhoc  " else "Tiger-managed ") +
      s"${if (delta != convertedDelta) s"Root delta: $delta" else "delta "}, HOST: ${System.getenv("HOST")} (transaction: ${if (myToCommit) "commited" else "rolled back"})" +
      (if (res.isEmpty) s"<br>$delta => No data accumulated."
      else res.sortBy(_._2.isRight).foldLeft("")((acc, it) =>
        if (acc.size < 3900) s"$acc <br>${if (convertedDelta.isInstanceOf[DiscreteDelta[_]]) "<br>" else ""}${if (it._2.isLeft) "<b>" else ""}${
          it._1 match {
            case DiscreteDelta(el: Set[Any]) if (el.size == 1) => el.head
            case _ => it._1
          }
        } => ${
          (it._2 match {
            case Left (etlMessage(msg,_)) => s"ERROR($msg)"
            case Right(etlMessage(msg,_)) => s"Success(WARNING($msg))"
            case Right(r) => s"Success(${r.toString})"
          }).replace("\n", "<br>")
        }</b>"
        else acc
      ))
    utils.recordEnd(id, if (res.exists(_._2.isLeft)) 1 else 0, details, propLink)(session)
    if (myToCommit) session.connection.commit() else session.connection.rollback()
    res
  })

  implicit def deltaToProvider[A <: Delta](delta: A) = (_: DBSession) => delta
  implicit def ToReader[A](f: DBSession => A) = Reader(f)

  def isEverythingOk = (res: resultsType) => res.forall(_._2.isRight)

  def aggregateResults(prefix: String, res: Seq[Either[etlMessage, Any]]): Either[etlMessage, Any] = {
    case class accum(errors: List[String], warnings: List[String], successes: List[String], recipients: Seq[String])

    val r = res.foldRight(new accum(List(), List(), List(), Seq()))((it, acc) => it match {
      case Left (etlMessage(msg, rcpts)) => acc.copy(errors = msg :: acc.errors    , recipients = acc.recipients ++ rcpts)
      case Right(etlMessage(msg, rcpts)) => acc.copy(warnings = msg :: acc.warnings, recipients = acc.recipients ++ rcpts)
      case Right(a)                      => acc.copy(successes = a.toString :: acc.successes)
    })
    if (!r.errors.isEmpty) Left(etlMessage(s"${if (prefix=="") "" else s"$prefix => "}${r.errors.mkString("\n", "\n", "")}", r.recipients.toSeq))
    else if (!r.warnings.isEmpty) Right(etlMessage(s"$prefix => ${r.warnings.mkString("\n", "\n", "")}", r.recipients))
    else Right(s"$prefix => ${r.successes.mkString("\n", "\n", "")}")
  }

  def mergeResults(a: Either[etlMessage, Any], b: Either[etlMessage, Any]) = (a, b) match {
    case (Left(etlMessage(msg1, rcpt1)), Left(etlMessage(msg2, rcpt2))) => Left(etlMessage(msg1 + "\n" + msg2, rcpt1 ++ rcpt2))
    case (Left(etlMessage(msg1, rcpt1)), Right(etlMessage(msg2, rcpt2))) => Left(etlMessage(msg1 + "\n" + msg2, rcpt1 ++ rcpt2))
    case (Left(etlMessage(msg1, rcpt1)), Right(msg2)) => Left(etlMessage(msg1 + "\n" + msg2, rcpt1))

    case (Right(etlMessage(msg1, rcpt1)), Left(etlMessage(msg2, rcpt2))) => Left(etlMessage(msg1 + "\n" + msg2, rcpt1 ++ rcpt2))
    case (Right(etlMessage(msg1, rcpt1)), Right(etlMessage(msg2, rcpt2))) => Right(etlMessage(msg1 + "\n" + msg2, rcpt1 ++ rcpt2))
    case (Right(etlMessage(msg1, rcpt1)), Right(msg2)) => Right(etlMessage(msg1 + "\n" + msg2, rcpt1))

    case (Right(msg1), Left(etlMessage(msg2, rcpt2))) => Left(etlMessage(msg1 + "\n" + msg2, rcpt2))
    case (Right(msg1), Right(etlMessage(msg2, rcpt2))) => Right(etlMessage(msg1 + "\n" + msg2, rcpt2))
    case (Right(msg1), Right(msg2)) => Right(msg1 + "\n" + msg2)
  }

  implicit def booleanToCommit(b: Boolean) = (_:resultsType) => b

  def main(args: scala.Array[String]): Unit = {

    val checker = (session: DBSession) => {
      implicit val sess = session
      sql"SELECT * FROM COGNOS.etl_property".map(_.string(1)).list().apply().foreach(println _)
    }

    utils.CognosDB.apply(Reader(checker))

  }

}

