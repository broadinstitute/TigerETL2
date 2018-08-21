package analytics.tiger

import java.io.{File, StringReader}
import java.sql.DriverManager
import java.util.Properties

import analytics.tiger.api.jira
import com.ning.http.client.{AsyncCompletionHandler, Response}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.xml.sax.InputSource
import scalikejdbc._
import scalikejdbc.config._

import scala.util.Try
//import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsString, JsValue, RootJsonFormat}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.xml.XML

/**
 * Created by atanas on 12/16/2014.
 */
object utils {
  // http://doc.akka.io/docs/akka/snapshot/general/configuration.html
  lazy val config = ConfigFactory.load()
  scalikejdbc.config.TypesafeConfigReader.loadGlobalSettings()
  lazy val user_dir = scala.reflect.io.File.apply(scala.tools.nsc.io.File(System.getenv("TIGER_HOME")))
  lazy val CognosDB = utils.mkConnProvider("db.default")
  lazy val BspDB = utils.mkConnProvider("db.bsp")
  lazy val DevDB = utils.mkConnProvider("db.dev")
  lazy val AnalyticsEtlDB = utils.mkConnProvider("db.analyticsetl")

  def recordStart(agentName: String, command: String, details:String, propLink: String = "")(implicit session: DBSession) = {
    val id = SQL(s"SELECT COGNOS.etl_runs_seq.NEXTVAL$propLink FROM dual").map(_.long(1)).single().apply().get
    SQL(s"""
         DECLARE
         PRAGMA AUTONOMOUS_TRANSACTION;
         BEGIN
         INSERT INTO COGNOS.etl_runs$propLink VALUES(?, ?, SYSDATE, NULL, NULL, ?, ?) ;
         COMMIT ;
         END ;
      """).bind(id, agentName, details, command).execute().apply()
    id
  }

  def recordEnd(id:Long, returnCode:Long, details:String, propLink: String = "")(implicit session: DBSession) = {
    SQL(s"""
         DECLARE
         PRAGMA AUTONOMOUS_TRANSACTION;
         BEGIN
         UPDATE COGNOS.etl_runs$propLink SET end_time=SYSDATE, return_code=?, details=? WHERE id=? ;
         COMMIT ;
         END ;
      """).bind(
        returnCode,
        if (details.size>4000) details.substring(0,4000) else details,
        id
      ).execute().apply()
  }

  def generateMERGE(tableName: String, preservedFields: List[String] = List())(session: DBSession) = {
    implicit val sess = session
    val columns = sql"SELECT column_id-1 col_index, column_name, data_type, data_precision, nvl(data_scale,0) data_scale, data_length FROM USER_TAB_COLUMNS WHERE table_name=? ORDER BY column_id".bind(tableName).map(row => row.string("column_name")).list().apply()

    val pkColumns = sql"""
SELECT cols.column_name --, cols.position, cons.status, cons.owner, cons.constraint_name
FROM all_constraints cons
JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
WHERE cols.table_name = ?
AND cons.constraint_type = 'P'
ORDER BY cols.position
""".bind(tableName).map(row => row.string("column_name")).list().apply()

    s"""
MERGE INTO $tableName tr
USING(SELECT ${(columns.map(col => s"? $col")).mkString(",")} FROM dual)
delta ON (${pkColumns.map(col => s"tr.$col=delta.$col").mkString(" AND ")})
WHEN NOT MATCHED THEN INSERT VALUES(${columns.map("delta." + _).mkString(",")})
WHEN MATCHED THEN UPDATE SET ${(columns.toList.filter(col => !(pkColumns++preservedFields).exists(_ == col)).map(col => s"$col=delta.$col").mkString(","))}"""
  }

  def sendEmail(from: String, to: Seq[String], subject: String, text: String) {
    import javax.mail.Message
    import javax.mail.internet.{InternetAddress, MimeMessage}

    // Setup mail server
    val props = new Properties()
    props.put("mail.smtp.host", "smtp.broadinstitute.org")
    val session = javax.mail.Session.getDefaultInstance(props, null)

    // Define message
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(from))
    to foreach { r => message.addRecipient(Message.RecipientType.TO, new InternetAddress(r)) }
    message.setSubject(subject)
    message.setText(text)

    // Send message
    javax.mail.Transport.send(message)
  }

  trait ConnProvider {
    def apply[A](f: Reader[DBSession, A]): A
  }


  def mkConnProvider(connectInfo: String, allParams: Boolean = false) = new ConnProvider {
    def apply[A](f: Reader[DBSession, A]): A = {
      import collection.JavaConverters._
      val conf = utils.config.getConfig(connectInfo)
      val keys = if (allParams) conf.entrySet().asScala.map(_.getKey).filterNot(it => Seq("driver","url","user","password").exists(_ == it)) else Set.empty[String]
      Class.forName(conf.getString("driver"))
      // http://scalikejdbc.org/documentation/query-inspector.html
      // GlobalSettings.loggingSQLAndTime = new LoggingSQLAndTimeSettings(enabled = Option(System.getProperty("scalikejdbc.LoggingSQLAndTimeSettings")).getOrElse("FALSE").toBoolean)
      val db = DB.connect(DriverManager.getConnection(
        conf.getString("url") + (if (keys.isEmpty) "" else keys.map(it => it + "=" + conf.getString(it)).mkString("?","&","")),
        conf.getString("user"),
        conf.getString("password"))
      )
      try {
        db.begin()
        db withinTx (f(_))
      }
      finally { db.commit() ; db.close() }
    }
  }



  def generateXML(objects: Seq[Object]) = objects.map {
    case s: String => s"""<item value=\"$s\"/>"""
    case o: Any =>
      val clazz = o.getClass
      clazz.getDeclaredFields.map { it =>
        val fieldName = it.getName
        s"""$fieldName=\"${clazz.getDeclaredMethod(fieldName).invoke(o)}\""""
      }.mkString("<item ", " ", "/>")
  }.mkString("<delta>", "", "</delta>")

  def argsToMap(args: Array[String]) =
    args.map { arg =>
      val Array(key, value) = arg.split("=")
      key -> value
    }.toMap

  case class RichResultSet(metadata: List[(String, Int)], pk: Seq[String], data: List[Array[Option[Any]]]) {
    def generateTable(styleFunc: Array[Option[Any]] => String = _ => "") = {
      s"""<table border="1" bordercolor="#000000" width="100%" cellpadding="5" cellspacing="3">${
        metadata.map(it => s"<td>${it._1}</td>").mkString("<tr>", "", "</tr>")
      }${
        data.map(row =>
          row.map(it => s"<td>${
            it match {
              case None => ""
              case Some(a) => a.toString
            }
          }</td>").mkString(s"<tr ${styleFunc(row)}>", "", "</tr>")
        ).mkString
      }</table>"""
    }
  }

  def getRichResultSet(sql: String)(session: DBSession) = {
    val res = SQL(sql).foldLeft((List.empty[(String, Int)], List.empty[Array[Option[Any]]]))((acc, rs) => {
      val mtd = acc._1 match {
        case Nil =>
          val mtd = rs.metaData
          (for (jjj <- 1.to(mtd.getColumnCount)) yield (mtd.getColumnName(jjj), mtd.getColumnType(jjj)))
        case l: List[(String, Int)] => l
      }
      val values = mtd.map { case (colName, colType) =>
        try Option(colType match {
          case java.sql.Types.VARCHAR | java.sql.Types.CHAR => rs.string(colName)
          case java.sql.Types.TIMESTAMP => rs.jodaDateTime(colName)
          case java.sql.Types.INTEGER =>
          case java.sql.Types.NUMERIC => rs.long(colName)
          case _ => rs.string(colName)
        })
        catch {
          case _ => None
        }
      }
      (mtd.toList, values.toArray :: acc._2)
    })(session)
    RichResultSet(res._1, Seq(), res._2.reverse)
  }

  def getPK(owner: String, table: String)(implicit session: DBSession) = {
    SQL( """SELECT cols.column_name
          |FROM all_constraints cons, all_cons_columns cols
          |WHERE cols.table_name = ?
          |AND cons.constraint_type = 'P'
          |AND cons.constraint_name = cols.constraint_name
          |AND cons.owner = cols.owner
          |AND cons.owner = ?
          |""".stripMargin).bind(table, owner).map(_.string(1)).list().apply()
  }

  def repeat(endTime: DateTime, conn: ConnProvider, etl : Reader[DBSession, ETL.resultsType]) {
    var n = 0
    var done = false
    while (!done) {
      println(s"Run $n")
      n += 1
      done = if (DateTime.now.isAfter(endTime)) {
        println("EndTime reached") ; true
      } else {
        val res = conn(etl)
        println(res)
        if (res.exists(_._1.size==0)) { println("Delta is exhausted.") ; true } else
        if (res.exists(_._2.isLeft)) { println("ETL error is detected.") ; true }
        else false // keep moving
      }
    }
  }

  val stripHtml = {
    val parser = (new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl).newSAXParser
    val adapter = new scala.xml.parsing.NoBindingFactoryAdapter
    (str: String) => adapter.loadXML(new InputSource(new StringReader(str)), parser).text
  }

  def loadXML(str: String) = {
    val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
    val parser = parserFactory.newSAXParser
    val adapter = new scala.xml.parsing.NoBindingFactoryAdapter
    val source = new InputSource(new StringReader(str))
    adapter.loadXML(source, parser)
  }


  //import analytics.tiger._
  //jira.process.map(tableau.toTDE).apply(jira.request("labopsjira", "filter=13334", "key,labels,links:toList( ):toString(\\n),customfield_13968:toDouble,status,reporter,created:toDate,customfield_10010:toList(<br/>\\n):toString","analytics", "analytics"))
  val toTDE = (jiraResponse: jira.response) => {
    import com.tableausoftware.DataExtract.{Extract, Row, TableDefinition, Type}

    lazy val folderName = utils.config.getString("tableauExtractFolder")
    val internalFileName = s"Jql_Extract_${new java.util.Random().nextInt(Int.MaxValue)}.tde"
    val file = new File(folderName, internalFileName)
    if (file.exists() && !file.delete()) throw new RuntimeException(s"$internalFileName exists and can't be deleted.")
    val tdefile = new Extract(s"$folderName${File.separator}$internalFileName")

    val tableDef = new TableDefinition()
    val workers = jiraResponse.fields.map{ case jira.metadata(_, fieldName, extractor) =>
      val (ttype, func) = extractor match {
        case x: jira.stringExtractor      => (Type.CHAR_STRING, (value: Any, row: Row, index: Int) => row.setCharString(index, value.asInstanceOf[String]))
        case x: jira.intExtractor         => (Type.INTEGER    , (value: Any, row: Row, index: Int) => row.setInteger(index, value.asInstanceOf[Int]))
        case x: jira.doubleExtractor      => (Type.DOUBLE     , (value: Any, row: Row, index: Int) => row.setDouble(index, value.asInstanceOf[Double]))
        case x: jira.booleanExtractor     => (Type.BOOLEAN    , (value: Any, row: Row, index: Int) => row.setBoolean(index, value.asInstanceOf[Boolean]))
        case x: jira.dateExtractor        => (Type.DATETIME   , (value: Any, row: Row, index: Int) => {
          val date = value.asInstanceOf[DateTime]
          row.setDateTime(index, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(), date.getHourOfDay, date.minuteOfHour().get(), date.secondOfMinute().get(), 0)
        })
      }
      (fieldName, ttype, func)
    }.zipWithIndex

    workers.foreach { case ((friendlyFieldName, ttype, _), _) => tableDef.addColumn(friendlyFieldName, ttype) }

    val table = tdefile.addTable("Extract", tableDef)
    var size = 0
    val row = new Row(tableDef)
    jiraResponse.stream.foreach{ line =>
      workers.foreach{ case ((_, _, func), index) =>
        line(index) match {
          case None => row.setNull(index)
          case Some(v) => func(v, row, index)
      }}
      table.insert(row)
      size += 1
    }
    tdefile.close()

    // https://github.com/spray/spray/blob/master/examples/spray-routing/on-spray-can/src/main/scala/spray/examples/DemoService.scala#L141
    scala.concurrent.Future {
      Thread.sleep(30*1000)
      file.delete()
    }
    (file, size)
  }

  //def toJsonField(s: String) = utils.loadXML(s).text replace("\"", "\\\"") replace("\n", "\\n") replace("\t", "\\t")

  val jsondtf = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss'.000Z'")

// https://github.com/spray/spray-json#providing-jsonformats-for-case-classes

  val toJson = (jiraResponse: jira.response) => {
    import spray.json._
    var firstTime = true
    "{" #::
    s""""fieldNames": ${ JsArray(jiraResponse.fields.map {
        case jira.metadata(_, userFriendlyFieldName, _) => JsString(userFriendlyFieldName)
      }.toVector).toString
    },""" #::
    s""""fieldTypes": ${
      JsArray(jiraResponse.fields.map { case jira.metadata(_, _, extractor) => JsString(extractor match {
      case x: jira.stringExtractor => "string"
      case x: jira.intExtractor => "integer"
      case x: jira.doubleExtractor => "float"
      case x: jira.booleanExtractor => "boolean"
      case x: jira.dateExtractor => "datetime"
    })}.toVector).toString},""" #::
    """"results" : [""".stripMargin #::
    jiraResponse.stream.map{ line =>
      { if (firstTime) { firstTime=false ; "" } else "," } +
      JsArray(line.map{
        case None => JsNull
        case Some(dt: DateTime) => JsString(jsondtf.print(dt))
        case Some(it) => JsString(it.toString)
      }.toVector).toString
    } ++ Stream("]}")
  }

  def urlEncode(s: String) = {
    def isSafe(ch: Char) = (ch>='A' && ch<='Z') || (ch>='a' && ch<='z') || (ch>='0' && ch<='9') || "$-_.!*'(),:;".exists(_ == ch)
    s.map(ch => if (isSafe(ch)) ch else java.net.URLEncoder.encode(ch.toString,"UTF-8")).mkString
  }

  val digits = "0123456789ABCDEF"
  def ascEncode(s: String) = s.map(ch => if (ch>=' ') ch else s"\\x${digits(ch.toInt/16)}${digits(ch.toInt%16)}").mkString
  def ascDecode(s: String) = """(\\x([0123456789ABCDEF]){2})""".r.findAllIn(s).map{_.toString}.toList.distinct.foldLeft(s){ case (acc, it) => acc.replaceAllLiterally(it, (16*digits.indexWhere(_ == it(2)) + digits.indexWhere(_ == it(3))).toChar.toString) }

  case class ApiHttpError(code: Int, body: String) extends Exception(s"Unexpected response status: $code: $body")

  class OkWithBodyHandler[T](f: Response => T, msgParser: (String => String) = (it:String) => it) extends AsyncCompletionHandler[T] {
    def onCompleted(response: Response) = {
      if (response.getStatusCode / 100 == 2) f(response)
      else throw ApiHttpError(response.getStatusCode, msgParser(response.getResponseBody))
    }
  }

  def objectName(o: Object) = {
    val s = o.getClass.getName
    s.substring(0, s.size-1)
  }

  def configToMap(config: Config) = {
    import collection.JavaConverters._
    config.entrySet().asScala.toList.map(it => it.getKey -> it.getValue.unwrapped.toString).toMap
  }

}

