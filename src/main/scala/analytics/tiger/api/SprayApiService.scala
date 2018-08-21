package analytics.tiger.api

import java.net.{InetAddress, URLEncoder}
import java.nio.file.{Files, StandardCopyOption}

import akka.actor.{Actor, ActorSystem}
import analytics.tiger.{ETL, Reader, utils}
import dispatch.Http
import play.twirl.api.{Html, HtmlFormat}
import scalikejdbc.ConnectionPool
import spray.routing._
import spray.http._
import spray.util.LoggingContext
import scalikejdbc._

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class TigerActor extends Actor with TigerService {

  implicit def myExceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e: Exception =>
        ctx => {
          log.error(e.getMessage)
          ctx.complete(StatusCodes.BadRequest, e.getMessage)
        }
    }

  def actorRefFactory = context
  //import scala.concurrent.duration._
  //implicit val timeout = akka.util.Timeout(180 seconds)
  def receive = runRoute(TigerRoute)
}

object ledger {
  val start = org.joda.time.DateTime.now
  var shuttingDown = false
}

// http://kamon.io/teamblog/2014/11/02/understanding-spray-client-timeout-settings/
trait TigerService extends HttpService with Authenticator {

  val regularFields = List(
    Array("timestamp", "timestamp" ,""),
    Array("key", "key" ,""),
    Array("title", "title" ,""),
    Array("link", "link" ,""),
    Array("project", "project" ,""),
    Array("description", "description" ,""),
    Array("environment", "environment" ,""),
    Array("summary", "summary" ,""),
    Array("type", "type" ,""),
    Array("priority", "priority" ,""),
    Array("status", "status" ,""),
    Array("statusCategory", "statusCategory" ,""),
    Array("resolution", "resolution" ,""),
    Array("assignee", "assignee" ,""),
    Array("reporter", "reporter" ,""),
    Array("labels", "labels" ,""),
    Array("created", "created" ,""),
    Array("updated", "updated" ,""),
    Array("components", "components" ,""),
    Array("due", "due" ,""),
    Array("votes", "votes" ,""),
    Array("watches", "watches" ,""),
    Array("issuelinks", "issuelinks" ,""),
    Array("attachments", "attachments" ,""),
    Array("subtasks", "subtasks" ,""),
    Array("customfields", "customfields" ,"")
  )

  def curlJiraCall(token: String, url: String, jql: String, fields: Option[String]) = {
    // https://www.base64encode.org/
    val buffer = new StringBuffer()
    val cmd = s""""${utils.config.getString("curl")}" -k -X GET -H "Authorization: Basic $token" -H "Content-Type: application/json" "$url?jql=${URLEncoder.encode(jql,"UTF-8")}&fields=${fields.getOrElse("")}&maxResults=100""""
    println(cmd)
    //val res = cmd ! ProcessLogger(buffer append _)
    //println(buffer.toString)
    buffer.toString
  }

  def launchEtlRoute(agent_name: String) = respondWithMediaType(MediaTypes.`text/plain`) {
    complete{
      import sys.process._
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val cmd = utils.config.getString("launchEtlAgent").replace("AGENT_NAME", agent_name)
      val status = Process(s"""cmd /K $cmd""") ! ProcessLogger(stdout append _, stderr append _)
      val errStr = stderr.toString()
      val outStr = stdout.toString()
      if (errStr!="") throw new RuntimeException(errStr)
      else s"Agent $agent_name launched at ${DateTime.now}"
    }}

  def jiraCall(token: String, url: String, jql: String, fields: Option[String]) = {
    val res = curlJiraCall(token, url, jql, fields)
    val totalRegex = """(.*)"total":(\d+),(.*)""".r
    val errRegex = """(.*)"errorMessages":\["(.*)"\](.*)""".r
    res match {
      case errRegex(_,err,_) =>
        println(err)
        throw new RuntimeException(err)
      case totalRegex(_,total,_) if (total == "0") => throw new RuntimeException("No data found.")
      case _ => complete(HttpData(res.getBytes))
    }
  }

  def etlRunsRoute(agentName: String, show_command: Option[String], latest_run_only: Option[String]) = {
    respondWithMediaType(MediaTypes.`text/html`) { complete{ using(ConnectionPool.borrow()) { conn => {
      DB(conn).readOnly(implicit session => {
        SQL(s"""SELECT '<a href="/etl_runs/' || agent_name || '">' || agent_name || '</a>' agent_name, start_time, end_time, 86400*(nvl(end_time,sysdate)-start_time) duration, decode(return_code, NULL, decode(sign(sysdate-1-start_time), -1, 'Running', 'Abandoned'), 5, 'Halted', 0, 'Succeeded', 'Failed') status, details, command
                |FROM (
                |   SELECT a.*, row_number() over(PARTITION BY agent_name ORDER BY start_time DESC) myrank
                |   FROM etl_runs a
                |   WHERE (instr(agent_name, ?)>0 or ?='All') and start_time>=trunc(sysdate)-5)
                |WHERE ?='false' OR myrank=1
                |ORDER BY start_time desc
                  """.stripMargin).bind(agentName, agentName, toBoolean(latest_run_only).toString).map(it => (it.string(1),it.timestamp(2),it.timestamp(3),it.int(4),it.string(5),it.string(6),it.string(7))).list().apply()
          .map{ case (agent_name, start_time, end_time, duration, status, details, command) =>
            s"""
<tr style="background-color: ${ status match {
              case "Succeeded" => if (details!=null && details.indexOf("WARNING") > -1) "orange" else "white"
              case "Failed" => "red"
              case "Running" => "yellow"
              case "Abandoned" => "aqua"
              case "Halted" => "LightBlue"
            }
            }"><td>
<img src="/images/${
              if (agent_name.indexOf("analytics.tiger.spark.") > -1) "Spark.jpg"
              else if (agent_name.indexOf("analytics.tiger.") > -1) "Tiger.jpg"
              else if (agent_name.indexOf(".plsql.")> -1) "plsql.jpg"
              else "Chicken.jpg"
            }" height=20 width=20>
$agent_name</td>
<td>${ETL.millisFormatter.print(start_time.getTime)}</td>
<td>${if (end_time==null) "" else ETL.millisFormatter.print(end_time.getTime)}</td>
<td>${duration/60} mins, ${duration%60} sec</td>
<td>$status</td>
<td>${if (end_time==null) "" else details}</td>
<td>${if (toBoolean(show_command)) command else ""}</td>
</tr>
"""}.mkString("""<html><body><img src=/images/PoweredBy.jpg><table border="1" bordercolor="#000000" width="100%" cellpadding="5" cellspacing="3"><tr><td>AGENT_NAME</td><td>START_TIME</td><td>END_TIME</td><td>DURATION</td><td>STATUS</td><td>DETAILS</td><td>COMMAND</td></tr>""","","</table></body></html>")
      })
    }}}}
  }

  def peekAtTableRoute(tableName: String, rows_limit: String) =
    respondWithMediaType(MediaTypes.`text/html`) { complete{
      val extr = """((.*)\.)?(.*)\.(.*)""".r
      val extr(_,db,schema,table) = tableName
      using((db match {
        case null | "SEQPROD" => ConnectionPool()
        case "GAP_PROD" => ConnectionPool('bsp)
        case "GPINFO" => ConnectionPool('gpinfojira)
        case "SEQBLDR" => ConnectionPool('labopsjira)
      }).borrow()
      ) { conn => DB(conn).readOnly(session =>
        utils.getPK(schema, table)(session).mkString("PrimaryKey: [",", ","]") +
          utils.getRichResultSet(s"SELECT * FROM $schema.$table WHERE rownum<=$rows_limit")(session).generateTable()
      )}}}
/*
  def peekAtSQLRoute(sqlIdStr: String) =
    respondWithMediaType(MediaTypes.`text/html`) { complete{
      val extr = """((.*)\.)?(.*)""".r
      val extr(_,db,sql_id) = sqlIdStr
      using((db match {
        case null | "SEQPROD" => ConnectionPool()
        case "GAP_PROD" => ConnectionPool('bsp)
        case "GPINFO" => ConnectionPool('gpinfojira)
        case "SEQBLDR" => ConnectionPool('labopsjira)
      }).borrow()
      ) { conn => DB(conn).readOnly(session =>
        val clob = sql"SELECT sql_fulltext FROM v$$sqlarea WHERE sql_id=?".bind(sql_id).map(_.clob(1)).single().apply().get
        val ss = scala.io.Source.fromInputStream(clob.getAsciiStream)
        Files.readAllBytes()
        new String()
        ""
      )}}}
*/
  def generateSql(project_name: String, domain: String) = {
    respondWithMediaType(MediaTypes.`text/plain`) { complete{
      using(ConnectionPool('labopsjira).borrow()) { DB(_).readOnly(implicit session => {
        var num = 0
        val (schemaFunc, jiraName) = domain match {
          case "gpinfojira" => ("gpinfojira_schema", "GPINFOJIRA")
          case "labopsjira" => ("labopsjira_schema", "LABOPSJIRA")
          case _ => throw new RuntimeException("unknown domain")
        }
        val cols = sql"SELECT cfname, cftype FROM jiradwh.cf_catalog WHERE pkey=? AND instance=?".bind(project_name, jiraName).map{it => {
          num += 1
          val cf_name = it.string(1)
          val cf_name_sanitized = "CF_" +(if (cf_name.length>27) cf_name.substring(0, 25) + num else cf_name).replaceAll("[^A-Za-z0-9]", "_").toUpperCase
          (cf_name, String.format("%-30s", cf_name_sanitized) ,it.string(2), cf_name_sanitized)
        }}.list().apply().filter(_._1.indexOf("'")== -1)
        if (cols.isEmpty) "Project " + project_name + " not found." else {
          // val schema_name = SQL(s"SELECT jiradwh.etl.$schemaFunc FROM dual").map(_.string(1)).single().apply().get
          val schema_name = "jiradwh.etl.labopsjira_schema"
          val funcs = Map("String" -> "get_string_fields  ", "Text" -> "get_text_fields    ", "Number" -> "get_number_fields  ", "Date" -> "get_date_fields    ", "Option" -> "get_option_fields  ", "Cascade" -> "get_cascade2_fields")
          "-- Query for labopsjira project: " + project_name + " automatically generated on " + String.format("%1$tm/%1$td/%1$tY %1$tH:%1$tM:%1$tS", new java.util.Date) + "\n\n" +
            "WITH\n" +
            cols.map(col => "\t" + col._2 + " AS (SELECT * FROM TABLE(jiradwh.etl." + funcs(col._3) + "(" + schema_name + ", '" + col._1 + "')))").mkString(",\n") + "\n\n" +
            "SELECT \n" +
            """
              |    -- PK
              |    a.id,
              |    --
              |    a.key, --UNIQUE
              |    a.project, a.project_lead, a.project_key,
              |    a.reporter, a.assignee, a.type, a.summary, a.description,
              |    a.priority, a.resolution, a.status, a.created, a.updated,
              |    a.duedate, a.resolutiondate, a.votes, a.timeoriginalestimate,
              |    a.timeestimate, a.timespent, a.workflow_id, a.security, a.fixfor,
              |    a.component, a.watches,
              |
            """.stripMargin + "\n" +
            cols.map(col => "\t" + (if (col._3=="Cascade") col._4 + ".VALUE1||','||" + col._4 + ".VALUE2   " else col._2 + ".VALUE ") +
              (if (col._2.apply(3) < 'A' || col._2.apply(3) > 'Z') "A" + col._2.substring(4) else col._2.substring(3))
            ).mkString(",\n") + "\n\n" +
            "FROM TABLE (jiradwh.etl.get_issues('" + schema_name + "', '" + project_name + "')) a\n\n" +
            cols.map(col => "LEFT JOIN " + col._2 + " ON " + col._2 + ".issue_id=a.id").mkString("\n")

        }
      })}}}

  }

  def bspFreezerLocations(jiraTicket: String) = respondWithMediaType(MediaTypes.`text/html`) { complete{
    using(ConnectionPool('labopsjira).borrow()) { conn => DB(conn).readOnly(session =>
      using(ConnectionPool('bsp).borrow()) { DB(_).localTx(implicit bsp_session => {
        SQL("""
              |SELECT trim(substr(value,4)) internal_sample_id
              |FROM TABLE(jiradwh.etl.get_itemized_fields(jiradwh.etl.schema_name, 'Sample IDs', jiradwh.issue_table(
              |    (SELECT id FROM TABLE(jiradwh.etl.get_issues(jiradwh.etl.schema_name, 'SRS')) WHERE key=?)
              |)))
              |WHERE SUBSTR(VALUE,1,3)='SM-'
            """.stripMargin).bind(jiraTicket).foreach(it =>
          sql"INSERT INTO temp_samples values(?)".bind(it.string(1)).executeUpdate().apply()(bsp_session)
        )(session)

        utils.getRichResultSet("""
                                 |SELECT
                                 |    fl.sc1, fl.sc2, fl.sc3, fl.sc4,
                                 |
                                 |    fl.container_id,
                                 |    fl.container_type,
                                 |    fl.position,
                                 |    fl.sample_id,
                                 |    fl.material_type_name
                                 |
                                 |FROM temp_samples target
                                 |LEFT JOIN bsp_freezer_locations fl ON fl.internal_sample_id=target.sample_id
                                 |ORDER BY 1
                               """.stripMargin)(bsp_session).generateTable(row =>
          s"""style="background-color:${
            if (row(8) /*materialType*/ == null) "Red"
            else if (row(0) /*SC1*/ == null) "Pink"
            else "white"
          }""""
        )
      })})
    }}}

  def toBoolean(str: Option[String]) = str.map(_.compareToIgnoreCase("true")==0).getOrElse(false)

  class projectSampleRequest(val project: String, val sample: String, val user_name: String, val params: Option[String])

  object projectSampleRequestR {
    def unapply(s: String): Option[projectSampleRequest] =
      """^([^:]+):([^:]+):([^:]+)(:([^:]+))?$""".r.unapplySeq(s) match {
        case Some(List(a, b, c, _, params)) => Some(new projectSampleRequest(a, b, c, Option(params)))
        case None => None
      }
  }

  def requestFinalAggregation(items: Array[projectSampleRequest]) = {
    val conf = utils.config.getConfig("aggregation_jms")
    val connectionFactory = new org.apache.activemq.ActiveMQSslConnectionFactory(conf.getString("url"))
    connectionFactory.setUserName(conf.getString("username"))
    connectionFactory.setPassword(conf.getString("password"))

    val connection = connectionFactory.createConnection()
    val session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE)
    connection.start()
    val queue = session.createQueue(conf.getString("queue"))
    val producer = session.createProducer(queue)

    val res = items.foldLeft(0){ (acc, it) => try {
      val newMessage = session.createTextMessage()
      newMessage.setStringProperty("project", it.project)
      newMessage.setStringProperty("sampleAlias", it.sample)
      newMessage.setStringProperty("userName", it.user_name)
      newMessage.setStringProperty("dataType", """(?:.*)data_type=(WGS|EXOME|RNA|N/A)(?:.*)""".r.unapplySeq(it.params.getOrElse("")).map(_.head).getOrElse("WGS"))
      producer.send(newMessage)
      acc+1
    } catch {
      case _: Exception => acc
    }}
    connection.close()
    res
  }

  def itemsToRequests(items: String) = items.split(",").map{ case projectSampleRequestR(p) => p }

  def sampleWorkflow(prod: Boolean) = {
    val sqlTextProd = "INSERT INTO project_sample_requests VALUES(?,?,?,?,SYSDATE,?)"
    val psr = SQL(if (prod) sqlTextProd else sqlTextProd.replace("project_sample_requests", "project_sample_requests_TEST"))

    val myrequestItems = (request_type: Int, items: Array[projectSampleRequest], paramsFunc: String => String) => Try {
      using(ConnectionPool().borrow()) { DB(_).localTx { implicit session =>
        items.foreach{it =>
          val data = Seq(it.project, it.sample, request_type, it.user_name, it.params.map(paramsFunc))
          psr.bind(data: _*).update.apply()
      }}}
    }

    parameters('requestType, 'params_fields.?, 'items) { (requestType, params_fields, items) =>
      respondWithMediaType(MediaTypes.`text/plain`) { complete {
        val triplets = itemsToRequests(items)
        val paramsFunc: String => String = params_fields match {
          case None => params => params
          case Some(fieldsString) => {
            val fields = fieldsString.split(";")
            params => fields.zip(params.split(";")).map(it => s"${it._1}=${it._2}").mkString(";")
          }
        }

        (requestType match {
          case "RequestFinalAggregation" =>
            val res = if (prod) requestFinalAggregation(triplets) else 0
            myrequestItems(1, triplets, paramsFunc)
            s"$res messages sent to JMS queue."

          case rt =>
            val requestType = Seq("", "RequestFinalAggregation", "RequestTopOff", "RequestTopOffGroup", "RequestPoolCreated", "RequestSendToRework", "RequestHoldForTopOff", "RequestWaitingForNewRwData", "RequestReset", "RequestPCHoldForTopOff","RequestVolumeFinal").indexOf(rt)
            if (requestType == -1) throw new RuntimeException("Unknown requestType.")
            myrequestItems(requestType, triplets, paramsFunc)
            ""
        }) +
        triplets.groupBy(_.project).map(it => s"${it._1}[${it._2.size}] -> (${it._2.map(_.sample).mkString(",")})").mkString(s"Total ${triplets.size} items processed:\n", "\n", "")
      }}
    }
  }

  val sampleWorkflowProd = sampleWorkflow(true)
  val sampleWorkflowDev  = sampleWorkflow(false)

  val TigerRoute =
    pathPrefix("api") {
        path("labopsjira" / "generate_sql") {
          parameters('project_name, 'domain) { (project_name, domain) => generateSql(project_name, domain)}
        } ~
        path("launchEtlAgent") {
          parameters('agent_name) { launchEtlRoute }
        } ~
        path("sqlFullText") {
          parameters('sql_id) { sql_id =>
            respondWithMediaType(MediaTypes.`text/html`) { complete {
              val sqlText = utils.CognosDB.apply(Reader { session =>
                implicit val sess = session
                SQL( """SELECT sql_fulltext FROM v$SQL WHERE sql_id=? AND child_number=0""").bind(sql_id).map { it =>
                  scala.io.Source.fromInputStream(it.asciiStream(1)).mkString
                }.single().apply()
              }).getOrElse("Apparently Sql has faded out of Oracle's sqlarea.").toString

              val data =
                Array("Check Time", "Host", "Username", "Machine", "Minutes", "SID_Serial") ::
                  utils.CognosDB.apply(Reader { session =>
                    implicit val sess = session
                    SQL("""
                          |WITH ids AS (
                          |SELECT
                          |LEVEL num,
                          |'<a href="([^"]*)">(\w*)</a>, host: ([^,]*), username: ([^,]*), machine: ([^,]*), minutes: ([^,]*), sid/serial#: (\d*),(\d*)' regex
                          |FROM dual CONNECT BY level<=20)
                          |
                          |SELECT
                          |    --a.id,
                          |    a.start_time check_time,
                          |--    REGEXP_SUBSTR(details, regex, 1, num, '', 1) url,
                          |--    REGEXP_SUBSTR(details, regex, 1, num, '', 2) sql_id,
                          |    REGEXP_SUBSTR(details, regex, 1, num, '', 3) host,
                          |    REGEXP_SUBSTR(details, regex, 1, num, '', 4) username,
                          |    REGEXP_SUBSTR(details, regex, 1, num, '', 5) machine,
                          |    REGEXP_SUBSTR(details, regex, 1, num, '', 6) minutes,
                          |
                          |    REGEXP_SUBSTR(details, regex, 1, num, '', 7) || ',' || REGEXP_SUBSTR(details, regex, 1, num, '', 8) sid_serial
                          |FROM etl_runs a, ids
                          |WHERE
                          |    details LIKE ?
                          |    -- AND a.id IN (1215829)
                          |    AND REGEXP_SUBSTR(details, regex, 1, num, '', 2) = ? -- sql_id
                          |ORDER BY a.start_time desc
                        """.stripMargin).bind(s"%$sql_id%", sql_id).map { it =>
                      Array(it.string(1), it.string(2), it.string(3), it.string(4), it.string(5), it.string(6))
                    }.list().apply()
                  })
              html.sqlFullText.render(sqlText, data, sql_id).toString
            }}
          }} ~
        path("sqlMonitor") {
          parameters('sql_id, 'host.?) { (sql_id, host) =>
            respondWithMediaType(MediaTypes.`text/html`) { complete {
              (host match {
                case Some("holmium.broadinstitute.org") => utils.BspDB
                //case Some("vmseqbldr2.broadinstitute.org") => utils.
                case _ => utils.CognosDB
              }).apply(Reader { session =>
                implicit val sess = session
                sql"SELECT DBMS_SQLTUNE.report_sql_monitor(sql_id => ?, type => 'HTML', report_level => 'ALL') as report from dual".bind(sql_id).map { it =>
                  import java.nio.charset.CodingErrorAction

                  implicit val codec = scala.io.Codec("UTF-8")
                  codec.onMalformedInput(CodingErrorAction.REPLACE)
                  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

                  scala.io.Source.fromInputStream(it.asciiStream(1)).mkString
                }.single().apply()
              }).get
            }}
          }} ~
        path("sampleWorkflow") { sampleWorkflowProd } ~
        path("sampleWorkflowTest") { sampleWorkflowDev } ~
        path("") { getFromResource("") }
    } ~
    pathPrefix("api" / "v2") {
      path("JqlTableauExtractService") {
        parameters('domain.?, 'jqlQuery, 'fields.?, 'tempMax.?, 'agentName.?, 'output.?) { (domain, jqlQuery, fields, tempMax, agentName, output) =>
          output match {
            case Some("XML") => {
              val conf = utils.config.getConfig(s"jira.${domain.get}")
              val rq = jira.request(domain.get, jqlQuery, fields.get, conf.getString("username"), conf.getString("password"))
              ctx => ctx.redirect(new jira.superExtractor(rq).req.url, StatusCodes.Found)
            }
            case _ => extract(identity) { ctx =>
              respondWithMediaType(MediaTypes.`application/octet-stream`) {
                respondWithHeaders(HttpHeaders.`Content-Disposition`("attachment", Map("filename" -> "test.tde"))) {
                  using(ConnectionPool().borrow()) { DB(_).localTx(implicit session => {
                    val details = s"Remote-Address: ${ctx.request.headers.find(_.name=="Remote-Address").get.value}<br>${ctx.request.uri.toString}"
                    println(ctx.request.headers)
                    val id = utils.recordStart(s"analytics.tiger.JqlTableauExtractService.${agentName.getOrElse("Anonymous")}", "", details)
                    try {
                      val conf = utils.config.getConfig(s"jira.${domain.get}")
                      val (file, size) = jira.process.map(utils.toTDE).apply(jira.request(domain.get, jqlQuery, fields.get, conf.getString("username"), conf.getString("password")))
                      val resultRoute = getFromFile(file)
                      utils.recordEnd(id, 0, s"$details<br>records_processed: $size")
                      println("TDE-file GENERATED: " + file.getAbsolutePath)
                      resultRoute
                    } catch { case err: Exception =>
                      utils.recordEnd(id, 1, s"$details<br>${err.getMessage}")
                      utils.sendEmail("atanas@broadinstitute.org", Seq("reportingerrors@broadinstitute.org"), "Jira/JqlTableauExtractService: Error", err.getMessage + (if (err.getMessage.contains("Unexpected response status: 403")) "\n\n403 suggests JIRA likely raised CAPTCHA challenge to 'analytics' user." else ""))
                      throw err
                    }
                  })}
                }}
            }
          }
        }
      } ~
      path("JiraWebConnector") {
        parameters('domain.?, 'jqlQuery, 'fields.?, 'username.?, 'password.?) { (domain, jqlQuery, fields, username, password) =>
          extract(identity) { ctx =>
            respondWithMediaType(MediaTypes.`application/json`) {
              respondWithHeader(HttpHeaders.`Access-Control-Allow-Origin`(AllOrigins)) { complete {
                val conf = utils.config.getConfig(s"jira.${domain.get}")
                val (username0, password0) = (username, password) match {
                  case (Some(user), Some(psw)) => (user, psw)
                  case (Some(user), None) => throw new SecurityException("Password not provided.")
                  case (None, _) =>
                    if (utils.config.getStringList("TrustedComputers").contains(ctx.request.headers.find(_.name=="Remote-Address").get.value)) (conf.getString("username"), conf.getString("password"))
                    else throw new SecurityException("Username/password not provided.")
                }
                jira.process.map(utils.toJson).apply(jira.request(domain.get, jqlQuery, fields.get, conf.getString("username"), conf.getString("password")))
              } } } } }
      } ~
      path("fields_catalog") {
        parameters('domain) { domain => respondWithMediaType(MediaTypes.`text/html`) { complete {
          jira.getFieldsMetadata(domain).sortWith((a,b) => a.name<b.name).map(it => s"<tr><td>${it.name}</td><td>${it.id}</td><td>${it.`type`.getOrElse("")}</td><td>${it.custom.getOrElse("")}</td></tr>").mkString("""<table border=\"1\" bordercolor=\"#000000\" width=\"100%\" cellpadding=\"5\" cellspacing=\"3\"><tr><td>NAME</td><td>ID</td><td>TYPE</td><td>CUSTOM</td></tr>""","\n","</table>")
      } } } } ~
      path("fieldsPicker") {
        parameters('mode, 'domain, 'url.?) { (mode, domain, urlOpt) => respondWithMediaType(MediaTypes.`text/html`) { complete {
          html.fieldPicker.render(
            (mode, urlOpt) match {
            case ("metadata", _) =>
              jira.getFieldsMetadata(domain).sortWith((a,b) => a.name<b.name).map(it => Array(it.name, it.id, it.`type`.getOrElse("")))

            case ("db", _) =>
              regularFields ++
              using(ConnectionPool('labopsjira).borrow()) { DB(_).readOnly(implicit session => {
                sql"""SELECT * FROM jiradwh.cf_catalog WHERE instance=? ORDER BY PKEY, CFNAME""".bind(domain match {
                  case "labopsjira" => "LABOPSJIRA"
                  case "gpinfojira" => "GPINFOJIRA"
                }).map{ it =>
                  Array(
                    s"""(${it.string("PKEY")}) ${it.string("CFNAME")}""",
                    "customfield_" + it.string("ID"),
                    s"""${it.string("CFTYPE")} (${it.string("EXAMPLES")})""")
                }.list().apply()
              })}

            case ("rawXml", Some(urlStr)) =>
              import dispatch._
              import scala.concurrent.ExecutionContext.Implicits.global
              val conf = utils.config.getConfig(s"jira.$domain")
              val req2 = dispatch.url(urlStr).as_!(conf.getString("username"), conf.getString("password"))
              val xml = (Http(req2 OK dispatch.as.xml.Elem)).apply()
              //(xml \ "channel" \ "item").head.child.filterNot(it => it.label=="#PCDATA" || List("component", "customfields").exists(_==it.label)).map(it => Array(it.label, it.label, "")) ++
              // (xml \ "channel" \ "item" \ "customfields").flatMap(_.child.filter(_.label!="#PCDATA").map(it => Array(it \ "customfieldname" text, it \ "@id" text, it \ "@key" text))).groupBy(_(1)).map(_._2.head)
              regularFields ++ (xml \ "channel" \ "item" \ "customfields").head.child.filter(_.label!="#PCDATA").map(it => Array(it \ "customfieldname" text, it \ "@id" text, it \ "@key" text))
          }).toString
        } } } } ~
      path("codeAssistant") {
        parameters('domain.?, 'jqlQuery.?, 'fields.?, 'error.?, 'action.?, 'url.?, 'recordsToPreview.?) { (domainOpt, jqlQueryOpt, fieldsOpt, someerror, action, url, recordsToPreviewOpt) => extract(identity) { ctx => respondWithMediaType(MediaTypes.`text/html`) { complete {

          val recordsToPreview = recordsToPreviewOpt.getOrElse("0").toInt
          val fieldR = """([^:\(]*)(\(([^:]*)\))?(:(.*))?""".r
          val exempt = Map("key" -> "", "timestamp" -> "", "type" -> "", "resolved" -> ":toDate", "comments" -> "")
          case object exemptR { def unapplySeq(s: String) = exempt.keys.mkString("(", "|", ")").r.unapplySeq(s).map(it => Seq(it(0), exempt(it(0)))) }
          (domainOpt, action) match {
            case (_, Some("Decode")) =>
              import scala.collection.JavaConversions._
              val params = dispatch.url(url.get).toRequest.getQueryParams.entrySet().map(it => (it.getKey -> it.getValue.head)).toMap
              html.codeAssistant.render(params("domain"), params("jqlQuery"), params("fields"), "", "", "", "", "", 0).toString

            case (None, _) => html.codeAssistant.render("", "", "", "", "", "", "", "", 0).toString

            case (Some(domain), _) =>
              val fieldsMetadata = jira.getFieldsMetadata(domain)
              val fields = fieldsOpt.get.split(",").map{ case fieldR(fn, _, friendlyName, tran, _) =>
                ( fn.trim,
                  friendlyName,
                  Option(tran).getOrElse(""),
                  fieldsMetadata.find(it => it.id.equalsIgnoreCase(fn.trim) || it.name.equalsIgnoreCase(fn.trim))
                )}.map{
                case (exemptR(fn, tr), fnf, tran, _) => (fn,fnf, tran, Some(jira.FieldMetadata(fn, fn, Some("special"), None)), if (tran!="") tran else tr)
                case (fn,fnf,tran, jf @ Some(jira.FieldMetadata(name,id,Some("number"), _      ))) => (fn,fnf, tran, jf, if (tran!="") tran else ":toDouble")
                case (fn,fnf,tran, jf @ Some(jira.FieldMetadata(name,id,Some(mtype)   , _      ))) if (Seq("date","datetime").exists(_ == mtype)) => (fn,fnf, tran, jf, if (tran!="") tran else ":toDate")
                case (fn,fnf,tran, jf @ Some(jira.FieldMetadata(name,id,Some("string"), Some(_)))) => (fn,fnf, tran, jf, if (tran!="") tran else ":concat")
                case (fn,fnf,tran, jf @ Some(jira.FieldMetadata(name,id,Some(_)       , Some(_)))) => (fn,fnf, tran, jf, if (tran!="") tran else "")

                case (fn,fnf,tran,None) => (fn,fnf, tran, None, ":ERROR")
                case (fn,fnf,tran, jf @ Some(jira.FieldMetadata(name,id,_, _))) => (fn,fnf, tran, jf, if (tran!="") tran else "")
              }
              def escapeLTGT(s: String) = s.replaceAll("<","&lt;").replaceAll(">","&gt;")
              val fieldsCompiled = fields.map{
                case (fn, fnf, _, Some(jira.FieldMetadata(name,id,_,_)),tran) => s"$id(${if (fnf!=null) fnf else name})$tran"
                case (fn, fnf, _, None                                 ,tran) => s"$fn$tran"
              }.mkString(",")
              val comments = fields.map(it => s"""<pre style="color:${if (it._5.contains("ERROR")) "red" else "black"}">${it._1}${escapeLTGT(it._3)} => JiraLookup(${it._4 match { case Some(jf) => s"<b>${jf.`type`.getOrElse("")}</b>, ${jf.custom.getOrElse("Regular")}" case None => "not found"}}) => <b>${
                s"${it._4 match {
                  case Some(jira.FieldMetadata(name,id,_,_)) => s"$id(${utils.urlEncode(if (it._2!=null) it._2 else name)})"
                  case None => it._1}
                }${utils.urlEncode(it._5)}"
              }</b></pre>""").mkString

              val analysis = Try { if (fields.exists(_._5.contains("ERROR"))) throw new RuntimeException("") else ""}
              val conf = utils.config.getConfig(s"jira.$domain")
              def getExtractor(selectedOnly: Boolean, tempMax: Int) = new jira.superExtractor(jira.request(domain, jqlQueryOpt.get, if (selectedOnly) fieldsCompiled else "", conf.getString("username"), conf.getString("password")), tempMax)
              val encodedUrl = s"""http://${InetAddress.getLocalHost.getHostName}:8090/api/v2/JqlTableauExtractService?domain=$domain&jqlQuery=${utils.urlEncode(jqlQueryOpt.get)}&fields=${utils.urlEncode(fieldsCompiled)}"""

              val (wdcsql, msg, htmlTable) =  (for(
                _ <- Try { if (fields.exists(_._5.contains("ERROR"))) throw new RuntimeException("") };
                res <- Try {
                  val sExtractor = getExtractor(true, 100)
                  val JWCurl = s"""http://${InetAddress.getLocalHost.getHostName}:8090/api/v2/JiraWebConnector?domain=$domain&jqlQuery=${utils.urlEncode(jqlQueryOpt.get)}&fields=${utils.urlEncode(fieldsCompiled)}"""
                  val mtd = sExtractor.metadata.zipWithIndex
                  val sql =
                  s"""
                     |-- Connect to 'vmseqbldr2:1521:sqbldr12' as 'REPORTING' and run this SQL. Pls use it wisely.
                     |SELECT
                     |${mtd.map { case (jira.metadata(_, fieldName, extractor), index) =>
                  s"""\t${ extractor match {
                  case _: jira.intExtractor | _: jira.doubleExtractor => s"to_number(col$index)"
                  case _: jira.dateExtractor => s"""to_timestamp(col$index, 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"')"""
                  case _ => s"col$index"
                  }} "$fieldName"${if (fieldName.size>30) "\t/* TOO LONG (>30) column-name */" else "" }"""
                  }.mkString(",\n")
                  }
                     |FROM
                     |(SELECT
                     |  jiradwh.readwebservice('$JWCurl') as jstring
                     |  FROM dual
                     |) jdata,
                     |JSON_TABLE(jstring, '$$.results[*]' COLUMNS (
                     |  ${mtd.map{ case (_, index) => s"""\tcol$index varchar2(4000) PATH '$$[$index]'"""}.mkString(",\n")})) AS jt""".stripMargin

                  val htmlTable = if (recordsToPreview > 0)
                    jira.process.map(response =>
                    s"""<table class="indent ruleTable"><tr>${response.fields.map(it =>
                      s"""<td class="ruleTableHeader">${it.friendlyFieldName}<br>${
                        """(.*)\x24(.*)Extractor""".r.findAllMatchIn(it.extractor.getClass.getName).next.group(2).toUpperCase
                      }</td>"""
                    ).mkString}</tr>\n${response.stream.take(recordsToPreview).map(_.map(it2 => """<td class="ruleTableCell">""" +  escapeLTGT(it2.getOrElse("").toString) + "</td>").mkString("<tr>","","</tr>\n")).mkString }</table>"""
                  ).apply(sExtractor.request)
                  else ""

                  ((sql, sExtractor.exploderField, htmlTable))
                  }
              ) yield res) match {
                case Success((sql, Some(it), htmlTable)) => (sql, s"""<pre style="color:crimson">WARNING: Field "$it" is exploded</pre>""", htmlTable)
                case Success((sql, None, htmlTable)) => (sql, "Exploded field: None", htmlTable)
                case Failure(f) => ("", s"""<pre style="color:red">${escapeLTGT(f.getMessage)}</pre>""", "")
              }
              val rawXmlAllUrl = getExtractor(false, 1).req.url
              html.codeAssistant.render(domain, jqlQueryOpt.get, fieldsCompiled, encodedUrl, rawXmlAllUrl, msg + "\n" + comments, wdcsql, htmlTable, recordsToPreview).toString
          }
        } }
        } } } ~
      path("urlRequest") {
        import dispatch._
        import dispatch.Defaults._
        parameters('url, 'filename.?) { (myurl, filenameOpt) =>
          val url = dispatch.url(myurl)
          val conf = utils.config.getConfig(s"jira.${url.toRequest.getURI.getHost.split("""\.""").head}")
          val req = url.as_!(conf.getString("username"), conf.getString("password"))

          filenameOpt match {
          case Some(filename) =>
            import java.nio.file._
            val dest = Paths.get(s"""\\\\gp-reports\\tableau_files\\UrlRequests\\$filename""")
            (Http(req OK dispatch.as.Response(res =>
              Files.copy(
                res.getResponseBodyAsStream,
                dest,
                StandardCopyOption.REPLACE_EXISTING
              )))
              ).apply()
            val file = dest.toFile
            complete(s"${file.length()} bytes written,  ${new org.joda.time.DateTime(file.lastModified())}")

          case None =>
            complete((Http(req OK dispatch.as.String)).apply())
        }

      }}
    } ~
    path("etl_runs" / "All") { ctx => ctx.redirect("/etl_runs",StatusCodes.Found) } ~
    path("etl_runs") {
      ctx => respondWithMediaType(MediaTypes.`text/html`) { complete {
        val query = ctx.request.message.uri.query
        val statuses  = Try{query.getAll("status" )} match { case Success(st) if (st!=Nil) => st case _ => List("Running", "Abandoned", "Halted", "Succeeded", "Failed") }
        val params = query.toMultiMap
        val group_by_agent = query.get("group_by_agent").getOrElse("off").equalsIgnoreCase("ON")
        val pickedAgent = Try{query.get("agent" )} match { case Success(Some(st)) => st case _ => "All" }
        val show_command = query.get("show_command").getOrElse("off").equalsIgnoreCase("ON")
        //      parameters('show_command.?, 'latest_run_only.?) { (show_command,latest_run_only) =>
        val res = using(ConnectionPool.borrow()) { DB(_).readOnly(implicit session => {
          SQL( s"""
                  |SELECT
                  |  agent_name,
                  |  start_time,
                  |  end_time,
                  |  86400*(nvl(end_time,sysdate)-start_time) duration,
                  |  decode(return_code, NULL, decode(sign(sysdate-1-start_time), -1, 'Running', 'Abandoned'), 5, 'Halted', 0, 'Succeeded', 'Failed') status,
                  |  details,
                  |  command
                  |FROM (
                  |   SELECT a.*, row_number() over(PARTITION BY agent_name ORDER BY start_time DESC) myrank
                  |   FROM etl_runs a
                  |   WHERE start_time>=trunc(sysdate)-5)
                  |WHERE ?='FALSE' OR myrank=1
                  |ORDER BY start_time desc
                """.stripMargin).bind(if (!group_by_agent) "FALSE" else "TRUE")
            .map{ it => {
              val status = it.string("status")
              val details = Option(it.string("details"))
              val end_time = Option(it.timestamp("end_time"))
              val duration = it.int("duration")
              ((status, details) match {
                case ("Succeeded", Some(d)) if d.indexOf("WARNING") > -1 => "orange"
                case ("Succeeded", _) => "white"
                case ("Failed",_) => "red"
                case ("Running",_) => "yellow"
                case ("Abandoned",_) => "aqua"
                case ("Halted",_) => "LightBlue"
              },
                it.string("agent_name"),
                ETL.millisFormatter.print(it.timestamp("start_time").getTime),
                end_time match { case Some(t) => ETL.millisFormatter.print(t.getTime) case _ => ""},
                s"${duration/60} mins, ${duration%60} sec",
                status,
                HtmlFormat.raw(details.getOrElse("")),
                it.string("command")
                )}}.list().apply()
        })}
        val agents = "All" :: res.map(_._2).distinct.sorted
        val counts = res.groupBy(_._6).map(it => it._1 -> it._2.size)
        val statusesHtml = List("Succeeded","Failed","Running","Abandoned","Halted").map(it =>
          new Html(s"""<tr><td><input type="checkbox" name="status" value="$it" ${statuses.find(_==it).map(_ => "checked").getOrElse("")}>$it (${try counts(it) catch{case _ => 0}})</td></tr>""")
        )
        val activity = utils.CognosDB.apply(Reader{session => implicit val sess = session ; sql"SELECT a.agent_name, trunc(sum(24*60*60*(a.end_time-a.start_time))) duration FROM etl_runs a WHERE a.start_time>sysdate-100 AND a.end_time is not null GROUP BY a.agent_name ORDER BY sum(a.end_time-a.start_time) desc".map(row => s"""['${row.string("AGENT_NAME")}', ${row.int("DURATION")}]""" ).list().apply()}).mkString(",\n")
        html.etl_runs.render(agents,statusesHtml,pickedAgent,group_by_agent,show_command,params,res.filter(run => statuses.exists(_==run._6) && (pickedAgent=="All" || pickedAgent==run._2)), activity).toString
      }}.apply(ctx)
    } ~
    path("shutdown") { parameters('immediate.?) { immediate => respondWithMediaType(MediaTypes.`text/html`) {
      complete {
        if (ledger.shuttingDown) "Shutdown has already been requested"
        else {
          ledger.shuttingDown = true
          import scala.concurrent.ExecutionContext.Implicits.global
          Future {
            while (true) {
              val running = using(ConnectionPool.borrow()) {
                DB(_).readOnly(implicit session => {
                  SQL("""
                        |SELECT count(*) FROM
                        |(SELECT a.*,
                        |dense_rank() over(PARTITION BY agent_name ORDER BY a.start_time DESC) myrank
                        |FROM etl_runs a
                        |WHERE a.start_time>SYSDATE-1
                        |) WHERE myrank=1 AND end_time IS null""".stripMargin).map(_.int(1)).single().apply().get
                })
              }
              if (immediate.map(_.equalsIgnoreCase("true")).get || running==0) {
                """powershell -Command "gwmi win32_process | ?{ $_.CommandLine -match \"scala.tools.nsc.CompileServer\" } | foreach { $_.Terminate() }"""" !
                System.exit(0)
              }
              else {
                Boot.log.info(s"$running ETLs still running. Waiting 1 more minute ...")
                Thread.sleep(60*1000)
              }
            }
          }
          "Shutdown initiated"
        }
      }
    }}} ~
    path("peekattable" / Segment) { tableName =>
      parameters('rows_limit.?) { rows_limit =>
        peekAtTableRoute(tableName, rows_limit.getOrElse("100"))
    }} ~
    path("labopsjira" / "BspFreezerLocations" / Rest) { bspFreezerLocations(_) } ~
    path("manage") { authenticate(basicUserAuthenticator(scala.concurrent.ExecutionContext.Implicits.global.prepare)) { authInfo =>
      complete { authInfo.toString }
    }} ~
    path("test") {
      respondWithMediaType(MediaTypes.`application/octet-stream`) {
        respondWithHeader(HttpHeaders.`Content-Disposition`("attachment", Map("filename" -> "test.tde"))) {
          getFromFile( """c:\temp\Jql_Extract_1447359223000.tde""")
    }}} ~
    path(RestPath) { path => getFromResource(path.toString()) }
}
