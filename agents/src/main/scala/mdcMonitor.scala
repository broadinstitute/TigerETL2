package analytics.tiger.agents

/**
  * Created by atanas on 10/26/2017.
  */

import dispatch._
import Defaults._
import analytics.tiger.utils
import analytics.tiger.Reader
import com.gargoylesoftware.htmlunit.{BrowserVersion, WebClient}
import com.gargoylesoftware.htmlunit.html.{HtmlPage, HtmlPasswordInput, HtmlSubmitInput, HtmlTextInput}
import com.typesafe.scalalogging.LazyLogging
import scalikejdbc._

import scala.util.{Failure, Success, Try}

object mdcMonitor extends LazyLogging {

  /**

  def getTicketData(url: String, username: String, password: String)(ticket: String, fields: Seq[String]) = {
    val req = dispatch.url(s"$url?tempMax=1&${fields.map("field=" + _).mkString("&")}&jqlQuery=key%3D$ticket&").as_!(username, password)
    utils.loadXML((Http(req OK as.String)).apply())
  }

  def load_bsp_export_summaryOld(ticket: String) {
    val conf = utils.config.getConfig(s"jira.gpinfojira")
    val req = dispatch.url(s"${conf.getString("url")}?tempMax=1&field=attachments&jqlQuery=key%3D$ticket&").as_!(conf.getString("username"), conf.getString("password"))
    val doc = utils.loadXML((Http(req OK as.String)).apply())
    val data = (doc \\ "attachment").find(it => (it \ "@name").text.endsWith("export_summary.csv")).map{ node =>
      val req = dispatch.url(s"""https://gpinfojira.broadinstitute.org/jira/secure/attachment/${node \ "@id" text}/${ticket}_export_summary.csv""").as_!(conf.getString("username"), conf.getString("password"))
      (Http(req > as.Response.apply(r => scala.io.Source.fromInputStream(r.getResponseBodyAsStream).getLines.toList))).apply().tail.map(_.split(",")).filterNot(_.isEmpty)
    }
    Http.shutdown()
    data.foreach(dataList =>
      utils.CognosDB.apply(Reader{ implicit session =>
        sql"DELETE FROM MDCM_LEDGER WHERE ticket=?".bind(ticket).update().apply()
        dataList.foreach(line =>
          sql"INSERT INTO MDCM_LEDGER VALUES(?,?,?,?,?,?,?,?,?)".bind(ticket :: line.toList: _*).update().apply()
        )})
    )
  }

import analytics.tiger.utils
val conf = utils.config.getConfig(s"jira.broadinstitute.atlassian.net")
val getCloudTicketData = analytics.tiger.agents.Operations.getTicketData(conf.getString("url"), conf.getString("username"), conf.getString("password")) _
val xml = getCloudTicketData("PO-10435", Seq("comments"))

import dispatch._
import Defaults._
val req = dispatch.url(s"https://picard.broadinstitute.org/pipeline/workflows/viewWorkflow/13705624")
val xml = utils.loadXML((Http(req OK as.String)).apply())
    */

  val webClient = {
    val w = new WebClient(BrowserVersion.BEST_SUPPORTED)
    w.waitForBackgroundJavaScript(30 * 1000)
    val options = w.getOptions()
    options.setJavaScriptEnabled(true)
    options.setThrowExceptionOnScriptError(false)
    options.setThrowExceptionOnFailingStatusCode(false)
    //options.setPrintContentOnFailingStatusCode(false)
    w
  }

  //webClient.close()

  def getPicardStatusXml(url: String) = {
    val htmlPage: HtmlPage = webClient.getPage(url)
    //logger.info(s"Connect to $url")
    Try{
      val form = htmlPage.getForms.get(0)
      val usernameInput: HtmlTextInput = form.getInputByName("username")
      val passwordInput: HtmlPasswordInput = form.getInputByName("password")
      usernameInput.setValueAttribute(utils.config.getString("ldap_user.username"))
      passwordInput.setValueAttribute(utils.config.getString("ldap_user.password"))
      val button: HtmlSubmitInput = form.getInputByName("doLogin")
      val res = button.click[HtmlPage]()
      logger.info("Authentication completed !")
      res
    }.recover{ case _ => htmlPage }.map(it => scala.xml.XML.loadString(it.asXml()))
  }

  case class PicardStatus(
    summary: Map[String, String],
    runs: Seq[Seq[String]],
    events: Seq[Seq[String]],
    steps: Seq[(String, String)]
  )

  def getPicardStatus(url: String) = for {
    doc <- getPicardStatusXml(url);

    res <- (doc \\ "table").filter(it => (it \ "@class").text == "viewItems").headOption match {
      case None => Failure(new RuntimeException(doc text /*doc \ "body" \ "p" text*/))
      case Some(tab) => Try {
        val summary = (tab \\ "tr").drop(1).map { row =>
          val Seq(label, value) = (row \\ "td").map(_.text.trim)
          label.substring(0, label.size - 1) -> value
        }.toMap

        def description(tab: Int) = (doc \\ "table").filter(it => (it \ "@class" text) == "wfDescription")(tab) \\ "tr" drop (2) map {
          _ \ "td" map (_.text.trim)
        }

        val runs = description(0)
        val events = description(1)
        val steps = ((doc \\ "div").filter(it => (it \ "@id").text == "stepTree") \\ "span").filter(it => !(it \ "@step-id").isEmpty).map(it => (it \ "@step-name").text -> (it \ "@step-status").text)
        val workflowR = """(.*) \((.*)\)""".r
        val Some(List(workflowType, workflowSubject)) = workflowR.unapplySeq("""Illumina Run Reanalysis Workflow (HL7NLBBXX)""")
        PicardStatus(
          summary + ("workflowType" -> workflowType) + ("workflowSubject" -> workflowSubject),
          runs,
          events,
          steps
        )
      }
    }
  } yield res


  val workflowUrl = """https://picard.broadinstitute.org/pipeline/workflows/viewWorkflow/"""
  val workflowRegex = (workflowUrl + "\\d*").r

  def getPoTicketData(ticket: String) = {
    val columns = Seq("BSP Sample ID", "GSSR ID", "Project(s)", "Old Collaborator Sample ID", "New Collaborator Sample ID", "Old Individual Alias", "New Individual Alias", "Sample LSID", "Old Project", "New Project", "Root Sample ID")
    val conf = utils.config.getConfig(s"jira.broadinstitute.atlassian.net")
    val (username, password) = (conf.getString("username"), conf.getString("password"))
    val dtformat = org.joda.time.format.DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss Z")
    for{
      xml <- Http(dispatch.url(s"${conf.getString("url")}?tempMax=1&field=comments&field=attachments&field=created&jqlQuery=key%3D$ticket").as_!(username, password) OK as.String).map(utils.loadXML)

      summaryLocation <- Http{
        (xml \\ "attachment").find(it => (it \ "@name").text.endsWith("export_summary.csv")) match {
          case Some(node) => dispatch.url(s"""https://broadinstitute.atlassian.net/secure/attachment/${node \ "@id" text}/${(node \ "@name").text}""").as_!(username, password) > as.Response{ _.getHeader("Location") /* HTTP 302 Found */ }
          case None => throw new RuntimeException("%export_summary.csv file missing")
        }}

      summary <- Http(url(summaryLocation) > as.Response{ r => scala.io.Source.fromInputStream(r.getResponseBodyAsStream).getLines.map(_.split(",")).filterNot(_.isEmpty).toList})
    } yield (
      dtformat.parseDateTime(xml \\ "created" text),
      {
        val titles = summary.head
        val indexes = columns.map(titles.indexOf)
        summary.tail.map{ it => indexes.map(ind => Try(it(ind)).getOrElse(None))}
      },
      (xml \\ "comment").flatMap { it => workflowRegex.findAllMatchIn(utils.stripHtml(it text)).map(_.toString.substring(workflowUrl.size).toInt) }
    )

  }

  implicit def strToTickets(ticketsStr: String) = """PO-\d*""".r.findAllMatchIn(ticketsStr).map(_.toString).toList

  // load bsp summary.csv, samples, workflows
  def register(tickets: Seq[String]): Unit = utils.CognosDB.apply(Reader{ implicit session =>
    tickets.foreach { ticket =>
    val (created, samplesData, workflowIDs) = getPoTicketData(ticket).apply()
    logger.info(samplesData.map(_.mkString(", ")).mkString("SAMPLES:\n", "\n", ""))
    logger.info(workflowIDs.mkString("WORKFLOW IDs\n", "\n", ""))
      sql"DELETE FROM mdcm_tickets WHERE ticket=?".bind(ticket).update().apply()
      sql"INSERT INTO mdcm_tickets VALUES(?,sysdate,'IN ANALYSIS',?)".bind(ticket,created).update().apply()
      sql"INSERT INTO mdcm_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?)".batch(samplesData.map(ticket :: _.toList): _*).apply()
/*
      workflowIDs.foreach(
        sql"INSERT INTO mdcm_workflows VALUES(?,?)".bind(ticket, _).update().apply()
      )
*/
    }

// register flowcells involved
sql"""
INSERT INTO mdcm_flowcells
SELECT DISTINCT s.ticket, mtd.flowcell_barcode
FROM mdcm_samples s
JOIN  slxre_readgroup_metadata mtd ON mtd.collaborator_sample_id=s.old_collaborator_sample_id /* AND mtd.root_sample=s.root_sample_id*/
WHERE s.ticket IN ($tickets)""".update().apply()

// register zamboni per-flowcell workflows
sql"""
INSERT INTO mdcm_workflows
SELECT t.ticket, wf.id
FROM zamboni.workflow wf
JOIN mdcm_flowcells fc ON fc.flowcell_barcode = REGEXP_SUBSTR(NAME, '[^\(]+[^\)]',1,2)
JOIN mdcm_tickets t ON t.ticket = fc.ticket
WHERE t.ticket IN ($tickets) AND parent_step_id IS NULL AND wf.created_at > t.created_at
""".update().apply()

    //Http.shutdown()
  })

  def registerAggWorkflows(): Unit = utils.CognosDB.apply(Reader{ implicit session =>
    // find tickets which have completed all fc-workflows
    val tickets = sql"SELECT a.ticket FROM mdcm_workflow_status a WHERE a.status='IN ANALYSIS' AND a.succeeded_workflows = a.total_workflows".map(_.string(1)).list().apply()

// register zamboni aggregation per-project/sample workflows
sql"""
INSERT INTO mdcm_workflows
SELECT DISTINCT t.ticket, wf.id
FROM zamboni.workflow wf
JOIN mdcm_samples s ON INSTR(wf.NAME, '(' || s.new_project || '.' || s.new_collaborator_sample_id || ')') >0
JOIN mdcm_tickets t ON t.ticket = s.ticket
WHERE parent_step_id IS NULL AND wf.created_at > t.created_at and
t.ticket IN ($tickets)
""".update().apply()

    // advance ticket status
    sql"UPDATE mdcm_tickets SET status = 'IN AGGREGATION ANALYSIS' WHERE ticket IN ($tickets)".update().apply()
  })

  def update_zamboni_details: Unit = utils.CognosDB.apply(Reader{ implicit session =>
    var count = 0
    val list = SQL("""
          |SELECT DISTINCT w.ticket, w.workflow_id
          |FROM mdcm_tickets t
          |JOIN mdcm_workflows w ON w.ticket = t.ticket
          |LEFT JOIN mdcm_workflows_details dt ON dt.workflow_id=w.workflow_id AND dt.wkey='Status'
          |WHERE t.status='IN ANALYSIS' AND (dt.wvalue NOT IN ('SUCCEEDED','FAILED') OR dt.wvalue IS NULL)
       """.stripMargin).map(it => (it.string(1), it.int(2))).list().apply()
    list.foreach{ case (_, workflow_id) =>
      getPicardStatus(workflowUrl + workflow_id) match {
        case Success(PicardStatus(summary, runs, events, steps)) =>
          sql"DELETE FROM mdcm_workflows_details WHERE workflow_id=?".bind(workflow_id).update().apply()
          summary.foreach{ case (key, value) =>
            sql"INSERT INTO mdcm_workflows_details VALUES(?,?,?)".bind(workflow_id, key, value).update().apply()
          }
        //case Failure(e) =>
      }
      count += 1
    }
    val tickets = list.map(_._1).distinct
    var tickets_analyzed = List.empty[String]
    if (!tickets.isEmpty) {
      sql"UPDATE mdcm_tickets SET workflows_details_updated_at=sysdate WHERE ticket IN ($tickets)".update().apply()
/*
      tickets_analyzed = sql"""
SELECT t.ticket
FROM mdcm_tickets t
LEFT JOIN mdcm_workflows w ON w.ticket=t.ticket
LEFT JOIN mdcm_workflows_details dt ON dt.workflow_id=w.workflow_id AND dt.wkey = 'Status'
WHERE t.status='IN ANALYSIS' AND t.ticket IN ($tickets)
GROUP BY t.ticket
HAVING SUM(DECODE(dt.wvalue, 'SUCCEEDED', 1, 0)) = COUNT(*)""".map(_.string(1)).list().apply()
*/
      sql"UPDATE mdcm_tickets SET status = 'ANALYSIS COMPLETED' WHERE ticket IN ($tickets)".update().apply()
    }
    //logger.info(s"WORKFLOWS updated: $count\nTICKETS updated: ${tickets.size}\nTICKETS completed analysis(all workflows): ${tickets_analyzed.size} ${tickets_analyzed.mkString}")
  })

}

/*
getPicardStatus("""https://picard.broadinstitute.org/pipeline/workflows/viewWorkflow/13705624""")
val req = dispatch.url(s"""https://broadinstitute.atlassian.net/secure/attachment/42444/SUPPORT-3420_export_summary.csv""").as_!(conf.getString("username"), conf.getString("password"))
*/

/*
-- Samples to be DMRefreshed (workflows succeeded) --
WITH workflows_per_ticket AS (
SELECT
    t.ticket,
    SUM(DECODE(st.wvalue, 'SUCCEEDED', 1, 0)) succeeded_workflows,
    COUNT(*) total_workflows
FROM mdcm_tickets t
LEFT JOIN mdcm_workflows w ON w.ticket=t.ticket
LEFT JOIN mdcm_workflows_status st ON st.workflow_id=w.workflow_id AND st.wkey = 'Status'
WHERE t.status<>'CLOSED'
GROUP BY t.ticket
)
SELECT sm.*
FROM workflows_per_ticket wpt
JOIN mdcm_samples sm ON sm.ticket=wpt.ticket
WHERE wpt.succeeded_workflows = wpt.total_workflows


"PO-10541,PO-10435,PO-10653,PO-10654".split(",").foreach(analytics.tiger.agents.mdcMonitor.init)
analytics.tiger.agents.mdcMonitor.update_zamboni_status
*/