import java.net.URL
import java.sql.Timestamp
import analytics.tiger.ETL._
import analytics.tiger.{DaysDelta, ETL, etlMessage, utils}
import org.joda.time.format.DateTimeFormat
import scalikejdbc._
import scala.xml.XML

val deckEventsAgent: ETL.etlType[DaysDelta] = delta => session => {
  val (d1, d2) = delta.unpack
  val df = DateTimeFormat.forPattern("yyyy/MM/dd")
  val df2 = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ")
  val url = new URL(s"http://squid-ui.broadinstitute.org:8000/squid/resources/workflowevent/query?eventDate=${df.print(d1)}")
  //conn.setRequestProperty("Accept", "application/xml")
  val xml = XML.load(url.openConnection().getInputStream)
  sql"DELETE FROM LIMS_DECK_EVENT WHERE ?<=event_start_time AND event_start_time<?".bind(d1, d2).executeUpdate().apply()(session)
  val res = (xml \ "deckEventBean").map(item =>
    try {
      val <deckEventBean><deckName>{deckName}</deckName><eventStartTime>{eventStartTimeStr}</eventStartTime><eventType>{eventType}</eventType><lcSet>{lcSet}</lcSet><sampleBarcode>{sampleBarcode}</sampleBarcode><stationEventId>{stationEventId}</stationEventId><workflowName>{workflowName}</workflowName></deckEventBean> = item
      sql"INSERT INTO LIMS_DECK_EVENT VALUES(?,?,?,?,?,?,?)".bind(
        stationEventId.text.toInt,
        sampleBarcode.text,
        deckName.text,
        df2.parseDateTime(eventStartTimeStr.text),
        eventType.text,
        lcSet.text,
        workflowName.text
      ).executeUpdate().apply()(session)
      Right()
    } catch {
      case e: Exception => Left(etlMessage(s"$url\n${e.getMessage()}\n${item.buildString(true)}", Seq("thompson@broadinstitute.org")))
    }
  )
  if (res.exists(_.isLeft)) Seq((delta, Left(etlMessage(res.filter(_.isLeft).mkString("\n\n")))))
  else Seq((delta, Right(s"OK, events processed: ${res.size}")))
}

val agentName = "analytics.tiger.LimsDeckEventsAgent"
val etlPlan = for (
  delta <- DaysDelta.loadFromDb(agentName);
  plan <- prepareEtl(agentName, delta, deckEventsAgent)(chunkSize = 1)
) yield plan
val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
