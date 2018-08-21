package analytics.tiger.agents

import java.net.URL
import java.sql.{SQLIntegrityConstraintViolationException, Timestamp}

import analytics.tiger.ETL._
import analytics.tiger.{utils, _}
import org.joda.time.DateTime
import scalikejdbc._

/**
  * Created by atanas on 5/30/2017.
  */
object LimsAncestryEvent {

  case class ancestryItem(domainId:Int, subjectType:String, triggers: Seq[String])

  val items = Seq(
    ancestryItem(1, "Enriched Pond"						, Seq("Enriched Library", "Denatured Library", "Enriched Catch",
      "Size Selected Enriched Library" )),

    ancestryItem(10, "Enriched Pond" 						, Seq("cDNA Enriched Library", "Denatured Library", "Enriched Catch",
      "Enriched Library", "External Library", "Normalized Library", "Pooled Normalized Library",
      "PCR Product Pool", "Size Selected Enriched Library", "TSCA Library")),

    ancestryItem(15, "SPRI Concentrated"                   , Seq("Denatured Library", "Normalized Library")),
    ancestryItem(20, "Size Selected Enriched Library"      , Seq("Denatured Library", "Normalized Library","Pooled Normalized Library")),

    ancestryItem(25, "Pooled Normalized Library"           , Seq("Denatured Library", "Normalized Library", "Pooled Normalized Library")),
    ancestryItem(30, "Nextera Enriched Library"            , Seq("Denatured Library", "Normalized Library", "Nextera Pooled Normalized Library")),
    ancestryItem(35, "Nexome Catch"                        , Seq("Denatured Library", "Normalized Library")),
    ancestryItem(40, "Nextera Pooled Normalized Library"   , Seq("Denatured Library", "Normalized Library")),
    ancestryItem(45, "Nextera SPRI Concentrated Pool"      , Seq("Denatured Library", "Normalized Library")),
    ancestryItem(50, "PCR-Free Pond"      				 , Seq("Denatured Library", "Normalized Library", "Pooled Normalized Library")),
    ancestryItem(55, "PCR-Plus Pond"      				 , Seq("Denatured Library", "Normalized Library", "Pooled Normalized Library", "PCR-Plus Norm Pond")),
    ancestryItem(70, "PCR-Plus Norm Pond"      			 , Seq("Denatured Library", "Normalized Library", "Pooled Normalized Library")),

    ancestryItem(60, "cDNA Enriched Library" 				 , Seq("Denatured Library", "Normalized Library", "Pooled Normalized Library", "Enriched Library")),

    ancestryItem(5, "Enriched Catch"                       , Seq("Denatured Library", "Normalized Library", "Enriched Library")),
    ancestryItem(6, "Plex Pond"                            , Seq("Denatured Library", "Normalized Library", "Enriched Library")),
    ancestryItem(65, "Normalized Library"           		 , Seq("Denatured Library"))


  )

  def etlItem(domainId:Int, subjectType:String) = (delta: DiscreteDelta[Int], session: DBSession) => {
    implicit val sess = session
    val targets = delta.elements
    val url = new URL(s"http://squid-ui.broadinstitute.org:8000/squid/resources/library/query?relativeType=ancestor&relatedLibraryType=${subjectType.replace(" ", "%20")}${targets map ("&libraryId=" + _) mkString}")
    println(url.toString)
    //conn.setRequestProperty("Accept", "application/xml")
    val xml = scala.xml.XML.load(url.openConnection().getInputStream)
    val now = new Timestamp(DateTime.now().getMillis)
    val prep_stmt = sql"INSERT INTO LIMS_ANCESTRY_EVENT VALUES(?,?,?,?,?)"
    val results = (xml \ "libraryBean").foldLeft((List[Int](), List[Either[etlMessage, etlMessage]](), List[Either[etlMessage, etlMessage]]()))((acc, item) =>
      try {
        val <libraryBean><libraryId>{libraryIdStr}</libraryId><libraryName>{libraryName}</libraryName><queryLibraryId>{queryLibraryId}</queryLibraryId><queryLibraryName>{queryLibraryName}</queryLibraryName></libraryBean> = item
        val libraryId = libraryIdStr.text.toInt
        prep_stmt.bind(domainId, libraryId, queryLibraryId.text.toInt, 1, now).executeUpdate().apply()(session)
        acc.copy(_1 = libraryId :: acc._1)
      } catch {
        case e: SQLIntegrityConstraintViolationException => acc.copy(_3 = Right(etlMessage(s"${e.getMessage()}, ${item.buildString(true)}")) :: acc._3)
        case e: Exception => acc.copy(_2 = Left(etlMessage(s"${e.getMessage()}, ${item.buildString(true)}")) :: acc._2)
      }
    )
    val prefix = s"domainId: $domainId, subjectType: $subjectType, libs processed: ${targets.size}\n$url\n"
    Seq((delta,
      if (!results._2.isEmpty) aggregateResults(prefix, results._2)
      else {
        (targets.toSet -- results._1).foreach(libraryId =>
          try prep_stmt.bind(domainId, libraryId, libraryId, 0, now).executeUpdate().apply()(session)
          catch { case e: SQLIntegrityConstraintViolationException => }
        )
        aggregateResults(prefix, results._3)
      }
    ))
  }

  def millisToLibConvertor(triggers: Seq[String])(a: MillisDelta, session: DBSession): DiscreteDelta[Int]= {
    implicit val sess = session
    val (d1, d2) = a.unpack
    val res = sql"SELECT library_id FROM slxre_library WHERE $d1<=creation_time AND creation_time<$d2 AND type IN ($triggers)".map(_.int(1)).list.apply()
    DiscreteDelta(res.toSet)
  }

  val agentName = utils.objectName(this)
  val etlAgent: ETL.etlType[MillisDelta] = delta => session => {
    val res = items.map{ case ancestryItem(domainId, subjectType, triggers) =>
      val chunkedDelta = ETL.discreteHandler.chunk(millisToLibConvertor(triggers)(delta, session), 100)
      if (chunkedDelta.size==0) Right(s"domainId: $domainId, subjectType: $subjectType => No Data Accumulated")
      else aggregateResults("", chunkedDelta.flatMap(etlItem(domainId, subjectType)(_, session).map(_._2)))
    }
    Seq((delta, aggregateResults("", res)))
  }

  def main(args: Array[String]) {
    val etlPlan = for (
      delta <- MillisDelta.loadFromDb(agentName);
      plan <- prepareEtl(agentName, delta, etlAgent)()
    ) yield plan
    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
  }

}
