package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics.metricUtils.Extractor
import dispatch.{Http, url}
import org.joda.time.format.DateTimeFormat
import scalikejdbc.SQL
import analytics.tiger.metrics.metricType

import scala.util.Try

/**
  * Created by atanas on 9/12/2017.
  */
object FluidigmPlate {

  val extr = (columnName: String, mt: metrics.metricType.EnumVal) => (titles: Seq[String]) => Extractor(columnName, (columns: Seq[String]) => metrics.metricUtils.recordableValue(Try(columns(titles.indexOf(columnName))).getOrElse(""), mt))
  val tmstmp = metricType.Timestamp("yyyy-MM-dd HH:mm:ss.SSS")

  def getExtractors(titles: Array[String]) = List(
    extr("DNA_Plate", metricType.String),
    extr("STA_Plate", metricType.String),
    extr("Barcode", metricType.String),
    extr("Chip_Run", metricType.String),
    extr("Status", metricType.String),
    extr("Platform", metricType.String),
    extr("Workflow", metricType.String),
    extr("Received_From_BSP", tmstmp),
    extr("STA_Plate_Creation_Date", tmstmp),
    extr("Latest_Open_Step", metricType.String),
    extr("Step_Start_Date", tmstmp),
    extr("Created_By", metricType.String),
    extr("Uploaded_Date", tmstmp),
    extr("Number_Of_Samples", metricType.Long),
    extr("Num_Hap_Map_Samples", metricType.Long),
    extr("Std_Call_Rate", metricType.Double),
    extr("Q17_Call_Rate", metricType.Double),
    extr("Q20_Call_Rate", metricType.Double),
    extr("Group", metricType.String),
    extr("Project", metricType.String),
    extr("Experiment", metricType.String),
    extr("FP_Panel_Version", metricType.String),
    extr("PassFail_Date", tmstmp)
  ).map(_(titles))

  val delete = SQL("DELETE FROM cognos.gap_fluidigm_plate WHERE PLATFORM=? and CHIP_RUN=? and BARCODE=?")
  val insert = SQL("INSERT INTO cognos.gap_fluidigm_plate VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

  val agent: ETL.etlType[DaysDelta] = delta => session => {
    implicit val sess = session
    val (d1, d2) = delta.unpack
    val dtf = DateTimeFormat.forPattern("dd-MMM-YYYY")
    val cfg = utils.config.getConfig("GapAutocall")

    import dispatch._
    import Defaults._
    import com.ning.http.client.Response
    val req = url(cfg.getString("fluidigm_plate_url")).as(cfg.getString("username"), cfg.getString("password")).addQueryParameter("begin_date", dtf.print(d1)).addQueryParameter("end_date", dtf.print(d2.minusDays(1))) <:< Map("Accept" -> "application/xml")
    object Streamer extends (Response => Seq[String]) {
      def apply(r: Response) = scala.io.Source.fromInputStream(r.getResponseBodyAsStream).getLines().toSeq
    }
    val res = Http(req OK Streamer).apply
    val entries = res.headOption match {
      case Some(h) =>
        val titles = h.substring(1).split("\t")
        val extractors = getExtractors(titles)
        val data = res.filterNot(_.startsWith("#")).map{it => val d = it.split("\t") ; extractors.map{_.func(d).toOption}}
        data.foreach{ line =>
          delete.bind(line(5), line(3), line(2)).executeUpdate().apply()
          insert.bind(line: _*).executeUpdate().apply()
        }
        data.size
      case None => 0
    }

    Seq((delta, Right(s"$entries entries imported: ${req.toRequest.getUrl}")))
  }

  val agentName = utils.objectName(this)
  val refreshWindowSize = 6 // in Days

  def main(args: Array[String]) {
    val etlPlan = for (
      delta <- DaysDelta.loadFromDb(agentName) map DaysDelta.pushLeft(refreshWindowSize);
      plan <- prepareEtl(agentName, delta, agent)(chunkSize = 7)
    ) yield plan

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    println(res)
    Http.shutdown()
  }

  /* run it manually like this
import analytics.tiger.ETL._
import analytics.tiger.agents.FluidigmPlate
import analytics.tiger.utils
val etlPlan = prepareEtl(FluidigmPlate.agentName, delta, FluidigmPlate.agent)(chunkSize = 7)
val res = utils.CognosDB.apply(etlPlan)
  */

}
