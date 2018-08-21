package analytics.tiger.agents

/**
  * Created by atanas on 5/30/2017.
  */
import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics.metricType
import analytics.tiger.metrics.metricUtils.Extractor
import dispatch.{Http, url}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalikejdbc.SQL

import scala.util.Success


object GapAutocall {

  def getExtractors(titles: Array[String]) = {
    def get(s: Seq[String], index: Int) = try s(index) catch { case _: IndexOutOfBoundsException => "" }
    val extr = (columnName: String, mt: metrics.metricType.EnumVal) => (titles: Seq[String]) => Extractor(columnName, (columns: Seq[String]) => metrics.metricUtils.recordableValue(get(columns, titles.indexOf(columnName)), mt))
    List(
      extr("Chemistry Plate"                , metricType.String),
      extr("Amp Plate Barcode"              , metricType.String),
      extr("Well"                           , metricType.String),
      extr("Chip Barcode"                   , metricType.String),
      extr("Call Rate"                      , metricType.Double),
      extr("Previous Call Rate"             , metricType.Double),
      extr("Birdsuite Call Rate"            , metricType.Double),
      extr("Reclustered"                    , metricType.Boolean),
      extr("zCalled"                        , metricType.Boolean),
      extr("Last Cluster File"              , metricType.String),
      extr("Zeroed SNPs"                    , metricType.Long),
      extr("HapMap Concordance"             , metricType.Double),
      extr("Het %"                          , metricType.Double),
      extr("p95 Green"                      , metricType.Double),
      extr("p95 Red"                        , metricType.Double),
      extr("Group"                          , metricType.String),
      extr("Project"                        , metricType.String),
      extr("Experiment"                     , metricType.String),
      extr("Chip Type"                      , metricType.String),
      extr("Amp Date"                       , metricType.Timestamp("yyyy-MMM-dd hh:mm a")),
      extr("Autocall Date"                  , metricType.Timestamp("yyyy-MMM-dd hh:mm a")),
      extr("Image Date"                     , metricType.Timestamp("yyyy-MMM-dd hh:mm a")),
      extr("Scanner Name"                   , metricType.String),
      extr("Sample Aliquot Id"              , metricType.String),
      extr("Root Sample Id"                 , metricType.String),
      extr("Stock Sample Id"                , metricType.String),
      extr("Collaborator Sample Id"         , metricType.String),
      extr("Participant Id"                 , metricType.String),
      extr("Collaborator Participant Id"    , metricType.String),
      extr("Infinium Gender"                , metricType.String),
      extr("Sequenom Fingerprint Gender"    , metricType.String),
      extr("Fluidigm Fingerprint Gender"    , metricType.String),
      extr("Reported Gender"                , metricType.String),
      extr("Gender Concordance Result"      , metricType.String),
      //      "Num Disconcordant Called FP SNPs",
      extr("Num Discordant Called FP SNPs"  , metricType.Long),
      extr("Num Common Called FP SNPs"      , metricType.Long),
      extr("Num Called Infinium SNPs"       , metricType.Long),
      extr("Num Called Fingerprint SNPs"    , metricType.Long),
      extr("Chip Location"                  , metricType.String),
      extr("User"                           , metricType.String),
      extr("PDO"                            , metricType.String),
      extr("Volume (uL)"                    , metricType.Double),
      extr("Concentration (ng/uL)"          , metricType.Double)
    ).map(_(titles)) ++
      List(
        Extractor("", (_:Seq[String]) => Success(new DateTime)),  // "TIMESTAMP"
        Extractor("", (_:Seq[String]) => Success(new DateTime))   // "FIRST_SEEN_DATE"
      )
  }

  val GapAutocallAgent: ETL.etlType[DaysDelta] = delta => session => {
    implicit val sess = session
    val (d1, d2) = delta.unpack
    val dtf = DateTimeFormat.forPattern("YYYY-MMM-dd")
    val cfg = utils.config.getConfig("GapAutocall")
    val req = url(cfg.getString("url")).as(cfg.getString("username"), cfg.getString("password")).addQueryParameter("begin_date", dtf.print(d1)).addQueryParameter("end_date", dtf.print(d2.minusDays(1))) <:< Map("Accept" -> "application/xml")
    var count = 0
    var extractors: List[Extractor] = null
    val mergeStatement = SQL(utils.generateMERGE("GAP_AUTOCALL_DATA", List("FIRST_SEEN_DATE"))(session))
    import dispatch._
    import Defaults._
    Http(req > as.stream.Lines{ line =>
      count match {
        case 0 => extractors = getExtractors(line.split("\t"))
        case 1 => // ignore
        case _ =>
          val columns = line.split("\t")
          val ex = extractors.map(_.func(columns).getOrElse(None))
          mergeStatement.bind(ex: _*).executeUpdate().apply()
      }
      count+=1
    }).apply()
    Seq((delta, Right(s"${if (count==0) "No" else count-2} entries imported: ${req.toRequest.getUrl}")))
  }

  val agentName = utils.objectName(this)
  val refreshWindowSize = 6 // in Days

  def main(args: Array[String]) {
    val etlPlan = for (
    //delta <- new DeltaProvider(loadDaysDeltaFromDB(agentName)).map(d => d.copy(start = d.start-refreshWindowSize, size = d.size+refreshWindowSize)) ;
      delta <- DaysDelta.loadFromDb(agentName) map DaysDelta.pushLeft(refreshWindowSize);
      plan <- prepareEtl(agentName, delta, GapAutocallAgent)(chunkSize = 7)
    ) yield plan

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    println(res)
    Http.shutdown()
  }

}
