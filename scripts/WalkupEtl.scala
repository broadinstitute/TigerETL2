import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics._
import analytics.tiger.metrics.metricUtils._
import scalikejdbc._

import scala.util.{Success, Failure}

val getFolderFunc = (groupId: Seq[Extractor], session: DBSession) => {
  implicit val sess = session
  val runName = groupId.head.func(Seq()) match { case Success(run: String) => run }
  val instrument = sql"SELECT instrument FROM slxre2_organic_run WHERE run_name=?".bind(runName).map(_.string(1)).single().apply().get
  scala.reflect.io.File.apply(rootFolder / "proc" / instrument / runName / "Data/Intensities/BaseCalls").toDirectory
}

val mytasks = Seq(
  metricTask(
    "basecalling",
    Seq(
      ("LANE"                         , metricType.Long),
      ("MOLECULAR_BARCODE_SEQUENCE_1" , metricType.String),
      ("MOLECULAR_BARCODE_NAME"       , metricType.String),
      ("TOTAL_BASES"                  , metricType.Long),
      ("PF_BASES"                     , metricType.Long),
      ("TOTAL_READS"                  , metricType.Long),
      ("PF_READS"                     , metricType.Long),
      ("TOTAL_CLUSTERS"               , metricType.Long),
      ("PF_CLUSTERS"                  , metricType.Long),
      ("MEAN_CLUSTERS_PER_TILE"       , metricType.Long),
      ("SD_CLUSTERS_PER_TILE"         , metricType.Long),
      ("MEAN_PCT_PF_CLUSTERS_PER_TILE", metricType.Double),
      ("SD_PCT_PF_CLUSTERS_PER_TILE"  , metricType.Double),
      ("MEAN_PF_CLUSTERS_PER_TILE"    , metricType.Double),
      ("SD_PF_CLUSTERS_PER_TILE"      , metricType.Double)
    ),
    6,
    "\t",
    "WALKUP_BASECALLING_RAW",
    """basecalling.(\d).metrics""".r,
    getFolderFunc
  ),

  metricTask(
    "phasing",
    Seq(
      ("LANE"             , metricType.Long),
      ("TYPE_NAME"        , metricType.String),
      ("PHASING_APPLIED"  , metricType.Double),
      ("PREPHASING_APPLIED", metricType.Double)
    ),
    6,
    "\t",
    "WALKUP_PHASING_RAW",
    """(.*)illumina_phasing_metrics""".r,
    getFolderFunc
  ),

  metricTask(
    "density",
    Seq(
      ("LANE"           , metricType.Long),
      ("CLUSTER_DENSITY", metricType.Double)
    ),
    6,
    "\t",
    "WALKUP_DENSITY_RAW",
    """(.*)illumina_lane_metrics""".r,
    getFolderFunc
  )

)

val Q30Handler = (groupId: Seq[Extractor], session: DBSession) => {
  val fieldExtractors = Seq(
    Extractor("READ", it => recordableValue(it(0), metricType.Long)),
    Extractor("LANE", it => Success(it(1))),  // just String
    Extractor("Q30" , it => recordableValue(it(2), metricType.Double))
  )
  val tableName = "WALKUP_Q30_RAW"
  val extractors = groupId ++ fieldExtractors

  val plan = IO {
    var req = ""
    try {
      val runName = groupId.head.func(Seq()) match {case Success(v) => v}
      implicit val sess = session
      val instrument = sql"SELECT instrument FROM slxre2_organic_run WHERE run_name=?".bind(runName).map(_.string(1)).single().apply().get
      req = s"http://illuminadashboard/illumina/automation/jersey/interop/q30?cycleCutoff=1&runPath=/seq/illumina/proc/$instrument/$runName"
      val xml = scala.xml.XML.load(req)
      deleteStatement(tableName, groupId).apply()(session)
      val prep_stmt = SQL(insertSQL(tableName, extractors))
      for (read <- xml \ "read"; lane <- read \ "lane") {
        val fields = Seq((read \ "name").text, (lane \ "name").text, (lane \ "percentQ30").text)
        val vals = extractors.map(_.func.apply(fields))
        vals.find(_.isFailure).map { case Failure(f) => throw f }
        prep_stmt.bind(vals.map{case Success(v) => v} : _*).executeUpdate().apply()
      }
      Right("Q30 => ok")
    } catch {
      case e: java.net.ConnectException => Right(etlMessage(s"Q30 => ${e.getMessage}\n$req"))
      case e: Exception => Right(etlMessage(s"Q30 => ${e.getMessage}"))
    }
  }
  plan.run
}

def walkupETL: ETL.etlType[DiscreteDelta[runItem]] = delta => session => {
  val run = delta.unpack.head // make sure 'chunkSize=1' is specified when calling prepareETL
  val handlers = mytasks.map(taskToHandler(_)) ++ Seq(Q30Handler, storedFunctionHandler("COGNOS.WALKUP_LANE"))
  val groupId = Seq(Extractor("RUN_NAME", _ => Success(run.runName)))
  val res = handlers.map(_(groupId, session))
  Seq((delta, ETL.aggregateResults("", res)))
}

val agentName = "analytics.tiger.WalkupMetricsAgent"

//val delta = new discreteDelta(Set(runItem("150522_NS500103_0118_AHCFHTBGXX","SL-NXA")))
//val etlPlan = prepareEtl(agentName, delta, walkupETL)(chunkSize=1/*, convertor = daysToOrganicRunConvertor*/)

val etlPlan = for (
  delta <- DaysDelta.loadFromDb(agentName) map DaysDelta.pushLeft(5);
  plan <- prepareEtl(agentName, delta, walkupETL)(chunkSize=1, convertor = daysToOrganicRunConvertor)
) yield plan

val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
