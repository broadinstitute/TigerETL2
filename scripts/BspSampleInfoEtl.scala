import java.net.URLEncoder
import java.sql.Timestamp
import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics.metricType
import org.joda.time.DateTime
import scalikejdbc.{DBSession, SQL}
import spray.client.pipelining._
import akka.actor.ActorSystem
import spray.http.{Uri, HttpResponse, BasicHttpCredentials, HttpRequest}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

val prep_stmt = SQL("""
MERGE INTO BSP_SAMPLE_TIGER tr
USING(SELECT ? SAMPLE_ID,? STOCK_TYPE,? ROOT_SAMPLES,? STOCK_SAMPLE,? COLLABORATOR_SAMPLE_ID,? UUID,? COLLECTION,? COLLABORATOR_PARTICIPANT_ID,? PARTICIPANT_IDS,? SAMPLE_LSID,? MATERIAL_TYPE,? ORIGINAL_MATERIAL_TYPE,? RECEIPT_DATE,? VOLUME,? CONC,? EXPORT_DATE,? GSSR_ID,? PICO_RUN_DATE,? SAMPLE_TYPE,? COLLABORATOR_SAMPLE_ID2,? TISSUE_SITE,? TISSUE_SITE_DETAIL,? BATCH_COMPLETED_ON_PROD_FROM,? BATCH_CREATED_ON_PROD_FROM,? BATCH_PRODUCED_FROM,? BATCH_RUN_IN,? CONTAINER_NAME,? RIN_NUMBER,? PROCESS_YIELD_PER_INPUT,? PROCESS_YIELD_PER_INPUT_UNITS,? PROCESS_TYPE_PROD_FROM,? ORGANISM_SCIENTIFIC_NAME,? STRAIN,? TIMESTAMP FROM dual)
delta ON (tr.SAMPLE_ID=delta.SAMPLE_ID)
WHEN NOT MATCHED THEN INSERT VALUES(delta.SAMPLE_ID,delta.STOCK_TYPE,delta.ROOT_SAMPLES,delta.STOCK_SAMPLE,delta.COLLABORATOR_SAMPLE_ID,delta.UUID,delta.COLLECTION,delta.COLLABORATOR_PARTICIPANT_ID,delta.PARTICIPANT_IDS,delta.SAMPLE_LSID,delta.MATERIAL_TYPE,delta.ORIGINAL_MATERIAL_TYPE,delta.RECEIPT_DATE,delta.VOLUME,delta.CONC,delta.EXPORT_DATE,delta.GSSR_ID,delta.PICO_RUN_DATE,delta.SAMPLE_TYPE,delta.COLLABORATOR_SAMPLE_ID2,delta.TISSUE_SITE,delta.TISSUE_SITE_DETAIL,delta.BATCH_COMPLETED_ON_PROD_FROM,delta.BATCH_CREATED_ON_PROD_FROM,delta.BATCH_PRODUCED_FROM,delta.BATCH_RUN_IN,delta.CONTAINER_NAME,delta.RIN_NUMBER,delta.PROCESS_YIELD_PER_INPUT,delta.PROCESS_YIELD_PER_INPUT_UNITS,delta.PROCESS_TYPE_PROD_FROM,delta.ORGANISM_SCIENTIFIC_NAME,delta.STRAIN,delta.TIMESTAMP)
WHEN MATCHED THEN UPDATE SET STOCK_TYPE=delta.STOCK_TYPE,ROOT_SAMPLES=delta.ROOT_SAMPLES,STOCK_SAMPLE=delta.STOCK_SAMPLE,COLLABORATOR_SAMPLE_ID=delta.COLLABORATOR_SAMPLE_ID,UUID=delta.UUID,COLLECTION=delta.COLLECTION,COLLABORATOR_PARTICIPANT_ID=delta.COLLABORATOR_PARTICIPANT_ID,PARTICIPANT_IDS=delta.PARTICIPANT_IDS,SAMPLE_LSID=delta.SAMPLE_LSID,MATERIAL_TYPE=delta.MATERIAL_TYPE,ORIGINAL_MATERIAL_TYPE=delta.ORIGINAL_MATERIAL_TYPE,RECEIPT_DATE=delta.RECEIPT_DATE,VOLUME=delta.VOLUME,CONC=delta.CONC,EXPORT_DATE=delta.EXPORT_DATE,GSSR_ID=delta.GSSR_ID,PICO_RUN_DATE=delta.PICO_RUN_DATE,SAMPLE_TYPE=delta.SAMPLE_TYPE,COLLABORATOR_SAMPLE_ID2=delta.COLLABORATOR_SAMPLE_ID2,TISSUE_SITE=delta.TISSUE_SITE,TISSUE_SITE_DETAIL=delta.TISSUE_SITE_DETAIL,BATCH_COMPLETED_ON_PROD_FROM=delta.BATCH_COMPLETED_ON_PROD_FROM,BATCH_CREATED_ON_PROD_FROM=delta.BATCH_CREATED_ON_PROD_FROM,BATCH_PRODUCED_FROM=delta.BATCH_PRODUCED_FROM,BATCH_RUN_IN=delta.BATCH_RUN_IN,CONTAINER_NAME=delta.CONTAINER_NAME,RIN_NUMBER=delta.RIN_NUMBER,PROCESS_YIELD_PER_INPUT=delta.PROCESS_YIELD_PER_INPUT,PROCESS_YIELD_PER_INPUT_UNITS=delta.PROCESS_YIELD_PER_INPUT_UNITS,PROCESS_TYPE_PROD_FROM=delta.PROCESS_TYPE_PROD_FROM,ORGANISM_SCIENTIFIC_NAME=delta.ORGANISM_SCIENTIFIC_NAME,STRAIN=delta.STRAIN,TIMESTAMP=delta.TIMESTAMP
""")
/* If necessary, use this command to re-generate that ugly MERGE statement
println(analytics.tiger.utils.CognosDB.apply(analytics.tiger.Reader(analytics.tiger.utils.generateMERGE("BSP_SAMPLE"))))
1st param - table name
*/

def samplesConvertor(delta: MillisDelta)(session: DBSession) = {
  implicit val sess = session
  val (d1, d2) = delta.unpack
  val res = SQL("""
SELECT sample_id
FROM bsp.bsp_sample@ANALYTICS.GAP_PROD s
WHERE ?<=s.last_updated_on AND s.last_updated_on<?
UNION
SELECT r.sample_id
FROM gap.object_alias_hist@ANALYTICS.GAP_PROD hist
JOIN gap.object_alias@ANALYTICS.GAP_PROD al ON al.alias_id=hist.alias_id
JOIN bsp.bsp_individual@ANALYTICS.GAP_PROD i ON i.lsid=al.lsid
JOIN bsp.bsp_sample@ANALYTICS.GAP_PROD root_s ON root_s.individual_id=i.individual_id
JOIN bsp.bsp_root_sample@ANALYTICS.GAP_PROD r ON r.root_sample_id=root_s.sample_id
WHERE
    ?<=hist.updated_on AND hist.updated_on<?
    AND hist.alias_type_id IN (4,6)
UNION
SELECT r.sample_id
FROM gap.object_alias_hist@ANALYTICS.GAP_PROD hist
JOIN gap.object_alias@ANALYTICS.GAP_PROD al ON al.alias_id=hist.alias_id
--JOIN bsp.bsp_individual i ON i.lsid=al.lsid
JOIN bsp.bsp_sample@ANALYTICS.GAP_PROD root_s ON root_s.lsid=al.lsid
JOIN bsp.bsp_root_sample@ANALYTICS.GAP_PROD r ON r.root_sample_id=root_s.sample_id
WHERE
    ?<=hist.updated_on AND hist.updated_on<?
    AND hist.alias_type_id IN (9,26)
""").bind(d1, d2, d1, d2, d1, d2).map(_.string(1)).list.apply()

  DiscreteDelta[String](res.toSet)
  //new discreteDelta[String](Set("SM-9W5RB","SM-9W7PP"))
}

object BspSpecies {

  // The injection method (optional)
  def apply(parts: String*): String = parts.mkString(":")

  // The extraction method (mandatory)
  def unapplySeq(whole: String): Option[Seq[String]] = Some(whole.split("\\:"))

  def parse(s: String) = {
    val (ignoreGenus, sciName, strain) =
      try s match {
          case BspSpecies(a,b,c) => (a     ,b  ,c)
          case BspSpecies(a,b)   => (a     ,b  ,None)
          case BspSpecies(b)     => (None  ,b  ,None)
          case BspSpecies(a,b,c,_) => (a   ,b  ,c)
          case BspSpecies(a,b,c,_,_) => (a   ,b  ,c)
      } catch {
        case e:scala.MatchError => throw new Exception(s"Can't parse $s")
      }
    (sciName.trim, if (strain==None) "" else strain.toString.trim)
  }
}

def get(s: Seq[String], index: Int) = try s(index) catch { case _: IndexOutOfBoundsException => "" }

def extr(columnName: String, mt: metrics.metricType.EnumVal) =
  (columnName,
  (titles: Seq[String]) => (columns: Seq[String]) => metrics.metricUtils.recordableValue(get(columns, titles.indexOf(columnName)), mt)
  )

// This list must match all columns in target table being merged into
val interestingColumns = Seq(
  extr("Sample ID", metricType.String),
  extr("Stock Type", metricType.String),
  extr("Root Sample(s)", metricType.String),
  extr("Stock Sample", metricType.String),
  extr("Collaborator Sample ID", metricType.String),
  extr("UUID", metricType.String),
  extr("Collection", metricType.String),
  extr("Collaborator Participant ID", metricType.String),
  extr("Participant ID(s)", metricType.String),
  extr("Sample LSID", metricType.String),
  extr("Material Type", metricType.String),
  extr("Original Material Type", metricType.String),
  extr("Receipt Date", metricType.Timestamp("MM/dd/yyyy")),
  extr("Volume", metricType.Double),
  extr("Conc", metricType.Double),
  extr("Export Date", metricType.Timestamp("MM/dd/yyyy")),
  extr("GSSR ID", metricType.String),
  extr("Pico Run Date", metricType.Timestamp("MM/dd/yyyy")),
  extr("Sample Type", metricType.String),
  extr("Collaborator Sample ID 2", metricType.String),

  extr("Tissue Site", metricType.String),
  extr("Tissue Site Detail", metricType.String),
  extr("Batch Completed On (Produced From)", metricType.Timestamp("MM/dd/yyyy")),
  extr("Batch Created On (Produced From)"  , metricType.Timestamp("MM/dd/yyyy")),
  extr("Batch Produced From", metricType.String),
  extr("Batch Run In", metricType.TrimmedString(500)),
  extr("Container Name", metricType.String),
  extr("RIN Number", metricType.Double),
  extr("Process Yield per Input", metricType.Double),
  extr("Process Yield per Input Units", metricType.String),
  extr("Process Type Produced From", metricType.String),

  ("Species", (titles: Seq[String]) => (columns:Seq[String]) => BspSpecies.parse(get(columns, titles.indexOf("Species")))._1),
  ("Species", (titles: Seq[String]) => (columns:Seq[String]) => BspSpecies.parse(get(columns, titles.indexOf("Species")))._2),
  (""       , (titles: Seq[String]) => (columns:Seq[String]) => new Timestamp(DateTime.now().getMillis))
)

implicit val system = ActorSystem()
val pipeline: HttpRequest => Future[HttpResponse] = (
  addHeader("X-My-Special-Header", "fancy-value")
    ~> addCredentials(BasicHttpCredentials("pmbridge","bspbsp2"))
    //  ~> encode(Gzip)
    ~> sendReceive
    //  ~> decode(Deflate)
    ~> unmarshal[HttpResponse]
  )

val etl: ETL.etlType[DiscreteDelta[String]] = delta => session => {
  implicit val sess = session
  val uriStr = s"http://bsp/ws/bsp/search/runSampleSearch?${interestingColumns.map(_._1).distinct.filterNot(_=="").map(it => s"columns=${URLEncoder.encode(it, "UTF-8")}").mkString("&") }"
  try {
    val groups = delta.unpack.grouped((2040-uriStr.size)/6)
    var count = 0
    groups.foreach(sample_ids => {
      import scala.concurrent.duration._
      val httpResponse = Await.result(pipeline(Get(Uri(s"$uriStr&sample_ids=${sample_ids.mkString(",")}"))), 5 minutes)
      val lines = scala.io.Source.fromRawBytes(httpResponse.entity.data.toByteArray).getLines()
      val titles = lines.next().split("\t")
      val extractors = interestingColumns.map(_._2.apply(titles))
      lines.foreach(line => {
        val columns = line.split("\t")
        val params = extractors.map(_.apply(columns))
        prep_stmt.bind(params: _*).executeUpdate().apply()
      })
      count += 1
    })
    Seq((delta, Right(s"${delta.size} samples processed in $count web-calls.")))
  } catch {
    case timeout: java.util.concurrent.TimeoutException => Seq((delta, Left(etlMessage("Service timed out."))))
  }
}

val agentName = "analytics.tiger.SampleInfoBspAgent"
val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName);
  plan <- prepareEtl(agentName, delta, etl)(convertor = samplesConvertor)
) yield plan
val res = utils.CognosDB.apply(etlPlan)
system.shutdown()
defaultErrorEmailer(agentName)(res)
print(res)
