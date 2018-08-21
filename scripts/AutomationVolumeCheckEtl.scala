import java.io.File
import java.sql.Timestamp

import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics.metricType
import org.joda.time.DateTime
import scalikejdbc.SQL

import scala.util.{Failure, Success}


val prep_stmt = SQL("""
                      |MERGE INTO VOLUMECHECK_METRICS tr
                      |USING(SELECT ? RACKID,? TUBE,? SAMPLES,? STATUS,? VOLMED,? VOLAVG,? VOLMIN,? VOLMAX,? VOLSTDEV,? DISMED,? DISAVG,? DISMIN,? DISMAX,? DISSTDEV,? SCANDATE,? TIMESTAMP FROM dual)
                      |delta ON (tr.RACKID=delta.RACKID AND tr.TUBE=delta.TUBE AND tr.SCANDATE=delta.SCANDATE)
                      |WHEN NOT MATCHED THEN INSERT VALUES(delta.RACKID,delta.TUBE,delta.SAMPLES,delta.STATUS,delta.VOLMED,delta.VOLAVG,delta.VOLMIN,delta.VOLMAX,delta.VOLSTDEV,delta.DISMED,delta.DISAVG,delta.DISMIN,delta.DISMAX,delta.DISSTDEV,delta.SCANDATE,delta.TIMESTAMP)
                      |WHEN MATCHED THEN UPDATE SET SAMPLES=delta.SAMPLES,STATUS=delta.STATUS,VOLMED=delta.VOLMED,VOLAVG=delta.VOLAVG,VOLMIN=delta.VOLMIN,VOLMAX=delta.VOLMAX,VOLSTDEV=delta.VOLSTDEV,DISMED=delta.DISMED,DISAVG=delta.DISAVG,DISMIN=delta.DISMIN,DISMAX=delta.DISMAX,DISSTDEV=delta.DISSTDEV,TIMESTAMP=delta.TIMESTAMP
                      |""".stripMargin)
/* If necessary, use this command to re-generate that ugly MERGE statement
println(analytics.tiger.utils.CognosDB.apply(analytics.tiger.Reader(analytics.tiger.utils.generateMERGE("VOLUMECHECK_METRICS"))))
1st param - table name.
*/

def get(s: Seq[String], index: Int) =
  try s(index) catch { case _: IndexOutOfBoundsException => "" }

val extr = (columnName: String, mt: metrics.metricType.EnumVal) => (titles: Seq[String]) => (columns: Seq[String]) => metrics.metricUtils.recordableValue(get(columns, titles.indexOf(columnName)), mt)

val folder = scala.reflect.io.File.apply(new File(utils.config.getString("volumeCheckFolder")))
val machineNames = scala.io.Source.fromInputStream((folder / "MachineNames" / "MachineNames_Managers.csv").toFile.inputStream()).getLines().map(_.split(",")(0)).toSet

// This list must match all columns in target table being merged into
val interestingColumns = Seq(
  // RACKID
  (titles: Seq[String]) => (columns:Seq[String]) =>
    metrics.metricUtils.recordableValue(get(columns, titles.indexOf("RACKID")), metricType.String) match {
      case Success(rackid: String) => if (machineNames.exists(_ == rackid)) Success(rackid) else Failure(new RuntimeException(s"Invalid RackID: $rackid not found in MachineNames.txt"))
      case Failure(f) => Failure(f)
      case _ => Failure(new RuntimeException("Invalid RackID"))
    },
  extr("TUBE", metricType.String),
  extr("SAMPLES", metricType.Long),
  extr("STATUS", metricType.String),
  extr("VOLMED", metricType.Double),
  extr("VOLAVG", metricType.Double),
  extr("VOLMIN", metricType.Double),
  extr("VOLMAX", metricType.Double),
  extr("VOLSTDEV", metricType.Double),
  extr("DISMED", metricType.Double),
  extr("DISAVG", metricType.Double),
  extr("DISMIN", metricType.Double),
  extr("DISMAX", metricType.Double),
  extr("DISSTDEV", metricType.Double),

  // DATE
  (titles: Seq[String]) => (columns:Seq[String]) => metrics.metricUtils.recordableValue(get(columns, titles.indexOf("DATE")), metricType.Timestamp("MM-dd-yy")),

  (titles: Seq[String]) => (columns:Seq[String]) => scala.util.Try { new Timestamp(DateTime.now().getMillis)}
)

val etl: ETL.etlType[MillisDelta] = delta => session => {
  val (d1, _) = delta.unpack
  implicit val sess = session
  val res = folder.toDirectory.files.filter(file => file.name.matches("(.*).(C|c)(S|s)(V|v)") && d1.getMillis <= file.lastModified).map(file =>
    try {
      val lines = scala.io.Source.fromInputStream(file.inputStream()).getLines()
      val headers = lines.next().split(",")
      val extractors = interestingColumns.map(_(headers))
      lines.map(line => extractors.map(_.apply(line.split(",")))).foreach(vals => {
        vals.find(_.isFailure).map { case Failure(e) => throw e case _ => None}
        prep_stmt.bind(vals.map{case Success(v) => v case _ => None} : _*).executeUpdate().apply()
      })
      Right(s"${file.name}: OK")
    } catch {
      case e: Exception => Right(etlMessage(s"${file.name}: ${e.getMessage}", Seq("scott@broadinstitute.org","reportingerrors@broadinstitute.org")))
    }
  )
  Seq((delta, ETL.aggregateResults("", res.toSeq)))
}

val agentName = "analytics.tiger.AutomationVolumeCheckAgent"
val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName);
  plan <- prepareEtl(agentName, delta, etl)()
) yield plan
val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
