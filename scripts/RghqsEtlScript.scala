import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.ext.mercury.sequencingRun
import scalikejdbc._

val db = utils.CognosDB
val tasks = Seq(
  ("analytics.tiger.AnalysisScannerAgent"     , "analytics.tiger.RghqsLoaderAgent"     , "Regular", ""),
  ("analytics.tiger.AnalysisScannerAgent.CRSP", "analytics.tiger.RghqsLoaderAgent.CRSP", "CRSP"   , "@CRSPREPORTING")
)

////////////////////
val rghqs_targets_sql =
  """
MERGE INTO slxre_rghqs_targets tr
USING (
  WITH core AS (
    SELECT DISTINCT flowcell_barcode, run_name, greatest(ba.modified_at, ba.workflow_end_date) workflow_end_date
      FROM metrics.basecalling_analysis/*DBLINK*/ ba
      WHERE greatest(ba.modified_at, ba.workflow_end_date)>=?
        and greatest(ba.modified_at, ba.workflow_end_date)<?
    UNION
    SELECT DISTINCT flowcell_barcode, run_name, greatest(pa.modified_at, pa.workflow_end_date) workflow_end_date
    FROM metrics.picard_analysis/*DBLINK*/ pa
    WHERE greatest(pa.modified_at, pa.workflow_end_date)>=?
      and greatest(pa.modified_at, pa.workflow_end_date)<?
  )
  SELECT core.flowcell_barcode, max(core.run_name) run_name, max(core.workflow_end_date) workflow_end_date
  FROM core
  JOIN slxre2_organic_run r ON r.run_name = core.run_name AND r.run_date >= to_date('1-NOV-2012') --rghqs_cutoff
  LEFT JOIN slxre_rghqs_targets tr ON tr.flowcell_barcode=core.flowcell_barcode
  WHERE workflow_end_date-NVL(tr.request_TIMESTAMP,'1-jan-2000')>0 --AND to_date(substr(core.run_name, 1, 6), 'YYMMDD')>=to_date('1-NOV-2012') --rghqs_cutoff
  GROUP BY core.flowcell_barcode
) delta ON (tr.flowcell_barcode=delta.flowcell_barcode)

WHEN NOT MATCHED THEN
INSERT VALUES(delta.flowcell_barcode, delta.run_name, sysdate, NULL, NULL, NULL, ?)

WHEN MATCHED THEN
UPDATE SET run_name=delta.run_name, request_timestamp=SYSDATE
WHERE tr.request_timestamp<delta.workflow_end_date
  """

def scanner_etl(source: String, dblink: String): ETL.etlType[MillisDelta] = delta => session => {
  val (d1, d2) = delta.unpack
  val runs_detected = SQL(rghqs_targets_sql.replace("/*DBLINK*/", dblink)).bind(d1, d2, d1, d2, source).executeUpdate().apply()(session)
  Seq((delta, Right(s"runs_detected: $runs_detected")))
}

tasks.foreach{ case (agentName, _, source, dblink) =>
  val etlPlan = for (
    delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(7L*24*60*60*1000);
    plan <- prepareEtl(agentName, delta, scanner_etl(source, dblink))()
  ) yield plan
  val res = db.apply(etlPlan)
  defaultErrorEmailer(agentName)(res)
  print(res)
}

////////////////////
val agentName2 = "analytics.tiger.RunMercuryAgent"
val etlPlan2 = sequencingRun.loadDeltaPending flatMap (prepareEtl(agentName2, _, sequencingRun.RunMercuryAgent)(chunkSize = 1))
val res2 = utils.CognosDB.apply(etlPlan2)
defaultErrorEmailer(agentName2)(res2)
print(res2)

////////////////////
def loadRghqsLoaderDelta(source: String) = Reader((session: DBSession) => {
  implicit val sess = session
  new DiscreteDelta(sql"""
  SELECT flowcell_barcode
  FROM slxre_rghqs_targets
  WHERE
    rgmd_timestamp IS NOT NULL AND
    (
        rghqs_timestamp<rgmd_timestamp OR
        (rghqs_timestamp IS NULL AND
            (sysdate-rgmd_timestamp<=1 OR mod(24*(sysdate-rgmd_timestamp),168)<2)
        )
    )
    AND source=?
    AND blacklist_timestamp IS NULL
  """.bind(source).map(_.string(1)).list.apply().toSet)
})

tasks.foreach{ case (_, agentName, source, dblink) =>
  val etlPlan = for (
    delta <- loadRghqsLoaderDelta(source);
    //delta <- new DeltaProvider(_ => new discreteDelta[String](Set("H3FF2CCXX")));
    plan <- prepareEtl(agentName, delta,
      sqlScript.etl(relativeFile("resources/rghqs_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/"))
      )(chunkSize=50)
  ) yield plan
  val res = db.apply(etlPlan)
  defaultErrorEmailer(agentName)(res)
  print(res)
}
