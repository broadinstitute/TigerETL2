import analytics.tiger.ETL._
import analytics.tiger._

val tasks = Seq(
    ("analytics.tiger.AggQcEtl"     , "Regular", ""              ),
    ("analytics.tiger.AggQcEtl.CRSP", "CRSP"   , "@CRSPREPORTING")
)

tasks.map { case (agentName, source, dblink) =>
  val etlPlan = for (
    delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(5L*24*60*60*1000 /* 5 days*/);
    plan <- prepareEtl(
      agentName,
      delta,
      sqlScript.etl[MillisDelta](relativeFile("resources/agg_qc_etl.sql"), Map( "/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/") 
      ))()
  ) yield plan

  utils.CognosDB.apply(etlPlan)
}
