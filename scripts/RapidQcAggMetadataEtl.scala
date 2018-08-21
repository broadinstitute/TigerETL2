import analytics.tiger.ETL._
import analytics.tiger._

val agentName = "analytics.tiger.RapidQcAggMetadataEtl"

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(1L*60*60*1000 /* 1 hour*/);
  plan <- prepareEtl(
    agentName,
    delta,
    sqlScript.etl[MillisDelta](relativeFile("resources/rapidqc_agg_metadata_etl.sql")))()
) yield plan
val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
print(res)
