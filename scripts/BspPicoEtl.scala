import analytics.tiger.ETL._
import analytics.tiger._

val agentName = "analytics.tiger.BspPicoEtl"
val dbName = "CognosDatabaseProd"
val storedFunctionName = "analytics.pico_etl.Pico_Request_Agent@ANALYTICS.GAP_PROD"

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName);
  plan <- prepareEtl(agentName, delta, storedFunctionETL(storedFunctionName))()
) yield plan
val res = utils.mkConnProvider(dbName).apply(etlPlan)
print(res)
defaultErrorEmailer(agentName)(res)
