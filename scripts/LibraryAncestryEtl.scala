import analytics.tiger.ETL._
import analytics.tiger._

val agentName = "analytics.tiger.LibraryAncestryAgent"
val storedFunctionName = "cognos.lab.LibraryAncestryAgent"

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(24L*60*60*1000 /* 1 day*/);
  plan <- prepareEtl(agentName, delta, storedFunctionETL[MillisDelta](storedFunctionName))()
) yield plan
val res = utils.CognosDB.apply(etlPlan)
print(res)
defaultErrorEmailer(agentName)(res)
