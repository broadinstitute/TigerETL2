import analytics.tiger.ETL._
import analytics.tiger._

println("Hello ! This is my 1st Etl Script. Hope you like it")

val agentName = "analytics.tiger.TestAgentMillis"
val functionName = "TEST_ETL_DAYS"

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName);
  plan <- prepareEtl(agentName, delta, storedFunctionETL(functionName))(toCommit = false)
) yield plan

val res = utils.mkConnProvider("CognosDatabaseProd").apply(etlPlan)
print(res)
defaultErrorEmailer(agentName)(res)
