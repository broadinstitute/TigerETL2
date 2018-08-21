import analytics.tiger.ETL._
import analytics.tiger._

val agentName =  "analytics.tiger.PicoQuantAgent"
val etlPlan = prepareEtl[DummyDelta,DummyDelta](agentName, dummyDelta, sqlScript.etl(relativeFile("resources/pico_quants_etl.sql")))(toCommit = false)

val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
print(res)
