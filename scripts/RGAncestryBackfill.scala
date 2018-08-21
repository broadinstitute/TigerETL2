import analytics.tiger.ETL._
import analytics.tiger._

val agentName = "analytics.tiger.RgAncestryAgent.Backfill"
val dbName = "CognosDatabaseProd"

val myDelta = MillisDelta("2015-may-01 00:00:00", "2015-may-21 00:00:00")
val etlPlan =prepareEtl(agentName, myDelta, sqlScript.etl(relativeFile("resources/rg_ancestry_backfill.sql")))()

val res = utils.mkConnProvider(dbName).apply(etlPlan)
print(res)