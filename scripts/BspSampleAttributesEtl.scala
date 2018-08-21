import analytics.tiger.ETL._
import analytics.tiger._

val agentName = "analytics.tiger.BspSampleAttributeEtl"

val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName, propLink = "@SEQPROD.COGNOS");
  plan <- prepareEtl(agentName, delta, sqlScript.etl(relativeFile("resources/bsp_sample_attributes_etl.sql")))(propLink = "@SEQPROD.COGNOS")
) yield plan
val res = utils.BspDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
