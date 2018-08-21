import analytics.tiger.ETL._
import analytics.tiger._

val tasks = Seq(
  ("analytics.tiger.PoolingCalculatorAgent"     , "Regular", ""              ),
  ("analytics.tiger.PoolingCalculatorAgent.CRSP", "CRSP"   , "@CRSPREPORTING")
)

tasks.foreach { case (agentName, source, dblink) =>
  val etlPlan = for (
    delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(1L*60*60*1000 /* 1 hour*/);
    plan <- prepareEtl(
      agentName,
      delta,
      sqlScript.etl[MillisDelta](relativeFile("resources/pooling_calculator_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/")
      ))()
  ) yield plan
  val res = utils.CognosDB.apply(etlPlan)
  defaultErrorEmailer(agentName)(res)
  print(res)
}
