import analytics.tiger.ETL._
import analytics.tiger._

  val agentName =  "analytics.tiger.DesignationAgent" 
  val etlPlan = for (
    delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(30L*24*60*60*1000);
    plan <- prepareEtl(agentName, delta, sqlScript.etl[MillisDelta](relativeFile("resources/designation_etl.sql")))()
  ) yield plan


  val res = utils.CognosDB.apply(etlPlan)
  defaultErrorEmailer(agentName)(res)
  print(res)

  /* Manual construction of the SQL executed;
println(sqlScript.createSteps(utils.CognosDB.apply(delta), relativeFile("resources/designation_etl.sql"), Map()).map(_.sql).head)
 */