import analytics.tiger.ETL._
import analytics.tiger._

  val agentName =  "analytics.tiger.LibraryQpcrEtl" 
  val etlPlan = for (
    delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(1L*24*60*60*1000);
    plan <- prepareEtl(agentName, delta, sqlScript.etl[MillisDelta](relativeFile("resources/library_qpcr_etl.sql")))()
  ) yield plan


  val res = utils.CognosDB.apply(etlPlan)
  defaultErrorEmailer(agentName)(res)
  print(res)

  /* Manual construction of the SQL executed;
println(sqlScript.createSteps(utils.CognosDB.apply(delta), relativeFile("resources/library_qpcr_etl.sql"), Map()).map(_.sql).head)
 */