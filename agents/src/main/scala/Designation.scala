package analytics.tiger.agents

/**
  * Created by atanas on 8/1/2017.
  */
import analytics.tiger.ETL._
import analytics.tiger._

object Designation {

  def main(args: Array[String]) {

    val agentName = utils.objectName(this)
    val etlPlan = for (
      delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(30L*24*60*60*1000);
      plan <- prepareEtl(agentName, delta, sqlScript.etl[MillisDelta](relativeFile("resources/designation_etl.sql")))()
    ) yield plan

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
  }

  /* Manual construction of the SQL executed;
println(sqlScript.createSteps(utils.CognosDB.apply(delta), relativeFile("resources/designation_etl.sql"), Map()).map(_.sql).head)
 */

}
