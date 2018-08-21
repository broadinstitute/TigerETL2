package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._

/**
  * Created by atanas on 8/1/2017.
  */
object PicoQuant {

  def main(args: Array[String]) {
    val agentName = utils.objectName(this)
    val delta = MillisDelta.loadFromDb(agentName)
    val etlPlan = delta flatMap( d => prepareEtl(agentName, d, sqlScript.etl(relativeFile("resources/pico_quants_etl.sql")))())

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    print(res)
  }
}
