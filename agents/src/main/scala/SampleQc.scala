package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._

/**
  * Created by mariela on 10/18/2017.
  */
object SampleQc {
  def main(args: Array[String]) {
    val agentName = utils.objectName(this)
    val delta = MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(24*60*60*1000)
    val etlPlan = delta flatMap( d => prepareEtl(agentName, d, sqlScript.etl(relativeFile("resources/sample_qc_metrics_etl.sql")))())

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    print(res)
  }

}
