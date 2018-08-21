package analytics.tiger.agents

/**
  * Created by atanas on 5/30/2017.
  */

import analytics.tiger.ETL._
import analytics.tiger._


object LibraryAncestry {

  val agentName = utils.objectName(this)
  val storedFunctionName = "cognos.lab.LibraryAncestryAgent"

  def main(args: Array[String]) {
    val etlPlan = for (
      delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(24L * 60 * 60 * 1000 /* 1 day*/);
      plan <- prepareEtl(agentName, delta, storedFunctionETL[MillisDelta](storedFunctionName))()
    ) yield plan
    val res = utils.CognosDB.apply(etlPlan)
    print(res)
    defaultErrorEmailer(agentName)(res)
  }

}
