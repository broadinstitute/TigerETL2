package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._

/**
  * Created by atanas on 5/30/2017.
  */
object RgAncestry {

  val tasks = Seq(
    ("analytics.tiger.agents.RgAncestry"     , "", "")
  )

  def main(args: Array[String]) {
    tasks.foreach { case (agentName, source, dblink) =>
      val etlPlan = for (
        delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(1L * 60 * 60 * 1000 /* 1 hour*/);
        plan <- prepareEtl(
          agentName,
          delta,
          sqlScript.etl[MillisDelta](relativeFile("resources/rg_ancestry_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/")
          ))()
      ) yield plan
      val res = utils.CognosDB.apply(etlPlan)
      defaultErrorEmailer(agentName)(res)
      print(res)
    }
  }

  /*
      val agentName = "analytics.tiger.RgAncestryAgent"
      val storedFunctionName = "cognos.lab.RgAncestryAgent"

      val etlPlan = for (
        delta <- MillisDelta.loadFromDb(agentName);
        plan <- prepareEtl(agentName, delta, storedFunctionETL(storedFunctionName))()
      ) yield plan
      val res = utils.CognosDB.apply(etlPlan)
      print(res)
      defaultErrorEmailer(agentName)(res)
  */

}
