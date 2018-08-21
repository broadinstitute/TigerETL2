package analytics.tiger.agents

/**
  * Created by atanas on 5/30/2017.
  */

import analytics.tiger.ETL._
import analytics.tiger._

object PoolingCalculator {

  val tasks = Seq(
    ("analytics.tiger.agents.PoolingCalculator"     , "Regular", ""              ),
    ("analytics.tiger.agents.PoolingCalculator.CRSP", "CRSP"   , "@CRSPREPORTING.CRSPPROD")
  )

  def main(args: Array[String]) {
    tasks.foreach { case (agentName, source, dblink) =>
      val etlPlan = for (
        delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(1L * 60 * 60 * 1000 /* 1 hour*/);
        plan <- prepareEtl(
          agentName,
          delta,
          sqlScript.etl[MillisDelta](relativeFile("resources/pooling_calculator_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/")
          ))()
      ) yield plan
      val res = utils.AnalyticsEtlDB.apply(etlPlan)
      defaultErrorEmailer(agentName)(res)
      print(res)
    }
  }

}
