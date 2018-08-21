package analytics.tiger.agents

/**
  * Created by atanas on 5/30/2017.
  */

import analytics.tiger.ETL._
import analytics.tiger._

object AggQc {

  val tasks = Seq(
    ("analytics.tiger.agents.AggQc"     , "Regular", ""              ),
    ("analytics.tiger.agents.AggQc.CRSP", "CRSP"   , "@CRSPREPORTING.CRSPPROD")
  )

  def main(args: Array[String]) {
    tasks.map { case (agentName, source, dblink) =>
      val etlPlan = for (
        delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(5L * 24 * 60 * 60 * 1000 /* 5 days*/);
        plan <- prepareEtl(
          agentName,
          delta,
          sqlScript.etl[MillisDelta](relativeFile("resources/agg_qc_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/")
          ))()
      ) yield plan

      utils.AnalyticsEtlDB.apply(etlPlan)
    }

  }

}
