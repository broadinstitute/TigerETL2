package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._

/**
  * Created by atanas on 5/30/2017.
  */
object PicardAggregator {

  val tasks = Seq(
    ("analytics.tiger.agents.PicardAggregator"     , "Regular", ""              ),
    ("analytics.tiger.agents.PicardAggregator.CRSP", "CRSP"   , "@CRSPREPORTING.CRSPPROD")
  )

  def main(args: Array[String]) {
    tasks.foreach { case (agentName, source, dblink) =>
      val etlPlan = for (
        delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(1L * 60 * 60 * 1000 /* 1 hour*/);
        plan <- prepareEtl(
          agentName,
          delta,
          sqlScript.etl[MillisDelta](relativeFile("resources/picard_agg_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/")
          ))()
      ) yield plan
      val res = utils.AnalyticsEtlDB.apply(etlPlan)
      defaultErrorEmailer(agentName)(res)
      print(res)
    }
  }

  /*
  // This is how to manually construct sqlScripts for further exploration
  val delta = new millisDelta("2015-may-22 00:00:00", "2015-may-23 00:00:00")
  Seq(tasks.apply(0)).map { case (agentName, source, dblink, missingTables) =>
    sqlScript.createSteps(delta,relativeFile("resources/picard_agg_etl.sql"), Map("/*SOURCE*/" -> s"'$source'/*SOURCE*/", "/*DBLINK*/" -> s"$dblink/*DBLINK*/") ++ missingTables).map(_.sql)
  }.head.apply(0)
  */

}
