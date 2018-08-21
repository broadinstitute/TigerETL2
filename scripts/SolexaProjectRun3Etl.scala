import analytics.tiger.ETL._
import analytics.tiger._

val tasks = Seq(
  ("analytics.tiger.SolexaProjectRun3Etl"     , "Regular", ""              ),
  ("analytics.tiger.SolexaProjectRun3Etl.CRSP", "CRSP"   , "@CRSPREPORTING")
)

tasks.foreach { case (agentName, source, dblink) =>
  val etlPlan = for (
    delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(240L*60*60*1000 /* 10 day*/);
    plan <- prepareEtl(
      agentName,
      delta,
      sqlScript.etl(relativeFile("resources/solexa_project_run3_etl.sql"), Map("/*DBLINK*/" -> s"$dblink/*DBLINK*/")
      ))()
  ) yield plan
  val res = utils.CognosDB.apply(etlPlan)
  defaultErrorEmailer(agentName)(res)
  print(res)
}

/*
// This is how to manually construct sqlScripts for further exploration
val delta = new millisDelta("2015-may-22 00:00:00", "2015-may-23 00:00:00")
Seq(tasks.apply(0)).map { case (agentName, source, dblink, missingTables) =>
  sqlScript.createSteps(delta,relativeFile("resources/solexa_project_run3_etl.sql"), Map("/*DBLINK*/" -> s"$dblink/*DBLINK*/") ++ missingTables).map(_.sql)
}.head.apply(0)
*/