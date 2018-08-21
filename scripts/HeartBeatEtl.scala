import analytics.tiger._
import analytics.tiger.ETL._
import scalikejdbc._

val agentName = "analytics.tiger.HeartBeatAgent"

val agents = Seq(
  ("analytics.tiger.RghqsLoaderAgent"	     ,60),
  ("analytics.tiger.PdoStar5Agent"		     ,120),
  //  ("analytics.tiger.PdoStar2AuxAgent"		 ,120),
  ("analytics.tiger.PicardAggregatorEtl"     ,120),
  ("analytics.tiger.PicardAggregatorEtl.CRSP",120),
  ("genomics.etl.plsql.GssrAgent" 		     ,24*60),
  //  ("analytics.tiger.BspSampleAttributeEtl"   ,15),
  ("analytics.tiger.LibraryAncestryAgent"    ,60),
  ("analytics.tiger.LibrarySampleLcSetEtl"   ,120),
  ("analytics.tiger.LimsAncestryEventAgent"  ,120),
  ("analytics.tiger.RgAncestryAgent"         ,30),
  ("analytics.tiger.LimsDeckEventsAgent"     ,24*60),
  ("analytics.tiger.AggQcEtl"	     		 ,60),
  ("genomics.etl.plsql.SolexaOrganicRunAgent",60),
  ("analytics.tiger.PoolingCalculatorAgent",15),
  ("analytics.tiger.PoolingCalculatorAgent.CRSP",15),
  ("genomics.etl.plsql.BSPPicoControls_Agent",60),
  ("genomics.etl.plsql.LabEventsAgent"       ,60),
  ("genomics.etl.plsql.Sample_QC_Agent"      ,30),
  //  ("genomics.etl.plsql.PDO_Sample_Agg_Tracking_Agent",4*60),

  ("genomics.etl.plsql.RunAggTimeAgent3"             ,3*60),
  ("genomics.etl.plsql.Aggregation_BAM_Files"        ,24*60),
  ("genomics.etl.plsql.Delivered_ReadGroups_SRAFiles",24*60),
  ("genomics.etl.plsql.ReadGroup_Level_SRAFiles"     ,24*60),
  ("genomics.etl.plsql.SubmissionAgent"              ,24*60),
  ("genomics.etl.plsql.QTP_Agent"                    ,24*60),
  ("analytics.tiger.agents.WalkupAgent$"				 ,24*60),
  ("analytics.tiger.GapAutocallAgent"                ,24*60),
  //("analytics.tiger.InfiniumControlAgent"            ,19*60),
  // ("analytics.tiger.AccessArrayAgent"                ,24*60), // decommissioned on Nov-03
  ("analytics.tiger.BspPlatingRequestAgent"          ,24*60),
  ("analytics.tiger.JiraTransitions"                 ,24*60),
  ("analytics.tiger.spark.ArraysQcAgent"            , 60)
)

val heartBeatAgent: ETL.etlType[DummyDelta] = delta => session => {
  implicit val sess = session
  val sql = """SELECT agents.agent_name, max(runs.end_time) last_successful_run
FROM (???) agents
LEFT JOIN etl_runs runs ON runs.agent_name=agents.agent_name AND runs.return_code=0 AND runs.start_time>sysdate-5
GROUP BY agents.agent_name, agents.timeout
HAVING 24*60*(sysdate-max(runs.end_time))>agents.timeout*2 OR max(runs.end_time) IS null
            """.replace("???", agents.map{ case (agent,timeout) => s"SELECT '$agent' agent_name, $timeout timeout FROM dual"}.mkString(" UNION ALL "))
  val items = SQL(sql).map{ it => s"${it.string(1)} last successful run ${it.string(2)}"}.list().apply()
  Seq((delta,
    if (items.isEmpty) Right(s"${agents.size} agents checked. OK")
    else Left(etlMessage(items.mkString("\n\r","\n\r","")))
    ))
}

val connectionPoolCheckerAgent: ETL.etlType[DummyDelta] = delta => session => {
  implicit val sess = session
  val Some((nConnections, details)) =
    SQL("""
SELECT SUM(connections), concat_string_csv(TO_CHAR(moment, 'DD-MON-YYYY HH24:MI') || ' -> ' || connections || ' conn')
FROM
(SELECT round(logon_time, 'MI') moment, COUNT(*) connections
FROM v$session where machine='analytics-etl'
GROUP BY round(logon_time, 'MI')
)""".stripMargin).map(it => (it.int(1), it.string(2))).single().apply()
  Seq((delta,
    if (nConnections<20) Right(s"$nConnections analytics-etl connections. OK ($details)")
    else if (nConnections<=30) Right(etlMessage(s"$nConnections analytics-etl connections.  ($details)"))
    else Left(etlMessage(s"Too many ($nConnections) analytics-etl connections.  ($details)"))
    ))
}

val zombieCheckerAgent: ETL.etlType[DummyDelta] = delta => session => {
  implicit val sess = session
  val items = sql"SELECT host,sql_id,username,machine,running_time_min,sid,serial# FROM running_sqls_all a WHERE a.running_time_min>30".map(it => (it.string(1), it.string(2), it.string(3), it.string(4), it.int(5), it.string(6), it.string(7))).list().apply()
  val itemsStr = items.map{ case (host,sql_id,username,machine,time,sid,serial) => s"""sql_id: <a href="http://analytics-etl:8090/api/sqlFullText?sql_id=$sql_id">$sql_id</a>, host: $host, username: $username, machine: $machine, minutes: $time, sid/serial#: $sid,$serial""" } mkString("\n\r")
  Seq((delta,
    if (items.exists{ case (_,_,_,_,time,_,_) => time>60 }) Left(etlMessage(itemsStr))
    else if (!items.isEmpty) Right(etlMessage(itemsStr))
    else Right(s"OK")
  ))
}

val etlPlan = prepareEtl(agentName, dummyDelta, heartBeatAgent)()
val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)

defaultErrorEmailer("analytics.tiger.connectionPoolCheckerAgent")(utils.CognosDB.apply(prepareEtl("analytics.tiger.connectionPoolCheckerAgent", dummyDelta, connectionPoolCheckerAgent)()))
defaultErrorEmailer("analytics.tiger.zombieCheckerAgent"        )(utils.CognosDB.apply(prepareEtl("analytics.tiger.zombieCheckerAgent"        , dummyDelta, zombieCheckerAgent)()))

//print(res)

