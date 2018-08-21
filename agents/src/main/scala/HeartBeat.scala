package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._
import scalikejdbc._

import scala.util.{Failure, Success, Try}

/**
  * Created by atanas on 5/30/2017.
  */
object HeartBeat {

  val agentName = utils.objectName(this)

  val agents = scala.io.Source.fromFile(utils.config.getString("heartbeatProperties")).getLines().filterNot(_.startsWith("#")).map(_.split(" ")).toList

  val heartBeatAgent: ETL.etlType[DummyDelta] = delta => session => {
    implicit val sess = session
    val sql = """SELECT agents.agent_name, max(runs.end_time) last_successful_run
FROM (???) agents
LEFT JOIN etl_runs runs ON runs.agent_name=agents.agent_name AND runs.return_code=0 AND runs.start_time>sysdate-5
GROUP BY agents.agent_name, agents.timeout
HAVING 24*60*(sysdate-max(runs.end_time))>agents.timeout*2 OR max(runs.end_time) IS null
""".replace("???", agents.map{ case Array(agent,timeout) => s"SELECT '$agent' agent_name, $timeout timeout FROM dual"}.mkString(" UNION ALL "))
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
      if (items.exists{ case (_,_,_,_,time,_,_) => time>60 }) Left(etlMessage(
        itemsStr,
        ETL.errorRecipients ++ (if (items.exists(_._3 == "BSP")) Seq("Jameslee@broadinstitute.org","pdunlea@broadinstitute.org") else Nil)
      ))
      else if (!items.isEmpty) Right(etlMessage(itemsStr))
      else Right(s"OK")
    ))
  }

  val webCheckerAgent: ETL.etlType[DummyDelta] = delta => session => {
    Seq((delta,
      Try { scala.io.Source.fromURL("""http://analytics:8090/status""").mkString } match {
        case Failure(_) => Left(etlMessage("""'http://analytics:8090' is down !"""))
        case Success(_) => Right("ok")
      }
    ))
  }

  def main(args: Array[String]) {
    val etlPlan = prepareEtl(agentName, dummyDelta, heartBeatAgent)()
    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)

    defaultErrorEmailer("analytics.tiger.agents.connectionPoolChecker")(utils.CognosDB.apply(prepareEtl("analytics.tiger.agents.connectionPoolChecker", dummyDelta, connectionPoolCheckerAgent)()))
    defaultErrorEmailer("analytics.tiger.agents.zombieChecker"        )(utils.CognosDB.apply(prepareEtl("analytics.tiger.agents.zombieChecker"        , dummyDelta, zombieCheckerAgent        )()))
    defaultErrorEmailer("analytics.tiger.agents.webChecker"        )(utils.CognosDB.apply(prepareEtl("analytics.tiger.agents.webChecker"        , dummyDelta, webCheckerAgent        )()))

    //print(res)
  }

}
