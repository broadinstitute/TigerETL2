package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._
import org.joda.time.DateTime
import scalikejdbc.DBSession

import scala.util.Try

/**
  * Created by atanas on 10/3/2017.
  */
object StoredFunction {

  case object timeSpec {
    def unapply(s: String) = """(MillisDelta|DaysDelta)\.(loadFromDb|now)(\((\d*),(\d*)\))?""".r.unapplySeq(s).map(it => (it(0), it(1), Try(it(3).toInt).getOrElse(0), Try(it(4).toInt).getOrElse(0)))
  }

  def main(args: Array[String]) {

    val argsMap = utils.argsToMap(args)

    val db = Map("cognos" -> utils.CognosDB, "analyticsetl" -> utils.AnalyticsEtlDB)(argsMap("db"))
    val storedFunction = argsMap("storedFunction")
    val agentName = argsMap.getOrElse("agentName", s"analytics.tiger.agents.storedFunction.$storedFunction")
    val deltaSpec = argsMap("delta")

    val etlPlan = deltaSpec match {
      case timeSpec("MillisDelta", mtype, left, right) =>
        val deltaReader = (mtype match {
          case "loadFromDb" => MillisDelta.loadFromDb(agentName, dbSysdate = ETL.getDbSysdate(right/1000))
          case "now" => Reader((_: DBSession) => MillisDelta(new DateTime, new DateTime().minusMillis(right)))
        }) map (d => new MillisDelta(d.start.minus(left), d.size+left) { override val persist = d.persist })
        deltaReader flatMap (prepareEtl(agentName, _, storedFunctionETL(storedFunction))())

      case timeSpec("DaysDelta", mtype, left, right) =>
        val deltaReader = (mtype match {
          case "loadFromDb" => DaysDelta.loadFromDb(agentName, dbSysdate = ETL.getDbSysdate(right*24*60*60))
          case "now" => Reader((_: DBSession) => DaysDelta(new DateTime, new DateTime().minusDays(right)))
        }) map (d => new DaysDelta(d.start.minus(left), d.size+left) { override val persist = d.persist })
        deltaReader flatMap (prepareEtl(agentName, _, storedFunctionETL(storedFunction))())

//      case _ =>  prepareEtl(agentName, dummyDelta, storedFunctionETL(storedFunction))()
      }

    val res = db.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    println(res)
  }
}
