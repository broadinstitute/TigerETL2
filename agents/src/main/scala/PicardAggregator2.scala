package analytics.tiger.agents

import analytics.tiger.ETL.{defaultErrorEmailer, prepareEtl, relativeFile, sqlScript}
import analytics.tiger.{MillisDelta, utils}
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import scalikejdbc._

import scala.util.Try

/**
  * Created by atanas on 12/1/2017.
  */
object PicardAggregator2 {

  val conf = ConfigFactory.load(""".\sqls.conf""")
  val aggregationConfig = conf.getConfig("аggregation")
  import cats.data.Reader

  case class ReadGroup(flowcell_barcode: String, lane: String, molecular_barcode_name: String)

  def timeToAggSamples(period: (DateTime, DateTime)) = Reader((session: DBSession) => {
    implicit val sess = session
    val dataId = scala.util.Random.nextInt()
    SQL(aggregationConfig.getString("timeToAggSample")).bind(period._1, period._2, period._1, period._2, dataId, period._1, period._2, dataId).execute().apply()
    dataId
  })

  def listOfSamples(s: String) = Reader((session: DBSession) => {
    implicit val sess = session
    val dataId = scala.util.Random.nextInt()
    sql"INSERT INTO ANALYTICS.DATA_SET VALUES(?,?)".batch(s.split(",").map(Seq(dataId, _)): _*).apply()
    dataId
  })

  def picardAgg(dataId: Int) = Reader((session: DBSession) => {
    implicit val sess = session
    //val mysql = aggregationConfig.getString("sampleAggregation").replaceFirst("<ID>", dataId.toString).replaceAllLiterally("/*SOURCE*/","'Regular'/*SOURCE*/").replaceAllLiterally("/*DBLINK*/","")
    val mysql = aggregationConfig.getString("sampleAggregation").replaceAllLiterally("/*DBLINK*/","")
    Try(SQL(mysql).bind(dataId, "Regular", "Regular").update().apply())
  })

  def main(args: Array[String]) {
    val tasks = Seq(
      ("analytics.tiger.agents.PicardAggregator"     , "Regular", ""              ),
      ("analytics.tiger.agents.PicardAggregator.CRSP", "CRSP"   , "@CRSPREPORTING.CRSPPROD")
    )

    val now = DateTime.now()
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



  implicit def toReader[B](a: Reader[DBSession,B]) =
    analytics.tiger.Reader{ (session: DBSession) => a(session) }

}
