package analytics.tiger.agents

/**
  * Created by atanas on 5/31/2017.
  */
import java.sql.SQLException

import analytics.tiger.ETL._
import analytics.tiger._

import scala.util.Left

object BspSample {

  // notify BSP for SQL exceptions
  def myExceptionToResult(e: Exception) : Either[etlMessage,Any] = Left(etlMessage(e.getMessage, ETL.errorRecipients ++ (
    e match {
      case e:SQLException => Seq("bsp-dev@broadinstitute.org")
      case e => Seq()
    }
    )))

  val agentName = utils.objectName(this)
  val storedFunctionName = "BSP_SAMPLE_ETL"

  def main(args: Array[String]) {
    val etlPlan = for (
    //delta <- new DeltaProvider(loadMillisDeltaFromDB(agentName)).map(advancer(30)) ;
      delta <- MillisDelta.loadFromDb(agentName);
      plan <- prepareEtl(agentName, delta, storedFunctionETL(storedFunctionName))(chunkSize = 30 * 24 * 60 * 60 * 1000L, exceptionToResult = myExceptionToResult)
    ) yield plan

    val res = utils.CognosDB.apply(etlPlan)
    print(res)
    defaultErrorEmailer(agentName)(res)
  }
  //val endTime = new DateTime(2015, 9, 9, 14, 0)
  //while (DateTime.now.isBefore(endTime)) { utils.CognosDB.apply(etlPlan) }
  //0.until(20).foreach(it => utils.CognosDB.apply(etlPlan))
}
