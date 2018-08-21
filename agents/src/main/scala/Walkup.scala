package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics._
import analytics.tiger.metrics.metricUtils._

import scala.util.{Left, Try}
import scala.util.matching.Regex

object Walkup {

  // notify BSP for SQL exceptions
  def myExceptionToResult(e: Exception) : Either[etlMessage,Any] = Left(etlMessage(e.getMessage, ETL.errorRecipients ++ (
    e match {
      case e: Exception if (e.getMessage.contains("No such file or directory")) => Seq("jowalsh@broadinstitute.org")
      case _ => Nil
    }
    )))


  def fromFiles(filenamePattern: Regex)(run: runItem) = {
    val dir = scala.reflect.io.File.apply(rootFolder / "proc" / run.instrument / run.runName / "Data/Intensities/BaseCalls").toDirectory
    if (!dir.exists) throw new Exception(s"Folder '${dir.path}' does not exist")
    dir.files.filter(_.name match {case filenamePattern(_*) => true case _ => false})
      .foldLeft((List.empty[String], Stream.empty[Array[String]])) { case (acc, file) =>
      (file.name :: acc._1,
        acc._2 ++
          scala.io.Source.fromInputStream(file.inputStream()).getLines()
          .dropWhile(!_.startsWith("## METRICS CLASS"))
          .toStream
          .drop(2)
          .filterNot(_.equals(""))
          .map(_.split('\t'))
      )
    }
  }

  val q30FromSummary = (run: runItem) => {
    val m = new metricUtils.metrics(scala.io.Source.fromFile(scala.reflect.io.File.apply(rootFolder / "proc" / run.instrument / run.runName / "dashboard/summary.csv").toFile.path).getLines().toList)
    val res = sectionsUnion(m.regularReads).select(Seq("Section Name", "Lane", "Surface", "%>=Q30"))
    (List.empty[String], res.data.filter(_(2) == "-").map(it => Array(it(0).substring(5), it(1), it(3))).toStream)
  }

  def q30FromWebService(run: runItem) = {
    val res = Try{
      val xml = scala.xml.XML.load(s"http://illuminadashboard/illumina/automation/jersey/interop/q30?cycleCutoff=1&runPath=/seq/illumina/proc/${run.instrument}/${run.runName}")
      for (
        read <- xml \ "read";
        lane <- read \ "lane"
      ) yield Array(read \ "name" text, lane \ "name" text, lane \ "percentQ30" text)
    }.recover{ case _ => Nil }.get
    (List.empty[String], res.toStream)
  }

  import analytics.tiger.metrics.metricType._
  val tasks = Seq(
  importTask(
    "ANALYTICS.WALKUP_BASECALLING",
    fromFiles("""basecalling.(\d).metrics""".r),
    Seq(
      Long,String,String,
      Long,Long,Long,Long,Long,Long,Long,Long,
      Double,Double,Double,Double
    )
  ),

  importTask(
    "ANALYTICS.WALKUP_PHASING",
    fromFiles("""(.*)illumina_phasing_metrics""".r),
    Seq(Long, String, Double, Double)
  ),

  importTask(
    "ANALYTICS.WALKUP_LANE",
    fromFiles("""(.*)illumina_lane_metrics""".r),
    Seq(Double, Long)
  ),

  importTask(
    "ANALYTICS.WALKUP_Q30",
    q30FromSummary,
    Seq(Long, Long, Double)
  ),

/*
  importTask(
    "ANALYTICS.WALKUP_Q30_TEST",
    q30FromSummary,
    Seq(
      Long, Long,
      Ignore,Ignore,Ignore,Ignore,Ignore,Ignore,Ignore,Ignore,Ignore,
      Double,
      Ignore,Ignore,Ignore,Ignore,Ignore,Ignorupe,Ignore,Ignore
    )
  ),
*/

  dbTask("ANALYTICS.WALKUP_LANE_AGGREGATE_ETL")
  )

  def walkupETL: ETL.etlType[DiscreteDelta[runItem]] = delta => session => {
    val run = delta.unpack.head // make sure 'chunkSize=1' is specified when calling prepareETL
    val res = tasks map { _.run(run)(session) } reduce mergeResults
    Seq((delta, res))
  }

  val agentName = utils.objectName(this)

  def main(args: Array[String]) = {
    val etlPlan =
    if (args.size==0)
      for (
        delta <- DaysDelta.loadFromDb(agentName) map DaysDelta.pushLeft(5);
        plan <- prepareEtl(agentName, delta, walkupETL)(chunkSize = 1, convertor = daysToOrganicRunConvertor, exceptionToResult = myExceptionToResult)
      ) yield plan
    else
      for (
        delta <- Reader(runToOrganicRunConvertor(args.head.split(",")));
        plan <- prepareEtl(agentName, delta, walkupETL)(chunkSize = 1, exceptionToResult = myExceptionToResult)
      ) yield plan

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    println(res)
  }

}

/*
import analytics.tiger.DiscreteDelta
import analytics.tiger.metrics.metricUtils.runItem
import analytics.tiger.ETL._
import analytics.tiger.agents.Walkup
import analytics.tiger.utils

//val delta = new DiscreteDelta(Set(runItem("150522_NS500103_0118_AHCFHTBGXX","SL-NXA")))

val agentName = "analytics.tiger.WalkupMetricsAgent.TEST"
val delta = new DiscreteDelta(Set(runItem("170510_NS500103_0730_AHWCK7BGXY","SL-NXA")))
val etlPlan = prepareEtl(agentName, delta, walkupETL)(chunkSize=1/*, convertor = daysToOrganicRunConvertor*/)
utils.CognosDB.apply(etlPlan)
*/
