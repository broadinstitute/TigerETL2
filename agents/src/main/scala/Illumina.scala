/**
  * Created by atanas on 6/26/2017.
  */
package analytics.tiger.agents

import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.metrics._
import analytics.tiger.metrics.metricUtils._

object Illumina {

  val metricsFromSummary = (run: runItem) => {
    val m = new metricUtils.metrics(scala.io.Source.fromFile(scala.reflect.io.File.apply(rootFolder / "proc" / run.instrument / run.runName / "dashboard/summary.csv").toFile.path).getLines().toList)
    val res = sectionsUnion(m.regularReads).select(Seq("Section Name", "Lane", "Surface", "Density", "Cluster PF", "Legacy Phasing/Prephasing Rate", "Reads PF", "%>=Q30", "Intensity C1", "Reads", "Yield"))
    (List.empty[String], res.data.filter(_(2) == "-").map{ it => Array(
      it(0).substring(5),
      it(1),
      it(3).split(" ")(0),
      it(4).split(" ")(0),
      it(5).split(" / ")(0),
      it(5).split(" / ")(1),
      it(6),
      it(7),
      it(8).split(" ")(0),
      it(9),
      it(10)
    )}.toStream)
  }

  import analytics.tiger.metrics.metricType._
  val tasks = Seq(
    importTask(
      "ANALYTICS.ILLUMINA_READ",
      metricsFromSummary,
      Seq(Long, Long, Double, Double, Double, Double, Double, Double, Double, Double, Double)
    ),

    sqlTask("ANALYTICS.ILLUMINA_LANE_AGGREGATE DELETE", "DELETE FROM ANALYTICS.illumina_lane_aggregate WHERE run_name=?"),

    sqlTask("ANALYTICS.ILLUMINA_LANE_AGGREGATE INSERT",
      """
        |INSERT INTO ANALYTICS.illumina_lane_aggregate
        |SELECT
        |    --PK
        |    a.run_name,
        |    a.lane,
        |    --
        |
        |    avg(a.density) cluster_density,
        |    sum(a.cluster_pf) pct_cluster_pf_per_tile,
        |    max(decode(a.READ, 1, a.phas, to_number(NULL))) r1_pct_phasing_applied,
        |    max(decode(a.READ, 1, to_number(NULL), a.phas)) r2_pct_phasing_applied,
        |    max(decode(a.READ, 1, a.prephas, to_number(NULL))) r1_pct_prephasing_applied,
        |    max(decode(a.READ, 1, to_number(NULL), a.prephas)) r2_pct_prephasing_applied,
        |
        |    sum(a.reads_pf)*1e6 lane_reads_pf, -- not sure if we need this by read
        |    max(decode(a.READ, 1, a.q30, to_number(NULL))) r1_pct_q30,
        |    max(decode(a.READ, 1, to_number(NULL), a.q30)) r2_pct_q30,
        |    sum(a.reads*a.q30)/sum(a.reads) lane_pct_q30,
        |
        |    max(decode(a.READ, 1, a.reads, to_number(NULL)))*1e6 r1_reads,
        |    max(decode(a.READ, 1, to_number(NULL), a.reads))*1e6 r2_reads,
        |    sum(a.reads)*1e6 lane_reads,
        |
        |    max(decode(a.READ, 1, a.yield_pf, to_number(NULL)))*1e9 r1_pf_bases,
        |    max(decode(a.READ, 1, to_number(NULL), a.yield_pf))*1e9 r2_pf_bases,
        |    sum(a.yield_pf)*1e9 lane_pf_bases,
        |
        |    avg(a.intensity_c1) intensity_c1
        |
        |FROM analytics.illumina_read a
        |WHERE a.run_name=?
        |GROUP BY a.run_name, a.lane
      """.stripMargin)
  )

  def etl: ETL.etlType[DiscreteDelta[runItem]] = delta => session => {
    val run = delta.unpack.head // make sure 'chunkSize=1' is specified when calling prepareETL

    val res =
      if (run.cancelled) Right(etlMessage(s"${run.runName} is cancelled."))
      else tasks map { _.run(run)(session) } reduce mergeResults
    Seq((delta, res))
  }

  val agentName = utils.objectName(this)

  def main(args: Array[String]) = {
    val etlPlan = for (
      delta <- Reader(runToOrganicRunConvertor(args.head.split(","), false));
      plan <- prepareEtl(agentName, delta, etl)(chunkSize = 1)
    ) yield plan

    val res = utils.CognosDB.apply(etlPlan)
    defaultErrorEmailer(agentName)(res)
    println(res)
  }

}
