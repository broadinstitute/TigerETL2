import java.nio.file.Files
import analytics.tiger.ETL._
import analytics.tiger.metrics.metricType
import analytics.tiger.metrics.metricUtils._
import analytics.tiger._
import org.joda.time.DateTime
import scalikejdbc._

import scala.util.Success

case class projectSampleDate(project: String, sample: String, analysisDate: DateTime)
type projectSampleDiscreteDelta = DiscreteDelta[projectSampleDate]

val AccessArrayAgent: ETL.etlType[projectSampleDiscreteDelta] = delta => session => {
  val projectSampleDate(project, sample, analysisDate) = delta.unpack.head
  val groupId = Seq(
    Extractor("PROJECT"     , _ => Success(project)),
    Extractor("SAMPLE_NAME" , _ => Success(sample))
  )
  val fieldExtractors = Seq(
    Extractor("INSERT_DATE"         , _ => Success(new DateTime())),
    Extractor("ANALYSIS_END_DATE"   , _ => Success(analysisDate)),
    Extractor("CHROM"               , it => Success(it(0))),
    Extractor("START_POSITION"      , it => recordableValue(it(1), metricType.Long)),
    Extractor("END_POSITION"        , it => recordableValue(it(2), metricType.Long)),
    Extractor("TARGET_LENGTH"       , it => recordableValue(it(3), metricType.Long)),
    Extractor("TARGET_NAME"         , it => Success(it(4))),
    Extractor("GC_PERCENT"          , it => recordableValue(it(5), metricType.Double)),
    Extractor("MEAN_COVERAGE"       , it => recordableValue(it(6), metricType.Double)),
    Extractor("NORMALIZED_COVERAGE" , it => recordableValue(it(7), metricType.Double))
  )
  val extractors = groupId ++ fieldExtractors
  val tableName = "ACCESS_ARRAY_METRICS"
  val prep_stmt = SQL(insertSQL(tableName, extractors))

  val sampleAdjusted = sample.replace("/", "_")
  val folder = java.nio.file.Paths.get(s"""\\\\iodine-cifs\\seq_picard_aggregation\\$project\\$sampleAdjusted""")
  try {
    val current = folder.resolve(Files.readSymbolicLink(folder.resolve("current")).getFileName)
    val source = scala.io.Source.fromFile(current.resolve(s"$sampleAdjusted.per_target_pcr_metrics").toFile)
    deleteStatement(tableName, groupId).apply()(session)
    source
      .getLines()
      .drop(1)
      .foreach { line =>
        prep_stmt.bind(extractors.map { _.func(line.split("\t")).get }: _*).executeUpdate().apply()(session)
      }
    Seq((delta, Right("OK")))
  } catch {
    case e: Exception => Seq((delta, Right(etlMessage(e.getClass.getCanonicalName + ": " + e.getMessage))))
  }

}

def timeToPSDConvertor(a: MillisDelta)(session: DBSession): DiscreteDelta[projectSampleDate] = {
  implicit val sess = session
  val (d1, d2) = a.unpack
  val res = sql"""
        SELECT DISTINCT project, SAMPLE, analysis_end
        FROM cognos.slxre2_pagg_sample
        WHERE product like 'Fluidigm%'
        AND product IS NOT NULL AND analysis_end IS NOT NULL
        AND timestamp>=?
        """.bind(d1).map{ it => projectSampleDate(it.string(1), it.string(2), it.jodaDateTime(3)) }.list.apply()
  DiscreteDelta(res.toSet)
}

val agentName = "analytics.tiger.AccessArrayAgent"
val etlPlan = for (
  delta <- MillisDelta.loadFromDb(agentName);
  //delta <- new DeltaProvider(_ => discreteDelta(Set(projectSampleDate("C1964", "T93-3T", new DateTime(2015,12,15,0,0,0))))) ;
  plan <- prepareEtl(agentName, delta, AccessArrayAgent)(chunkSize = 1, convertor = timeToPSDConvertor)
) yield plan

val res = utils.CognosDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
