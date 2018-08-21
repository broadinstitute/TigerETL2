package analytics.tiger.agents.spark2

import java.sql.Timestamp

import analytics.tiger.ETL.{defaultErrorEmailer, etlType, prepareEtl}
import analytics.tiger.{MillisDelta, Reader, etlMessage, utils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone, Seconds}
import scalikejdbc.DBSession

import scala.concurrent.{Await, Future, duration}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
/**
  * Created by atanas on 8/30/2017.
  */
object ArraysQc {

  val NYTZ = DateTimeZone.forID("America/New_York")
  val utcToLocalTimeConv = (df: DataFrame) => (row: Row) => df.schema.zipWithIndex.map {
    case (StructField(_, DataTypes.TimestampType, _, _), ind) =>
      val time = row.getTimestamp(ind)
      if (time == null) null
      else new Timestamp(time.getTime + NYTZ.getOffset(time.getTime))
    case (_, ind) => row.get(ind)
  }

  def refreshTable(tableName: String, pk: Seq[String])(df: DataFrame)(session: DBSession) = Try {
    implicit val sess = session
    import scalikejdbc._
    val start = DateTime.now()
    val data = df.collect
    val conv = utcToLocalTimeConv(df)
    SQL(s"DELETE FROM $tableName WHERE ${pk.map(it => s"$it=?").mkString(" AND ")}").batch(data.map(row => pk.map(pkit => row.getAs[Any](pkit))): _*).apply()
    val stmt = SQL(s"INSERT INTO $tableName VALUES(${df.schema.map(_ => "?").mkString(",")})")
    val count =
      try stmt.batch(data.map(conv): _*).apply().size
      catch {
        case e: Exception => throw new RuntimeException(s"$tableName, ${e.getMessage}")
      }
    s"$tableName: $count  (${ Seconds.secondsBetween(start, DateTime.now).getSeconds } seconds)"
  }

  def main(args: Array[String]) {

    def toTimestamp(delta: MillisDelta) = {
      val (d1core, d2core) = delta.unpack
      (new Timestamp(d1core.getMillis), new Timestamp(d2core.getMillis))
    }

    val envString = args(0)
    val env = utils.config.getConfig(s"metricsCloud.$envString")
    val tableFactory = sparkutils.tableDF(utils.configToMap(env.getConfig("connectInfo")))
    val agentName = env.getString("agentName")

    def ArraysQcAgent(spark: SparkSession): etlType[MillisDelta] = delta => session => {

      import spark.implicits._

      val arrayQCReader = Reader((delta: MillisDelta) => {
        val (d1, d2) = toTimestamp(delta)
        tableFactory(spark, "ARRAYS_QC").where($"MODIFIED_AT" >= d1 && $"MODIFIED_AT" < d2).select("ID","CHIP_WELL_BARCODE","SAMPLE_ALIAS","ANALYSIS_VERSION","IS_LATEST","CHIP_TYPE","AUTOCALL_PF","AUTOCALL_DATE","IMAGING_DATE","IS_ZCALLED","AUTOCALL_GENDER","FP_GENDER","REPORTED_GENDER","GENDER_CONCORDANCE_PF","HET_PCT","HET_HOMVAR_RATIO","CLUSTER_FILE_NAME","P95_GREEN","P95_RED","AUTOCALL_VERSION","ZCALL_VERSION","TOTAL_ASSAYS","TOTAL_SNPS","TOTAL_INDELS","NUM_CALLS","NUM_NO_CALLS","NUM_IN_DB_SNP","NOVEL_SNPS","FILTERED_SNPS","PCT_DBSNP","NUM_SINGLETONS","CALL_RATE","CREATED_AT","MODIFIED_AT","scanner_name","NUM_AUTOCALL_CALLS","AUTOCALL_CALL_RATE")
        //SparkMyApp.tableDF("ARRAYS_QC").where("ID = 13156 or ID = 13198").select("ID","CHIP_WELL_BARCODE","SAMPLE_ALIAS","ANALYSIS_VERSION","IS_LATEST","CHIP_TYPE","AUTOCALL_PF","AUTOCALL_DATE","IMAGING_DATE","IS_ZCALLED","AUTOCALL_GENDER","FP_GENDER","REPORTED_GENDER","GENDER_CONCORDANCE_PF","HET_PCT","HET_HOMVAR_RATIO","CLUSTER_FILE_NAME","P95_GREEN","P95_RED","AUTOCALL_VERSION","ZCALL_VERSION","TOTAL_ASSAYS","TOTAL_SNPS","TOTAL_INDELS","NUM_CALLS","NUM_NO_CALLS","NUM_IN_DB_SNP","NOVEL_SNPS","FILTERED_SNPS","PCT_DBSNP","NUM_SINGLETONS","CALL_RATE","CREATED_AT","MODIFIED_AT","scanner_name")
      })

      val aqc_main = arrayQCReader map refreshTable("ANALYTICS.ARRAYS_QC", Seq("CHIP_WELL_BARCODE", "ANALYSIS_VERSION"))

      val aqc_fingerprint = arrayQCReader map { qc =>
        qc.join(
          tableFactory(spark, "ARRAYS_QC_FINGERPRINT").as("tab2").select("ARRAYS_QC_ID", "READ_GROUP", "SAMPLE", "LL_EXPECTED_SAMPLE", "LL_RANDOM_SAMPLE", "LOD_EXPECTED_SAMPLE", "HAPLOTYPES_WITH_GENOTYPES", "HAPLOTYPES_CONFIDENTLY_CHECKED", "HAPLOTYPES_CONFIDENTLY_MATCHIN", "HET_AS_HOM", "HOM_AS_HET", "HOM_AS_OTHER_HOM"),
          $"ID" === $"arrays_qc_id"
        ).select("tab2.*")
      } map refreshTable("ANALYTICS.ARRAYS_QC_FINGERPRINT", Seq("ARRAYS_QC_ID"))

      val aqc_controlcodes = arrayQCReader map { qc =>
        val res = qc.join(
          tableFactory(spark, "ARRAYS_QC_CONTROL_CODES").as("tab2").select("ARRAYS_QC_ID", "CONTROL", "CATEGORY", "RED", "GREEN"),
          $"ID" === $"arrays_qc_id"
        ).select("tab2.*")
        res.explain()
        res
      } map refreshTable("ANALYTICS.ARRAYS_QC_CONTROL_CODES", Seq("ARRAYS_QC_ID", "CONTROL"))

      val aqc_gt_concordance = arrayQCReader map { qc =>
        qc.join(
          tableFactory(spark, "ARRAYS_QC_GT_CONCORDANCE").as("tab2").select("ARRAYS_QC_ID","VARIANT_TYPE","TRUTH_SAMPLE","CALL_SAMPLE","HET_SENSITIVITY","HET_PPV","HET_SPECIFICITY","HOMVAR_SENSITIVITY","HOMVAR_PPV","HOMVAR_SPECIFICITY","VAR_SENSITIVITY","VAR_PPV","VAR_SPECIFICITY","GENOTYPE_CONCORDANCE","NON_REF_GENOTYPE_CONCORDANCE"),
          $"ID" === $"arrays_qc_id"
        ).select("tab2.*")
      } map refreshTable("ANALYTICS.ARRAYS_QC_GT_CONCORDANCE", Seq("ARRAYS_QC_ID"))

      val aqc_blacklisting = Reader((delta: MillisDelta) => {
        val (d1, d2) = toTimestamp(delta)
        tableFactory(spark, "ARRAYS_QC_BLACKLISTING").where($"MODIFIED_AT" >= d1 && $"MODIFIED_AT" < d2).select("CHIP_WELL_BARCODE","BLACKLISTED_ON","BLACKLISTED_BY","WHITELISTED_ON","WHITELISTED_BY","BLACKLIST_REASON","MODIFIED_AT","NOTES")
      }) map refreshTable("ANALYTICS.ARRAYS_QC_BLACKLISTING", Seq("CHIP_WELL_BARCODE","BLACKLIST_REASON"))

      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      val res = Seq(aqc_main, aqc_fingerprint, aqc_gt_concordance, aqc_controlcodes, aqc_blacklisting).foldLeft(Right(""): Either[etlMessage,String]) { case (acc, task) => acc match {
        case Left(_) => acc
        case Right(msg) => Await.result(Future(task(delta)(session)), 5 minutes) match {
          case Success(str) => Right(msg + "\n" + str)
          case Failure(e) => Left(etlMessage(msg + "\n" + e.getMessage))
        }
      }}
      Seq((delta, res))
    }

    val myDB = Map("prod" -> utils.CognosDB, "staging" -> utils.DevDB).get(envString).get

    // create Spark context with Spark configuration
    val sc = new SparkConf().setAppName(s"ArraysQc.$envString")
    if (sc.getOption("spark.master").isEmpty) sc.setMaster("local[2]")
    val spark = SparkSession.builder.config(sc).getOrCreate()

    val etlPlan = for(
      delta <- MillisDelta.loadFromDb(agentName, dbSysdate = _ => new DateTime(tableFactory(spark, "(SELECT CURRENT_TIMESTAMP() - interval 1 MINUTE time) tab").collect()(0).getTimestamp(0).getTime));
      plan <- prepareEtl(agentName, delta, ArraysQcAgent(spark))(chunkSize = 1000*60*60*24*7L)
    ) yield plan

    val res = myDB.apply(etlPlan)
    spark.close()

    defaultErrorEmailer(agentName)(res)
    println(res)

/*
  import analytics.tiger.utils
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import analytics.tiger.agents.spark2.ArraysQc._

  val spark = SparkSession.builder.config(new SparkConf().setMaster("local[2]")).getOrCreate()
  var sqlContext = spark.sqlContext
  val cloudConnectInfo = configToMap(utils.config.getConfig("metricsCloud.prod.connectInfo"))
  val tableDF = getTableDF(spark, cloudConnectInfo) _
  val queryDF = getQueryDF(spark, cloudConnectInfo) _
*/

  }

}
