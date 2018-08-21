package analytics.tiger.agents.spark2

import java.sql.Timestamp
import analytics.tiger.ETL.{defaultErrorEmailer, etlType, prepareEtl}
import analytics.tiger.{MillisDelta, Reader, etlMessage, utils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by atanas on 9/21/2017.
  */
object ArraysQcChecker {

  def checker(spark: SparkSession): etlType[MillisDelta] = delta => session => {
    val (a1, a2) = delta.unpack
    val (d1, d2) = (new Timestamp(a1.getMillis), new Timestamp(a2.getMillis))

    val List(cloud, local) = List("metricsCloud.prod.connectInfo","db.default") map utils.config.getConfig map utils.configToMap map sparkutils.tableDF
    val arrqc = cloud(spark, "ARRAYS_QC")
    val codes = cloud(spark, "ARRAYS_QC_CONTROL_CODES")
    val codesLocal = local(spark, "ANALYTICS.ARRAYS_QC_CONTROL_CODES")
    val missingCodes =
      arrqc
        .join(codes, arrqc("ID") === codes("ARRAYS_QC_ID"))
        .join(codesLocal, codesLocal("ARRAYS_QC_ID") === codes("ARRAYS_QC_ID") and codesLocal("CONTROL") === codes("CONTROL"), "leftouter")
        .where(arrqc("MODIFIED_AT") >= d1 and arrqc("MODIFIED_AT") < d2 and codesLocal("ARRAYS_QC_ID").isNull)
        .select(codes("ARRAYS_QC_ID"), codes("CONTROL"))
        .collect()
    val res = if (missingCodes.isEmpty) Right("OK")
    else Left(etlMessage("Missing pairs (arrays_id, control): " + missingCodes.map(it => (it.getInt(0), it.getString(1))).mkString))
    Seq((delta, res))
  }

  def main(args: Array[String]): Unit = {
    // create Spark context with Spark configuration
    val agentName = utils.objectName(this)

    val mins = (new DateTime().getMillis / 1000 / 60).toInt
    val now = new DateTime(0).plusMinutes((mins/15)*15 - 1)
    val delta = MillisDelta(now.minusYears(1), now)

    val sc = new SparkConf().setAppName(s"ArraysQc.Checker")
    if (sc.getOption("spark.master").isEmpty) sc.setMaster("local[2]")
    val spark = SparkSession.builder.config(sc).getOrCreate()
    val res = utils.CognosDB.apply(prepareEtl(agentName, delta, checker(spark))())
    spark.close()

    defaultErrorEmailer(agentName)(res)
    println(res)

  }

}
