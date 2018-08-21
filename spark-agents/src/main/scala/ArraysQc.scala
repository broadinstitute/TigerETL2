package analytics.tiger.agents.spark

/**
  * Created by atanas on 5/31/2017.
  */
import analytics.tiger.ETL._
import analytics.tiger.{MillisDelta, utils, Reader, etlMessage}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp

import scalikejdbc.DBSession
import scala.util.{Failure, Success, Try}

object ArraysQc {

  import collection.JavaConverters._

  def configToMap(configName: String) = utils.config.getConfig(configName).entrySet().asScala.toList.map(it => it.getKey -> it.getValue.unwrapped.toString).toMap

  val keyStore = System.getProperty("javax.net.ssl.keyStore")
  val spark = SparkSession.builder.config(new SparkConf().setAll(configToMap("spark"))).getOrCreate()
  val (myDB, cloudConnectInfo, agentName) =
    if      (keyStore.contains("broad-gotc-prod"   )) (utils.CognosDB, configToMap(s"metricsCloud.prod"   ), "analytics.tiger.agents.spark.ArraysQcProd")
    else if (keyStore.contains("broad-gotc-staging")) (utils.DevDB   , configToMap(s"metricsCloud.staging"), "analytics.tiger.agents.spark.ArraysQcStaging")
    else throw new RuntimeException("No destination defined.")

  def tableDF(table: String) = spark.read.format("jdbc").options(cloudConnectInfo + ("dbtable" -> table)).load()
  def queryDF(sql: String) = tableDF(s"($sql) tab")

  // println(Seq("ARRAYS_QC","ARRAYS_QC_FINGERPRINT","ARRAYS_QC_GT_CONCORDANCE").map(ddl(_)).mkString)
  // println(ddl("ARRAYS_QC_BLACKLISTING"))

  /*
DELETE FROM ANALYTICS.arrays_qc ;
DELETE FROM ANALYTICS.arrays_qc_blacklisting ;
DELETE FROM ANALYTICS.arrays_qc_control_codes ;
DELETE FROM ANALYTICS.arrays_qc_fingerprint ;
DELETE FROM ANALYTICS.arrays_qc_gt_concordance ;
*/

  def ddl(tableName: String) = {
    object double  { def unapply(s: String) = """(?:double)\((\d+),(\d+)\)""".r.unapplySeq(s).map(it => (it(0), it(1))) }
    object int     { def unapply(s: String) = """(?:int)\((\d+)\)""".r.unapplySeq(s).map(_.head.toInt) }
    object bit     { def unapply(s: String) = """(?:bit)\((\d+)\)""".r.unapplySeq(s).map(_.head.toInt) }
    object tinyint { def unapply(s: String) = """(?:tinyint)\((\d+)\)""".r.unapplySeq(s).map(_.head.toInt) }
    object bigint  { def unapply(s: String) = """(?:bigint)\((\d+)\)""".r.unapplySeq(s).map(_.head.toInt) }
    object varchar { def unapply(s: String) = """(?:varchar)\((\d+)\)""".r.unapplySeq(s).map(_.head.toInt) }

    val metadata = queryDF(s"SELECT * FROM information_schema.columns\nWHERE table_schema = DATABASE()\nand table_name = '$tableName'\nORDER BY table_name, ordinal_position").cache()
    import spark.implicits._

    val tableItems = metadata.map{ it => {
      val colName = it.getString(it.fieldIndex("COLUMN_NAME"))
      val t = it.getString(it.fieldIndex("COLUMN_TYPE")) match {
        case varchar(pr)    => (s"VARCHAR2($pr BYTE)", "String")
        case tinyint(1)     => ("NUMBER(1)"          , "Boolean")
        case bit(1)         => ("NUMBER(1)"          , "Boolean")
        case int(pr)        => (s"NUMBER($pr)"       , "Int")
        case bigint(pr)     => (s"NUMBER($pr)"       , "BigInt")
        case double(pr,sc)  => (s"NUMBER($pr,$sc)"   , "Double")
        case "timestamp"    => ("timestamp"          , "java.sql.Timestamp")
        case "datetime"     => ("timestamp"          , "java.sql.Timestamp")
        case it => throw new RuntimeException(s"Unknown columnType: $it")
      }
      (
        f"$colName%-30s  " + t._1 + (if (it.getString(it.fieldIndex("IS_NULLABLE"))=="NO") " NOT NULL" else ""),
        f"$colName%-30s  : " + t._2,
        colName
      )
    }}.collect()

    s"\n--SELECT ${tableItems.map(_._3).mkString(",")}" +
      s"\n--SIZE:${tableItems.size}" +
      tableItems.map(_._1).mkString(s"\nCREATE TABLE $tableName(\n", ",\n", ")\n;\n") +
      queryDF(s"\nSELECT index_name, non_unique, column_name, seq_in_index FROM INFORMATION_SCHEMA.STATISTICS where TABLE_NAME='$tableName'").cache().collect().groupBy(row => (row.getString(0), row.getLong(1) == 0)).map { case ((name,unique), cols) => s"ALTER TABLE $tableName ADD ${
        if (name=="PRIMARY") "PRIMARY KEY"
        else s"CONSTRAINT $name ${if (unique) s"UNIQUE" else ""}"
      } (${cols.map(_.getString(2)).mkString(",")});"}.mkString("\n") +
      s"\nGRANT SELECT ON $tableName TO metrics_reader;" +
      s"\nGRANT ALL ON $tableName TO cognos;"
  }

  def toTimestamp(delta: MillisDelta) = {
    val (d1core, d2core) = delta.unpack
    (new Timestamp(d1core.getMillis), new Timestamp(d2core.getMillis))
  }

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
    val data = df.collect
    val conv = utcToLocalTimeConv(df)
    SQL(s"DELETE FROM $tableName WHERE ${pk.map(it => s"$it=?").mkString(" AND ")}").batch(data.map(row => pk.map(pkit => row.getAs[Any](pkit))): _*).apply()
    val count =
      try SQL(s"INSERT INTO $tableName VALUES(${df.schema.map(_ => "?").mkString(",")})").batch(data.map(conv): _*).apply().size
      catch {
        case e: Exception => throw new RuntimeException(s"$tableName, ${e.getMessage}")
      }
    s"$tableName: $count"
  }

  import spark.implicits._

  val arrayQCReader = Reader((delta: MillisDelta) => {
    val (d1, d2) = toTimestamp(delta)
    tableDF("ARRAYS_QC").where($"MODIFIED_AT" >= d1 && $"MODIFIED_AT" < d2).select("ID","CHIP_WELL_BARCODE","SAMPLE_ALIAS","ANALYSIS_VERSION","IS_LATEST","CHIP_TYPE","AUTOCALL_PF","AUTOCALL_DATE","IMAGING_DATE","IS_ZCALLED","AUTOCALL_GENDER","FP_GENDER","REPORTED_GENDER","GENDER_CONCORDANCE_PF","HET_PCT","HET_HOMVAR_RATIO","CLUSTER_FILE_NAME","P95_GREEN","P95_RED","AUTOCALL_VERSION","ZCALL_VERSION","TOTAL_ASSAYS","TOTAL_SNPS","TOTAL_INDELS","NUM_CALLS","NUM_NO_CALLS","NUM_IN_DB_SNP","NOVEL_SNPS","FILTERED_SNPS","PCT_DBSNP","NUM_SINGLETONS","CALL_RATE","CREATED_AT","MODIFIED_AT","scanner_name","NUM_AUTOCALL_CALLS","AUTOCALL_CALL_RATE")
    //SparkMyApp.tableDF("ARRAYS_QC").where("ID = 13156 or ID = 13198").select("ID","CHIP_WELL_BARCODE","SAMPLE_ALIAS","ANALYSIS_VERSION","IS_LATEST","CHIP_TYPE","AUTOCALL_PF","AUTOCALL_DATE","IMAGING_DATE","IS_ZCALLED","AUTOCALL_GENDER","FP_GENDER","REPORTED_GENDER","GENDER_CONCORDANCE_PF","HET_PCT","HET_HOMVAR_RATIO","CLUSTER_FILE_NAME","P95_GREEN","P95_RED","AUTOCALL_VERSION","ZCALL_VERSION","TOTAL_ASSAYS","TOTAL_SNPS","TOTAL_INDELS","NUM_CALLS","NUM_NO_CALLS","NUM_IN_DB_SNP","NOVEL_SNPS","FILTERED_SNPS","PCT_DBSNP","NUM_SINGLETONS","CALL_RATE","CREATED_AT","MODIFIED_AT","scanner_name")
  })

  val aqc_main = arrayQCReader map refreshTable("ANALYTICS.ARRAYS_QC", Seq("CHIP_WELL_BARCODE", "ANALYSIS_VERSION"))

  val aqc_fingerprint = arrayQCReader map { qc =>
    qc.join(
      tableDF("ARRAYS_QC_FINGERPRINT").as("tab2").select("ARRAYS_QC_ID", "READ_GROUP", "SAMPLE", "LL_EXPECTED_SAMPLE", "LL_RANDOM_SAMPLE", "LOD_EXPECTED_SAMPLE", "HAPLOTYPES_WITH_GENOTYPES", "HAPLOTYPES_CONFIDENTLY_CHECKED", "HAPLOTYPES_CONFIDENTLY_MATCHIN", "HET_AS_HOM", "HOM_AS_HET", "HOM_AS_OTHER_HOM"),
      $"ID" === $"arrays_qc_id"
    ).select("tab2.*")
  } map refreshTable("ANALYTICS.ARRAYS_QC_FINGERPRINT", Seq("ARRAYS_QC_ID"))

  val aqc_controlcodes = arrayQCReader map { qc =>
    qc.join(
      tableDF("ARRAYS_QC_CONTROL_CODES").as("tab2").select("ARRAYS_QC_ID", "CONTROL", "CATEGORY", "RED", "GREEN"),
      $"ID" === $"arrays_qc_id"
    ).select("tab2.*")
  } map refreshTable("ANALYTICS.ARRAYS_QC_CONTROL_CODES", Seq("ARRAYS_QC_ID", "CONTROL"))

  val aqc_gt_concordance = arrayQCReader map { qc =>
    qc.join(
      tableDF("ARRAYS_QC_GT_CONCORDANCE").as("tab2").select("ARRAYS_QC_ID","VARIANT_TYPE","TRUTH_SAMPLE","CALL_SAMPLE","HET_SENSITIVITY","HET_PPV","HET_SPECIFICITY","HOMVAR_SENSITIVITY","HOMVAR_PPV","HOMVAR_SPECIFICITY","VAR_SENSITIVITY","VAR_PPV","VAR_SPECIFICITY","GENOTYPE_CONCORDANCE","NON_REF_GENOTYPE_CONCORDANCE"),
      $"ID" === $"arrays_qc_id"
    ).select("tab2.*")
  } map refreshTable("ANALYTICS.ARRAYS_QC_GT_CONCORDANCE", Seq("ARRAYS_QC_ID"))

  val aqc_blacklisting = Reader((delta: MillisDelta) => {
    val (d1, d2) = toTimestamp(delta)
    tableDF("ARRAYS_QC_BLACKLISTING").where($"MODIFIED_AT" >= d1 && $"MODIFIED_AT" < d2).select("CHIP_WELL_BARCODE","BLACKLISTED_ON","BLACKLISTED_BY","WHITELISTED_ON","WHITELISTED_BY","BLACKLIST_REASON","MODIFIED_AT","NOTES")
  }) map refreshTable("ANALYTICS.ARRAYS_QC_BLACKLISTING", Seq("CHIP_WELL_BARCODE","BLACKLIST_REASON"))

  val ArraysQcAgent: etlType[MillisDelta] = delta => session => {
    val res = Seq(aqc_main, aqc_fingerprint, aqc_gt_concordance, aqc_controlcodes, aqc_blacklisting).foldLeft(Right(""): Either[etlMessage,String]) { case (acc, task) => acc match {
      case Left(_) => acc
      case Right(msg) => task(delta)(session) match {
        case Success(str) => Right(msg + "\n" + str)
        case Failure(e) => Left(etlMessage(msg + "\n" + e.getMessage))
      }
    }}
    Seq((delta, res))
  }

  def main(args: Array[String]): Unit = {
    val res =
      myDB.apply(MillisDelta.loadFromDb(agentName, dbSysdate = _ => new DateTime(queryDF("SELECT CURRENT_TIMESTAMP() time").collect()(0).getTimestamp(0).getTime))
        flatMap (prepareEtl(agentName, _, ArraysQcAgent)())
      )

    defaultErrorEmailer(agentName)(res)
    println(res)

  }

}


