package analytics.tiger.agents.spark2

import org.apache.spark.sql.SparkSession

/**
  * Created by atanas on 9/18/2017.
  */
object sparkutils {

  val tableDF = (connectInfo: Map[String, String]) => (spark: SparkSession, table: String) => spark.read.format("jdbc").options(connectInfo + ("dbtable" -> table)).load()


}
