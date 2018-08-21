package analytics.tiger.agents

/**
  * Created by atanas on 10/18/2017.
  */

import analytics.tiger.ETL._
import analytics.tiger._
import analytics.tiger.utils.ConnProvider
import org.joda.time.{DateTime, Minutes, Seconds}
import scalikejdbc._

import scala.util.{Failure, Success, Try}

object DataPump {

  def etl(source: ConnProvider, tasks: Seq[(String, String)]): etlType[DummyDelta] = delta => session => {
    val res = source.apply(Reader(implicit sess =>
      tasks.foldLeft(Success(""): Try[String]){ case (acc, (destTable, sql)) =>
        val start = DateTime.now
        acc.flatMap(msg => Try{
        SQL(s"DELETE FROM $destTable").update().apply()(session)
        var count = 0
        var (extractors, insert) = (Seq.empty[(String, String, WrappedResultSet => Any)], SQL(""))
        SQL(sql).foreach{ row =>
          //row.underlying
          if (extractors.isEmpty) {
            val mtd = row.metaData
            val extractors0 = 1.to(mtd.getColumnCount).map { index =>
              val colName = mtd.getColumnName(index)
              val colType = mtd.getColumnTypeName(index)
              Try((colName, colType, (colType match {
                case "VARCHAR"    => _.stringOpt(index)
                case "TINYINT" => _.booleanOpt(index).map(if (_) 1 else 0)
                case "INTEGER"| "INTEGER UNSIGNED" | "BIT" | "SMALLINT" => _.intOpt(index)
                case "BIGINT" | "BIGINT UNSIGNED" => _.longOpt(index)
                case "DOUBLE"     => _.doubleOpt(index)
                case "TIMESTAMP"  => _.timestampOpt(index)
                case _ => throw new RuntimeException(s"Unknown type: $colType ($colName)")
              }): (WrappedResultSet => Any))
              )
            }
            val failures = extractors0.foldLeft(List.empty[String]){
              case (acc, Failure(e)) => e.getMessage :: acc
              case (acc, Success(_)) => acc
            }
            if (!failures.isEmpty) throw new RuntimeException(failures.mkString("\n","\n","\n"))
            extractors = extractors0.map(_.get)
            insert = SQL(s"INSERT INTO $destTable VALUES(${extractors.map(_ => "?").mkString(",")})")
          }
          val data = extractors.map{it =>
            try it._3(row)
            catch { case e: Exception => println(it._1 + " => " + e.getMessage); throw e }
          }
          try insert.bind(data: _*).update().apply()(session)
          catch { case e: Exception => throw new RuntimeException(e.getMessage + "\n\n" + extractors.map(_._1).zip(data).map(it => it._1+"="+it._2.toString).mkString("\nDATA RECORD:","\n","\n")) }
          count += 1
          if (count%10000 == 0) println(s"$count records processed")
        }
        val end = DateTime.now
        s"$msg\n$destTable : ${extractors.map(it => s"${it._1}: ${it._2}").mkString("[", ", ", "]")}\n$count records, Duration: ${ Minutes.minutesBetween(start, end).getMinutes } minutes, ${ Seconds.secondsBetween(start, end).getSeconds%60 } seconds"
    })}))
    Seq((delta, res match {
      case Success(msg) => Right(msg)
      case Failure(e) => Left(etlMessage(e.getMessage))
    }))
  }

  val agentName = utils.objectName(this)

  def main(args: Array[String]) {
    val mySqlConn = utils.mkConnProvider(args(0), true)
    import collection.JavaConverters._
    val tasks = utils.config.getConfigList(args(2)).asScala.map(it => (it.getString("destinationTable"), it.getString("sql")))
    val etlPlan = etl(mySqlConn, tasks)(dummyDelta)
      //prepareEtl(agentName, dummyDelta, etl(mySqlConn))()
    val res = utils.mkConnProvider(args(1)).apply(etlPlan)
    //defaultErrorEmailer(agentName)(res)
    println(res)
  }

}
