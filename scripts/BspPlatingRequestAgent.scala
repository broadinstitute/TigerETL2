import analytics.tiger.ETL._
import analytics.tiger._
import scalikejdbc._

import scala.util.{Failure, Success, Try}
import scala.xml.XML

val wrItemsSql =
  SQL(
    """
      |SELECT
      |    --    wr.work_request_id,
      |    --    wr.work_request_name,
      |    --    --wr.work_request_data
      |    --    wr.completed_on,
      |
      |    wri.work_request_item_id,
      |    wri.work_request_item_type,
      |    wri.work_request_item_data
      |
      |FROM bsp.bsp_work_request wr
      |JOIN bsp.bsp_work_request_item wri ON wri.work_request_id=wr.work_request_id
      |WHERE wr.work_request_status='FINISHED'
      |    AND wri.work_request_item_type IN ('SEQ_PLATING_REQUEST','GAP_PLATING_REQUEST') --KIT_REQUEST,FINGERPRINT_REQUEST
      |    AND ((TRUNC(wr.completed_on) >= ? AND TRUNC(wr.completed_on) < ? ) OR
      |    wri.work_request_item_id IN (
      |        SELECT DISTINCT wrt.work_request_item_id
      |        FROM bsp.bsp_wr_task_sample tss
      |        JOIN bsp.bsp_work_request_task wrt ON wrt.work_request_task_id=tss.work_request_task_id
      |        JOIN bsp_plating_special_refreshes sp ON sp.sample_id=tss.sample_id
      |        --WHERE tss.sample_id='6E3K2'
      |        )
      |    )
      |
      |    --AND wri.work_request_item_id=9156 --15107 --14002
      |    --AND wri.work_request_id=14826
      |
    """.stripMargin)

val mergePrepStat = SQL(
  """
    |MERGE INTO BSP_WR_ITEM_DATA tr
    |USING(SELECT ? WORK_REQUEST_ITEM_ID,? SAMPLE_ID,? PDO,? PLATE_NAME,? WELL_POSITION,? CONCENTRATION,? VOLUME,? ALIQUOT_TYPE,? PLATING_ISSUES,? WORK_REQUEST_ITEM_TYPE,SYSDATE TIMESTAMP FROM dual)
    |delta ON (tr.WORK_REQUEST_ITEM_ID=delta.WORK_REQUEST_ITEM_ID AND tr.SAMPLE_ID=delta.SAMPLE_ID)
    |WHEN NOT MATCHED THEN INSERT VALUES(delta.WORK_REQUEST_ITEM_ID,delta.SAMPLE_ID,delta.PDO,delta.PLATE_NAME,delta.WELL_POSITION,delta.CONCENTRATION,delta.VOLUME,delta.ALIQUOT_TYPE,delta.PLATING_ISSUES,delta.WORK_REQUEST_ITEM_TYPE,delta.TIMESTAMP)
    |WHEN MATCHED THEN UPDATE SET PDO=delta.PDO,PLATE_NAME=delta.PLATE_NAME,WELL_POSITION=delta.WELL_POSITION,CONCENTRATION=delta.CONCENTRATION,VOLUME=delta.VOLUME,ALIQUOT_TYPE=delta.ALIQUOT_TYPE,PLATING_ISSUES=delta.PLATING_ISSUES,WORK_REQUEST_ITEM_TYPE=delta.WORK_REQUEST_ITEM_TYPE,TIMESTAMP=delta.TIMESTAMP""".stripMargin)

val platedSamplesAgent: ETL.etlType[DaysDelta] = delta => session => try {
  implicit val sess = session
  val (d1, d2) = delta.unpack
  val rs = wrItemsSql.bind(d1, d2).map(it => (it.long(1), it.string(2), it.characterStream(3))).list().apply()
  val res = rs map{ case (work_request_item_id, wr_item_type, data) => (work_request_item_id, Try {
    val xml = XML.load(data)
    val productOrderId = (xml \ "productOrderId").text
    val plateName = (xml \ "plateName").text

    val samplePdoMap = (xml \ "sampleProductOrderMap" \ "entry") map {
      case <entry>{_}<string>{string1}</string>{_}<string>{string2}</string>{_}</entry> => string1.text -> string2.text
    } toMap

    val plateMap = (xml \ "plateNameMap" \ "entry") map {
      case <entry>{_}<int>{item_int}</int>{_}<string>{item_string}</string>{_}</entry> => item_int.text -> item_string.text
    } toMap

    (xml \ "placementList" \ "placementInfo" \ "edu.mit.broad.bsp.core.business.plating.PlacementInfo") filter (node => (node \ "sampleId").text != "") foreach {node =>
      val generatedPlateNumber = (node \ "generatedPlateNumber").text
      val sampleId = (node \ "sampleId").text
      mergePrepStat.bind(
        work_request_item_id,
        sampleId,
        samplePdoMap.get(sampleId).getOrElse(productOrderId),
        plateMap.get(generatedPlateNumber).getOrElse(plateName + generatedPlateNumber),
        node \ "generatedPosition"   text,
        node \ "concentration"       text,
        node \ "volume"              text,
        node \ "aliquotType"         text,
        (node \ "platingIssues" \ "edu.mit.broad.bsp.core.business.plating.PlatingIssue").map(_.text).mkString(","),
        wr_item_type
      ).executeUpdate().apply()
    }
  })}

  val err = res flatMap {
    case (work_request_item_id, Success(_)) => Nil
    case (work_request_item_id, Failure(e)) => Seq(s"work_request_item_id=$work_request_item_id, ${e.getMessage}")
  }

  if (!err.isEmpty) throw new RuntimeException(err mkString("\n\n"))

  sql"BEGIN ETL.BSP_PLATED_SAMPLES_LOAD(?,?) ; END;".bind(d1, d2).execute().apply()
  SQL("""
        |BEGIN
        |FOR rec IN (
        |    SELECT DISTINCT sp.sample_id
        |    FROM bsp.bsp_wr_task_sample tss
        |    JOIN bsp.bsp_work_request_task wrt ON wrt.work_request_task_id=tss.work_request_task_id
        |    JOIN bsp_plating_special_refreshes sp ON sp.sample_id=tss.sample_id
        |    JOIN bsp_wr_item_data wrid ON wrid.work_request_item_id=wrt.work_request_item_id
        |) LOOP
        |    DELETE FROM bsp_plating_special_refreshes WHERE sample_id=rec.sample_id ;
        |END LOOP ;
        |END ;
      """.stripMargin).execute().apply()

  Seq((delta, Right(s"${res.size} items processed.")))
} catch {
  case e: Exception => Seq((delta, Left(etlMessage(e.getMessage))))
}

val agentName = "analytics.tiger.BspPlatingRequestAgent"
val etlPlan = for (
  delta <- DaysDelta.loadFromDb(agentName, propLink = "@SEQPROD.COGNOS");
  plan <- prepareEtl(agentName, delta, platedSamplesAgent)(propLink = "@SEQPROD.COGNOS")
) yield plan
val res = utils.BspDB.apply(etlPlan)
defaultErrorEmailer(agentName)(res)
println(res)
