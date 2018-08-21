package analytics.tiger.agents

/**
  * Created by atanas on 5/30/2017.
  */
import analytics.tiger.ETL._
import analytics.tiger._
import scalikejdbc._

object LibrarySampleLcSet {

  val lib_lcset_sql = """
MERGE INTO slxre_library_lcset lib_lcset
USING
    (SELECT
        --PK: Library_name, LSID/GSSR_ID

        s.barcode gssr_id,
        l.NAME                                                          library_name,
        --
        l.creation_time                                                 library_creation,
        cognos.concat_string_csv(DISTINCT lde.lcset)                    lcset,
        cognos.concat_string_csv(DISTINCT nvl(jira.TYPE, lde.workflow)) workflow,
        l.TYPE                                                          library_type,
        cognos.concat_string_csv(DISTINCT sps.pdo)                      pdo,
        cognos.concat_string_csv(DISTINCT sps.pdo_sample_id)            pdo_samples,

        CASE
            WHEN count(DISTINCT decode(sps.on_risk, 'T', sps.pdo_sample_id))>0 THEN 'T'
            WHEN  count(DISTINCT decode(sps.on_risk, 'F', sps.pdo_sample_id))>0 THEN 'F'
            ELSE 'N/A'
        END on_risk,
        cognos.concat_string_csv(decode(sps.on_risk, 'T', sps.pdo_sample_id))            pdo_samples_on_risk,
        cognos.concat_string_csv(DISTINCT sps.risk_types)                                risk_types,
        cognos.concat_string_csv(DISTINCT nvl(sps.plated_sample_id, bsp.sample_barcode)) plated_sample_id,
        cognos.concat_string_csv(DISTINCT mis.NAME)                                      molecular_barcode,
		s.lsid,
        cognos.concat_string_csv(DISTINCT jira.protocol)                                 lcset_protocol,
        cognos.concat_string_csv(DISTINCT jira.topoffs )                                 lcset_topoff,
		bsp.collaborator_sample_id,
        cognos.concat_string_csv(DISTINCT jira.type )                                    lcset_type,
        to_char(null) calibration_version,
        cognos.concat_string_csv(DISTINCT sps.position)                                    position

    FROM slxre_library l
    JOIN seq20.seq_content sc ON sc.name = l.NAME
    JOIN seq20.seq_content_descr_set scds ON scds.seq_content_id = sc.seq_content_id
    JOIN seq20.next_generation_library_descr ngld ON ngld.library_descr_id = scds.seq_content_descr_id
    JOIN seq20.lc_sample s ON s.lc_sample_id = ngld.sample_id AND s.type_id <>14  -- exclude HS oligo pool samples
    LEFT JOIN seq20.molecular_indexing_scheme mis ON mis.id = ngld.molecular_indexing_Scheme_id

    LEFT JOIN (SELECT DISTINCT lde.lc_set lcset, lde.sample_barcode, lde.workflow_name workflow
        FROM  lims_deck_event lde ) lde ON  lde.sample_barcode = s.barcode
	LEFT JOIN jiradwh.loj_issue_lcset@loj_link.seqbldr jira ON jira.KEY = lde.lcset

    LEFT JOIN   (SELECT DISTINCT sps.plated_sample_id, sps.pdo_sample_id, sps.pdo, sps.plated_sample_lsid, pdos.on_risk, pdos.risk_types, sps.concentration, sps.position
            FROM cognos.bsp_plated_samples sps
            JOIN mercurydw.product_order_sample pdos   ON pdos.sample_name = sps.pdo_sample_id
            JOIN mercurydw.product_order pdo           ON pdo.product_order_id = pdos.product_order_id AND pdo.jira_ticket_key = sps.pdo
    ) sps  ON sps.plated_sample_lsid = s.lsid

	LEFT JOIN analytics.bsp_sample bsp ON bsp.sample_lsid = s.lsid
    WHERE
        l.creation_time >= ?
        AND l.creation_time < ?
    GROUP BY l.library_id, s.barcode , s.lsid, bsp.collaborator_sample_id, l.NAME , l.creation_time , --lde.lcset, nvl(jira.TYPE, lde.workflow) ,
	l.type

    UNION ALL

    SELECT
        'NA'                                                    gssr_id,
        lls.library_label                                       library_name,
        lls.library_creation_date                               library_creation ,
        concat_string_csv(DISTINCT lls.batch_name)              lcset,
        concat_string_csv(DISTINCT jira.TYPE)                   workflow,
        lls.library_type                                        library_type,
        concat_string_csv(DISTINCT lls.product_order_key)       pdo,
        concat_string_csv(DISTINCT lls.product_order_sample )   pdo_samples,

        CASE
            WHEN count(DISTINCT decode(pdos.on_risk, 'T', pdos.sample_name))>0 THEN 'T'
            WHEN  count(DISTINCT decode(pdos.on_risk, 'F', pdos.sample_name))>0 THEN 'F'
            ELSE 'N/A'
        END                                                     on_risk,
        cognos.concat_string_csv(decode(pdos.on_risk, 'T', pdos.sample_name))   pdo_samples_on_risk,
        concat_string_csv(DISTINCT pdos.risk_types)                             risk_types,
        concat_string_csv(DISTINCT lls.lcset_sample)                            plated_sample_id ,
        concat_string_csv(DISTINCT lls.molecular_barcode)                       molecular_barcode,
        s.sample_lsid                                                           lsid,
        concat_string_csv(DISTINCT jira.protocol)                               lcset_protocol,
        concat_string_csv(DISTINCT jira.topoffs)                                lcset_topoff,
        s.collaborator_sample_id                                                collaborator_sample_id,
        concat_string_csv(DISTINCT jira.type)                                   lcset_type,
        concat_string_csv(DISTINCT lls.calibration_version)                     calibration_version, -- to be obtained from lls.calibration_version
        concat_string_csv(DISTINCT lls.position)                                position
    FROM mercurydw.library_lcset_sample lls
    LEFT JOIN jiradwh.loj_issue_lcset@loj_link.seqbldr jira ON jira.KEY = lls.batch_name
    LEFT JOIN mercurydw.product_order pdo ON  pdo.jira_ticket_key = lls.product_order_key
    LEFT JOIN mercurydw.product_order_sample pdos ON
        pdos.product_order_id = pdo.product_order_id AND
        pdos.sample_name = lls.product_order_sample
    JOIN analytics.bsp_sample s  ON s.sample_barcode = lls.lcset_sample
    WHERE
        lls.library_creation_date >= ?
        AND lls.library_creation_date < ?
    GROUP BY lls.library_label, lls.library_creation_date ,lls.library_type, s.sample_lsid, s.collaborator_sample_id

	) DELTA
    ON (lib_lcset.library_name = DELTA.library_name
        AND lib_lcset.lsid = DELTA.lsid
       )

WHEN NOT MATCHED THEN INSERT
    VALUES (

    DELTA.gssr_id,
    DELTA.library_name,
    DELTA.library_creation,
    DELTA.lcset,
    DELTA.workflow,
    SYSDATE,
    DELTA.library_type,
    DELTA.pdo,
    DELTA.pdo_samples,
    DELTA.on_risk,
    DELTA.pdo_samples_on_risk,
    DELTA.risk_types,
		DELTA.plated_sample_id,
		DELTA.molecular_barcode,
		DELTA.lsid,
		DELTA.lcset_protocol,
		DELTA.lcset_topoff,
		DELTA.collaborator_sample_id,
    DELTA.lcset_type,
    DELTA.calibration_version,
    DELTA.position
    )
WHEN MATCHED THEN UPDATE SET
        lib_lcset.pdo                   = DELTA.pdo,
        lib_lcset.pdo_samples           = DELTA.pdo_samples,
        lib_lcset.on_risk               = DELTA.on_risk,
        lib_lcset.pdo_samples_on_risk   = DELTA.pdo_samples_on_risk,
        lib_lcset.risk_types            = DELTA.risk_types,
		lib_lcset.plated_sample_id 		= DELTA.plated_sample_id,
		lib_lcset.molecular_barcode 	= DELTA.molecular_barcode,
		lib_lcset.workflow 				= DELTA.workflow,
		lib_lcset.lcset 				= DELTA.lcset,
		lib_lcset.lcset_protocol		= DELTA.lcset_protocol,
		lib_lcset.lcset_topoff			= DELTA.lcset_topoff,
        lib_lcset.lcset_type            = DELTA.lcset_type,
        lib_lcset.calibration_version   = DELTA.calibration_version,
        lib_lcset.insert_date           = SYSDATE,
        lib_lcset.position              = DELTA.position
"""

  val agentName = utils.objectName(this)

  def etl: ETL.etlType[MillisDelta] = delta => session => {
    val (d1, d2) = delta.unpack
    val records = SQL(lib_lcset_sql).bind(d1, d2, d1, d2).executeUpdate().apply()(session)
    Seq((delta, Right(s"libs processed: $records")))
  }

  def main(args: Array[String]) {
    val etlPlan = for (
      delta <- MillisDelta.loadFromDb(agentName) map MillisDelta.pushLeft(30L * 24 * 60 * 60 * 1000);
      plan <- prepareEtl(agentName, delta, etl)()
    ) yield plan
    val res = utils.CognosDB.apply(etlPlan)
    print(res)
    defaultErrorEmailer(agentName)(res)
  }

}
