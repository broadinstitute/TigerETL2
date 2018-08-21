--STATEMENT
MERGE INTO   slxre2_pico_quants p 
USING
(
SELECT
        --PK
        library_name, 
        quant_type,
        --
        run_date, 
        lcset, 
        well, 
        quant_value,
        workflow, 
        protocol, 
        topoffs, 
        bsp_concentration, 
        on_risk, 
        pdo_samples_on_risk,
        risk_types, 
        pdo, 
        pdo_title, 
        pdo_samples, 
        plated_samples, 
        original_lcset,
        gssr_ids, 
        run_id, 
        run_name
    FROM (
        SELECT q.*
        , dense_rank () over (PARTITION BY q.library_name, q.quant_type ORDER BY q.run_date DESC, q.run_id DESC)  myrank
        FROM slxre2_library_pico_quant q )
    WHERE myrank =1


    UNION ALL

    SELECT  
        --PK
        lm.vessel_barcode library_name,
        decode(lm.quant_type, 'POND_PICO', 'Pond Pico', 'CATCH_PICO', 'Catch Pico', 'CDNA_ENRICHED_PICO', 'cDNA Enriched Pico'  ) quant_type,
        --
        lm.run_date,
        cognos.concat_string_csv(DISTINCT lls.batch_name) lcset,  -- this is not parsed from run name as in Squid version
        lm.rack_position                                  well,
        lm.quant_value,

        cognos.concat_string_csv(DISTINCT jira.TYPE)     workflow,
        cognos.concat_string_csv(DISTINCT jira.protocol) protocol,
        cognos.concat_string_csv(DISTINCT jira.topoffs)  topoffs,
        avg(ps.concentration) bsp_concentration,
        CASE
            WHEN count(DISTINCT decode(pdos.ON_RISK, 'T', lls.PRODUCT_ORDER_sample))>0 THEN 'T'
            WHEN  count(DISTINCT decode(pdos.ON_RISK, 'F', lls.PRODUCT_ORDER_sample))>0 THEN 'F'
            ELSE 'N/A'
        END on_risk,
        cognos.concat_string_csv(DISTINCT  decode(pdos.ON_RISK, 'T', lls.PRODUCT_ORDER_sample, NULL)) pdo_samples_on_risk,
        cognos.concat_string_csv(DISTINCT  pdos.risk_types)         risk_types,
        cognos.concat_string_csv(DISTINCT  lls.PRODUCT_ORDER_KEY)   pdo,
        cognos.concat_string_csv(DISTINCT  pdo.title)               pdo_title,
        cognos.concat_string_csv(DISTINCT  lls.PRODUCT_ORDER_sample) pdo_samples,
        cognos.concat_string_csv(DISTINCT  lls.lcset_sample)        plated_samples,
        cognos.concat_string_csv(DISTINCT lls.batch_name)           original_lcset,  -- LLS should have the actual LCSET; there should be  very rare cases when the actual lcset can not be determined
        NULL                                                        gssr_ids,
        0                                                           run_id,
        nvl(lm.run_name, 'Manual entry')                            run_name
    FROM mercurydw.lab_metric lm
    JOIN mercurydw.library_lcset_sample lls             ON     lls.library_label = lm.vessel_barcode
    JOIN jiradwh.loj_issue_lcset@loj_link.seqbldr jira  ON              jira.KEY = lls.batch_name
    LEFT JOIN mercurydw.product_order pdo               ON   pdo.jira_ticket_key = lls.PRODUCT_ORDER_key
    LEFT JOIN mercurydw.product_order_sample pdos       ON pdos.product_order_id = pdo.product_order_id AND pdos.sample_name = lls.PRODUCT_ORDER_sample
    JOIN analytics.bsp_sample ps     					ON    ps.sample_barcode  = lls.lcset_sample
    WHERE
        lm.quant_type IN (
        'CDNA_ENRICHED_PICO',
        'CATCH_PICO',
        'POND_PICO'
        )
        AND lm.run_date >= SYSDATE -365
    GROUP BY lm.vessel_barcode , lm.quant_type, lm.run_date,
        lm.rack_position ,lm.quant_value,lm.run_name
) q 
ON 
    (p.library_name = q.library_name
    AND p.quant_type = q.quant_type
    )
WHEN NOT MATCHED THEN INSERT VALUES (
    q.library_name, q.quant_type, q.run_date, q.lcset, q.well,
    q.quant_value, q.workflow, q.protocol, q.topoffs,
    q.bsp_concentration, q.on_risk, q.pdo_samples_on_risk,
    q.risk_types, q.pdo, q.pdo_title, q.pdo_samples,
    q.plated_samples, q.original_lcset, q.gssr_ids , q.run_id,
    q.run_name
)
WHEN MATCHED THEN UPDATE 
SET 
    p.lcset                 = q.lcset, 
    p.well                  = q.well,
    p.quant_value           = q.quant_value, 
    p.workflow              = q.workflow, 
    p.protocol              = q.protocol, 
    p.topoffs               = q.topoffs,
    p.bsp_concentration     = q.bsp_concentration, 
    p.on_risk               = q.on_risk, 
    p.pdo_samples_on_risk   = q.pdo_samples_on_risk,
    p.risk_types            = q.risk_types, 
    p.pdo                   = q.pdo, 
    p.pdo_title             = q.pdo_title, 
    p.pdo_samples           = q.pdo_samples,
    p.plated_samples        = q.plated_samples, 
    p.original_lcset        = q.original_lcset, 
    p.gssr_ids              = q.gssr_ids , 
    p.run_id                = q.run_id,
    p.run_name              = q.run_name
;
