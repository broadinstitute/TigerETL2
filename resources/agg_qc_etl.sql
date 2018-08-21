--STATEMENT
DELETE FROM COGNOS.slxre2_library_agg_qc NOLOGGING
WHERE analysis_id NOT IN (SELECT id FROM metrics.aggregation/*DBLINK*/)
AND source = /*SOURCE*/
;

--STATEMENT
DELETE FROM COGNOS.slxre2_sample_agg_qc NOLOGGING
WHERE analysis_id NOT IN (SELECT id FROM metrics.aggregation/*DBLINK*/)
AND source = /*SOURCE*/
;

--STATEMENT
DELETE FROM COGNOS.target_lagg_rg nologging
;

--STATEMENT
INSERT INTO COGNOS.target_lagg_rg nologging
    SELECT * FROM (
    WITH
    agg AS (
    SELECT * FROM metrics.aggregation/*DBLINK*/ a WHERE a.LIBRARY IS NOT NULL
    AND (a.project,a.SAMPLE, a.LIBRARY, a.data_type) IN
      (SELECT a.project,a.SAMPLE, a.LIBRARY, a.data_type
      FROM metrics.aggregation/*DBLINK*/ a
      WHERE a.LIBRARY IS NOT NULL
      AND nvl(a.modified_at, a.workflow_end_date) >= /*DELTA_START*/
      AND nvl(a.modified_at, a.workflow_end_date) < /*DELTA_END*/
      )
    )--,

    --lagg_rg AS (
    SELECT
        --PK
        agg.project, agg.SAMPLE, agg.LIBRARY, agg.data_type, agg.version,
        rg.flowcell_barcode, rg.lane, rg.molecular_barcode_name,
        --
        agg.id lagg_analysis_id,
        agg.is_latest,
        rg.aggregation_id sagg_analysis_id

    FROM metrics.aggregation_read_group/*DBLINK*/ rg
    JOIN metrics.aggregation/*DBLINK*/  a  ON a.id = rg.aggregation_id --AND a.is_latest =1

    JOIN agg ON
        agg.project = a.project         AND
        agg.SAMPLE = a.SAMPLE           AND
        agg.LIBRARY = rg.library_name   AND
        agg.data_type = a.data_type     AND
        agg.version = a.version

    )
;

--STATEMENT
MERGE INTO COGNOS.slxre2_library_agg_qc laggqc
USING (
SELECT
    -- PK
    rg.project,
    rg.SAMPLE,
    rg.LIBRARY,
    rg.data_type,
    rg.version,
    --
    rg.is_latest,
    rg.lagg_analysis_id,
    max(pls.concentration                            ) bsp_concentration,
    max(pond_qc.quant_value)                    pond_quant,  -- only 1 Pond is expected 
    max(pond_qc.well)                           pond_qc_well,
    avg(catch_qc.quant_value)                   catch_quant,  -- could be multiple Catches for ExEx
    cognos.concat_string_csv(DISTINCT catch_qc.well)   catch_qc_well,-- may not be correct for ExEx (multiple Catches = multiple wells)

    max(rna_qc.quant_value                           ) cdna_quant,
    max(rna_qc.well                                  ) cdna_well,
    max(rin.rin                           			 ) rin_score,
    ''                                                 rin_well,

to_number(null)				 sage_qpcr_quant,
max(pls.volume)              bsp_volume,
max(rin.rqs                ) rqs,

--max(qpcr.concentration)      agg_library_qpcr
max(qpcr.quant_value )  agg_library_qpcr

FROM  COGNOS.target_lagg_rg rg

LEFT JOIN COGNOS.slxre_readgroup_metadata rgmd ON
    rgmd.flowcell_barcode          = rg.flowcell_barcode AND
    rgmd.lane                      = rg.lane AND
    (rgmd.molecular_indexing_scheme = nvl(rg.molecular_barcode_name,'NULL') OR rgmd.is_greedy = 1) 
	
LEFT JOIN COGNOS.slxre2_rg_ancestry ancestry ON
    ancestry.flowcell_barcode           = rgmd.flowcell_barcode AND
    ancestry.lane                       = rgmd.lane AND
    ancestry.molecular_indexing_scheme  = rgmd.molecular_indexing_scheme  -- ancestry.molecular_indexing_scheme comes from RGMD

LEFT JOIN COGNOS.sample_qc_metrics pond_qc  ON
    pond_qc.content_name = COALESCE(ancestry.pond, ancestry.nextera, ancestry.pcr_plus_pond, substr(rg.LIBRARY, 1, instr(rg.LIBRARY, '_Illumina_P5')-1)) AND
    --(pond_qc.lsid = rgmd.lsid OR 
	pond_qc.plated_sample_id = rgmd.sample_id  AND
	pond_qc.quant_type = 'Pond Pico'

LEFT JOIN COGNOS.sample_qc_metrics catch_qc ON
	instr(COALESCE(ancestry.catch, ancestry.nexome_catch) , catch_qc.content_name)>0 
	AND --(catch_qc.lsid = rgmd.lsid  OR 
	catch_qc.plated_sample_id = rgmd.sample_id --)
    AND catch_qc.quant_type = 'Catch Pico'

LEFT JOIN COGNOS.sample_qc_metrics rna_qc   ON
    rna_qc.content_name = rgmd.LIBRARY  AND
    rna_qc.lsid = rgmd.lsid  AND
    rna_qc.quant_type = 'cDNA Enriched Pico'

LEFT JOIN analytics.bsp_sample rin ON rin.sample_barcode = rgmd.product_order_sample
--rin.sample_id = substr(rgmd.product_order_sample, 4)

LEFT JOIN COGNOS.bsp_plated_samples pls ON
    pls.plated_sample_id = rgmd.sample_id AND
    pls.pdo              = rgmd.product_order_key AND
    pls.pdo_sample_id    = rgmd.product_order_sample AND
    pls.plated_sample_id NOT IN ('SM-5A3LU', 'SM-5A3LX')  --RPT-2146 - duplicate in BSP_Plated_Samples because daughter and parent are in the PDO

--LEFT JOIN slxre_library_qpcr_value qpcr ON qpcr.library_name = rg.library
LEFT JOIN COGNOS.slxre2_library_qpcr qpcr ON qpcr.library_name = COALESCE(ancestry.pcr_plus_norm_pond, ancestry.pcr_free_pond, ancestry.pond )  --nvl(ancestry.pcr_plus_norm_pond, ancestry.pond )

GROUP BY rg.project, rg.SAMPLE, rg.LIBRARY, rg.data_type, rg.version, rg.is_latest, rg.lagg_analysis_id

) DELTA
ON ( laggqc.analysis_id = DELTA.lagg_analysis_id and laggqc.source = /*SOURCE*/)

WHEN NOT MATCHED THEN
INSERT (
    project,
    SAMPLE,
    LIBRARY,
    data_type,
    version,
    bsp_concentration,
    pond_quant,
    pond_qc_well,
    catch_quant,
    catch_qc_well,
    cdna_quant,
    cdna_well,
    rin_score,
    rin_well,
    analysis_id,
    timestamp,
    sage_qpcr_quant,
    bsp_volume ,
	rqs	,
	agg_library_qpcr,
	source
)

VALUES (
    DELTA.project,
    DELTA.SAMPLE,
    DELTA.LIBRARY,
    DELTA.data_type,
    DELTA.version,
    DELTA.bsp_concentration,
    DELTA.pond_quant,
    DELTA.pond_qc_well,
    DELTA.catch_quant,
    DELTA.catch_qc_well,
    DELTA.cdna_quant,
    DELTA.cdna_well,
    DELTA.rin_score,
    DELTA.rin_well,
    DELTA.lagg_analysis_id,
    sysdate,
    DELTA.sage_qpcr_quant,
    DELTA.bsp_volume,
	DELTA.rqs,
	DELTA.agg_library_qpcr,
	/*SOURCE*/
)
WHERE DELTA.is_latest <> 0 --= 1

WHEN MATCHED THEN
UPDATE SET
    laggqc.version         = decode(DELTA.is_latest, 0, 99999, DELTA.version),
    laggqc.bsp_concentration = DELTA.bsp_concentration,
    laggqc.pond_quant      = DELTA.pond_quant,
    laggqc.pond_qc_well    = DELTA.pond_qc_well,
    laggqc.catch_quant     = DELTA.catch_quant,
    laggqc.catch_qc_well   = DELTA.catch_qc_well,
    laggqc.cdna_quant      = DELTA.cdna_quant,
    laggqc.cdna_well       = DELTA.cdna_well,
    laggqc.rin_score       = DELTA.rin_score,
    laggqc.rin_well        = DELTA.rin_well,
    laggqc.rqs       	   = DELTA.rqs,
    laggqc.agg_library_qpcr = DELTA.agg_library_qpcr,
--    laggqc.sage_qpcr_quant = DELTA.sage_qpcr_quant,
    laggqc.bsp_volume      = DELTA.bsp_volume,
    laggqc.TIMESTAMP       = sysdate
DELETE  WHERE laggqc.version = 99999   --DELTA.is_latest=0
;

--STATEMENT
MERGE INTO COGNOS.slxre2_sample_agg_qc sqc
USING (

    WITH
    agg AS (
    SELECT * FROM metrics.aggregation/*DBLINK*/ a WHERE a.LIBRARY IS NULL
    AND (a.project,a.SAMPLE, a.data_type) IN
      (SELECT a.project,a.SAMPLE, a.data_type
      FROM metrics.aggregation/*DBLINK*/ a
      WHERE a.LIBRARY IS NULL 
        AND nvl(a.modified_at, a.workflow_end_date) >= /*DELTA_START*/
        AND nvl(a.modified_at, a.workflow_end_date) < /*DELTA_END*/       
      )
    ),

    sagg_rg AS (
    SELECT
        --PK
        agg.project, agg.SAMPLE, agg.data_type, agg.version,
        rgmd.flowcell_barcode, rgmd.lane, rgmd.molecular_indexing_Scheme,
        --
        agg.id analysis_id,
        agg.is_latest,

        rgmd.sample_id plated_Sample_id,
        rgmd.product_order_key pdo,
        rgmd.product_order_sample pdo_sample ,

        rgmd.lcset,
        rgmd.lsid,
        rgmd.run_date

    FROM metrics.aggregation_read_group/*DBLINK*/ rg
    JOIN agg  ON agg.id = rg.aggregation_id --AND a.is_latest =1
    left JOIN COGNOS.slxre_readgroup_metadata rgmd ON
        rgmd.flowcell_barcode          = rg.flowcell_barcode AND
        rgmd.lane                      = rg.lane AND
        (case when rgmd.molecular_indexing_scheme = nvl(rg.molecular_barcode_name,'NULL') OR rgmd.is_greedy = 1 then 1 else 0 end =1) 
    )


    SELECT DISTINCT
        --PK
        sagg_rg.project,
        sagg_rg.SAMPLE,
        sagg_rg.data_type,
        sagg_rg.version,
        --

        sagg_rg.analysis_id,

        max(pls.volume) KEEP (DENSE_RANK LAST ORDER BY nvl(pls.plating_task_completed_on, to_date('1-jan-1900'))) OVER (PARTITION BY sagg_rg.project, sagg_rg.SAMPLE, sagg_rg.daTA_TYPE, sagg_rg.version) Latest_BSP_Volume,
        max(pls.concentration) KEEP (DENSE_RANK LAST ORDER BY nvl(pls.plating_task_completed_on, to_date('1-jan-1900'))) OVER (PARTITION BY sagg_rg.project, sagg_rg.SAMPLE, sagg_rg.daTA_TYPE, sagg_rg.version) Latest_BSP_Concentration,
        max(pls.plating_task_completed_on) KEEP (DENSE_RANK LAST ORDER BY nvl( pls.plating_task_completed_on, to_date('1-jan-1900'))) OVER (PARTITION BY sagg_rg.project, sagg_rg.SAMPLE, sagg_rg.daTA_TYPE, sagg_rg.version) Latest_Plated_Date,

		-- Commented on Jan 14, 2016, we no longer do Sage
--        max(sage_qpcr.concentration) KEEP (DENSE_RANK LAST ORDER BY nvl(sage_qpcr.run_date, to_date('1-jan-1900'))) OVER (PARTITION BY sagg_rg.project, sagg_rg.SAMPLE, sagg_rg.daTA_TYPE, sagg_rg.version) Latest_sage_qpcr,
--        max(sage_qpcr.run_date) KEEP (DENSE_RANK LAST ORDER BY nvl(sage_qpcr.run_date, to_date('1-jan-1900'))) OVER (PARTITION BY sagg_rg.project, sagg_rg.SAMPLE, sagg_rg.daTA_TYPE, sagg_rg.version) Latest_sage_qpcr_date,

        to_number(NULL) Latest_sage_qpcr,
        to_date(NULL) 	Latest_sage_qpcr_date,


        sagg_rg.is_latest

    FROM sagg_rg

    LEFT JOIN COGNOS.bsp_plated_samples pls ON
        pls.plated_sample_id = sagg_rg.plated_sample_id AND
        pls.pdo              = sagg_rg.pdo AND
        pls.pdo_sample_id    = sagg_rg.pdo_sample AND
        pls.plated_sample_id NOT IN ('SM-5A3LU', 'SM-5A3LX')  --RPT-2146 - duplicate in BSP_Plated_Samples because daughter and parent are in the PDO

    LEFT JOIN COGNOS.slxre2_rg_ancestry ancestry ON
        ancestry.flowcell_barcode           = sagg_rg.flowcell_barcode AND
        ancestry.lane                       = sagg_rg.lane AND
        ancestry.molecular_indexing_scheme  = sagg_rg.molecular_indexing_scheme  -- ancestry.molecular_indexing_scheme comes from RGMD

    LEFT JOIN COGNOS.slxre_library_qpcr_value sage_qpcr ON sage_qpcr.library_name = ancestry.sage    -- sage_qpcr.library_name is a PK
) DELTA
ON (DELTA.analysis_id = sqc.analysis_id and sqc.source = /*SOURCE*/)

WHEN NOT MATCHED THEN
INSERT VALUES (
    DELTA.project,
    DELTA.SAMPLE,
    DELTA.data_type,
    DELTA.version,
    DELTA.analysis_id,
    DELTA.Latest_BSP_Volume,
    DELTA.Latest_BSP_Concentration,
    DELTA.Latest_Plated_Date,
    DELTA.Latest_sage_qpcr,
    DELTA.Latest_sage_qpcr_date,
	/*SOURCE*/
)
WHERE DELTA.is_latest <> 0  --=1

WHEN MATCHED THEN UPDATE
SET
    sqc.data_type               = decode(DELTA.is_latest, 0, 'DELETE', DELTA.data_type),
    sqc.latest_bsp_volume       = DELTA.Latest_BSP_Volume,
    sqc.Latest_BSP_Concentration= DELTA.Latest_BSP_Concentration,
    sqc.Latest_Plated_Date      = DELTA.Latest_Plated_Date,
    sqc.Latest_sage_qpcr        = DELTA.Latest_sage_qpcr,
    sqc.Latest_sage_qpcr_date   = DELTA.Latest_sage_qpcr_date
DELETE WHERE sqc.data_type = 'DELETE'
;
