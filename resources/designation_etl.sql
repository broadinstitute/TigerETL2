--STATEMENT
MERGE INTO  slxre2_designation d 
USING (
SELECT * from(
	SELECT /*+ USE_NL(a rg fdg fld r rwg rga) */
        --PK
        a.group_memb_id designation_id,
        'Squid' Source,
        --
        lib.NAME designated_library ,
        r.barcode flowcell_barcode ,
        to_number(fcld.lane_name) lane ,
        rg.creation_date designation_time,
        
        CASE WHEN upper(a.comments) LIKE '%POOL%TEST%' THEN 1 ELSE 0 END is_pool_test,
        CASE WHEN rg.creation_date <= trunc(/*DELTA_END*/)-7 AND r.barcode IS NULL THEN 1 ELSE 0 END is_lost,
        'NA' fct_status

    FROM seq20.receptacle_group_memb a
    JOIN seq20.receptacle_GROUP rg           ON        rg.receptacle_group_id = a.receptacle_group_id
    JOIN seq20.fcell_designation_group fdg   ON       fdg.receptacle_group_id = a.receptacle_group_id
    JOIN seq20.fcell_lane_designation fld    ON             fld.group_memb_id = a.group_memb_id
    JOIN seq20.flowcell_lane_descr fcld      ON   fcld.flowcell_lane_descr_id = fld.flowcell_lane_descr_id
    LEFT JOIN seq20.receptacle r             ON               r.receptacle_id = fdg.flowcell_id
    JOIN seq20.receptacle_work_group rwg     ON       rwg.receptacle_group_id = a.receptacle_group_id
    JOIN seq20.receptacle_group_activity rga ON               rga.activity_id = rwg.activity_id AND rga.activity_name = 'Flowcell Registration'
    JOIN seq20.seq_content lib               ON             lib.receptacle_id = a.receptacle_id
    WHERE
        rg.creation_date >= /*DELTA_START*/
		and rg.creation_date < /*DELTA_END*/


    UNION ALL
    -- Mercury DWH
    SELECT
        --PK
        d.designation_id,
        'Mercury' Source,
        --
        d.designation_library designated_library ,
        d.flowcell_barcode ,
        to_number(d.lane) lane ,
        d.creation_date designation_time,
        decode(d.is_pool_test, 'Y', 1, 'N', 0) is_pool_test,
        CASE WHEN d.creation_date <= trunc(/*DELTA_END*/)-7 AND d.flowcell_barcode IS NULL THEN 1 ELSE 0 END is_lost,
        fct.status fct_status
    FROM 
        mercurydw.flowcell_designation d,
        reporting.loj_issue_fct@loj_link.seqbldr fct 
    WHERE
        d.creation_date >= /*DELTA_START*/
		and d.creation_date < /*DELTA_END*/
        AND d.fct_name = fct.KEY 
) 
) fcld 
ON (fcld.designation_id = d.designation_id AND 
    fcld.source = d.source)
WHEN NOT MATCHED THEN INSERT VALUES (
    fcld.designation_id,
    fcld.Source,
    fcld.designated_library ,
    fcld.flowcell_barcode ,
    fcld.lane ,
    fcld.designation_time,
    fcld.is_pool_test,
    fcld.is_lost,
	sysdate ,
	fcld.fct_status
)    

WHEN MATCHED THEN UPDATE
SET 
    d.flowcell_barcode  = fcld.flowcell_barcode ,
    d.is_pool_test      = fcld.is_pool_test,
    d.is_lost           = fcld.is_lost,
	d.fct_status		= fcld.fct_status,
	d.timestamp 		= sysdate 
;

--STATEMENT
MERGE INTO  slxre2_designation_metadata dm 
USING (
SELECT
  DISTINCT -- to eliminate sample in a single library with multiple ngld.molecular_indexing_scheme_id
    --PK
    d.designation_id,
    d.source,
	lls.lsid,
    --
    lls.plated_sample_id, 
    d.designated_library,
    d.flowcell_barcode,
    d.lane,
    d.designation_time,
    d.is_pool_test, 
    d.is_lost,    
    
    lls.pdo,
    lls.pdo_samples pdo_sample_id,  
    lls.collaborator_sample_id external_id,

    r.run_name,
    r.run_date run_start_date,
    r.run_end_date,
    r.instrument_model,
    r.last_cycle_copied,
    r.is_cancelled,
	d.fct_status

FROM slxre2_designation d
JOIN        slxre_library_lcset lls ON   lls.library_name = d.designated_library
LEFT JOIN   slxre2_organic_run r    ON r.flowcell_barcode = d.flowcell_barcode AND r.is_cancelled = 0 AND r.run_type <>  'UNDECIDED'
WHERE d.designation_time >= /*DELTA_START*/
	and d.designation_time < /*DELTA_END*/
) d 
ON (d.designation_id = dm.designation_id AND
    d.source = dm.source AND
    d.lsid = dm.lsid )
WHEN NOT MATCHED THEN INSERT VALUES (
    d.designation_id,
    d.source,
	d.lsid,
    d.plated_sample_id,

    d.designated_library,
    d.flowcell_barcode,
    d.lane,
    d.designation_time,
    d.is_pool_test, 
    d.is_lost,    
    
    d.pdo,
    d.pdo_sample_id,  
    d.external_id,

    d.run_name,
    d.run_start_date,
    d.run_end_date,
    d.instrument_model,
    d.last_cycle_copied,
    d.is_cancelled,
    SYSDATE ,
	d.fct_status

)

WHEN MATCHED THEN UPDATE 
SET
    dm.flowcell_barcode     = d.flowcell_barcode,
    dm.is_pool_test         = d.is_pool_test, 
    dm.is_lost              = d.is_lost,    
    dm.pdo                  = d.pdo,
    dm.pdo_sample_id        = d.pdo_sample_id,  
    dm.external_id          = d.external_id,
    dm.run_name             = d.run_name,
    dm.run_start_date       = d.run_start_date,
    dm.run_end_date         = d.run_end_date,
    dm.instrument_model     = d.instrument_model,
    dm.last_cycle_copied    = d.last_cycle_copied,
    dm.is_cancelled         = d.is_cancelled,
	dm.fct_status			= d.fct_status,
    dm.TIMESTAMP            = SYSDATE 
;

