--STATEMENT
MERGE INTO slxre2_library_qpcr lqpcr
USING (
SELECT
    --PK
    library_name,
    --
    run_name,
    library_type,
    library_creation_date,
    run_date,
    status          pass_fail,
    concentration   quant_value,
    well_location


FROM (
SELECT
    --PK - should include run_name as well until MArion fixes data so that only 1 run per library will be in_archive='N'
    l.NAME library_name,
    qpcr_r.run_name,
    --
    l.TYPE library_type,
    l.creation_time library_creation_date,
    qpcr_r.run_date,
    qpcr_rl.status,  -- Pass/Fail
    qpcr_rl.concentration ,
    NULL  well_location
    , row_number() over (partition BY l.NAME ORDER BY qpcr_r.run_date DESC ) myrank
FROM seq20.qpcr_run_library qpcr_rl,
    seq20.qpcr_run qpcr_r,
    slxre_library l
WHERE qpcr_rl.qpcr_run_id = qpcr_r.qpcr_run_id
    AND qpcr_rl.seq_content_id = l.library_id
    AND qpcr_rl.is_archived = 'N'
AND qpcr_r.run_date >= /*DELTA_START*/
AND qpcr_r.run_date < /*DELTA_END*/
)
WHERE myrank =1

UNION ALL 

SELECT 
    library_name,
    run_name, 
    library_type,
    library_creation_date ,
    run_date,
    pass_fail,
    quant_value,
    well_location
FROM (
    SELECT DISTINCT  
    lm.vessel_barcode library_name,
    lm.run_name, 
    lls.library_type,
    lls.library_creation_date ,
    lm.run_date,
    lm.decision pass_fail,
    lm.quant_value,
    lm.rack_position well_location,
    dense_rank() over (PARTITION BY lm.vessel_barcode ORDER BY lm.run_date DESC ) myrank
    FROM mercurydw.lab_metric lm ,
    mercurydw.library_lcset_sample lls 
    WHERE lm.quant_type IN ('VIIA_QPCR', 'ECO_QPCR')
    AND lm.vessel_barcode = lls.library_label
    AND lm.run_date >= /*DELTA_START*/
    AND lm.run_date < /*DELTA_END*/
)WHERE myrank =1
) DELTA 
ON (DELTA.library_name = lqpcr.library_name )

WHEN NOT MATCHED THEN 
INSERT VALUES (
DELTA.library_name,
DELTA.run_name, 
DELTA.library_type,
DELTA.library_creation_date ,
DELTA.run_date,
DELTA.pass_fail,
DELTA.quant_value,
DELTA.well_location,
SYSDATE 
)

WHEN MATCHED THEN 
UPDATE SET 
lqpcr.run_name      = DELTA.run_name, 
lqpcr.run_date      = DELTA.run_date,
lqpcr.pass_fail     = DELTA.pass_fail,
lqpcr.quant_value   = DELTA.quant_value,
lqpcr.well_location = DELTA.well_location,
lqpcr.TIMESTAMP     = SYSDATE 
;