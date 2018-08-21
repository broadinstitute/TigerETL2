--STATEMENT
MERGE INTO sample_qc_metrics qc
USING ( -- SQUID source
WITH
qc AS (
    SELECT  DISTINCT  -- for 1 sample with multiple barcodes in NGLD
        --PK
        sc.NAME content_name,
        'LCSET-'||nvl(regexp_substr (a.run_name, '[0|1|2|3|4|5|6|7|8|9]+'), a.run_name) lcset,
        lqt.quant_type_name quant_type,
        --

        lq.quant_value,
        a.run_date,
        sc.receptacle_barcode,

        wd.NAME well,
        wd.row_name well_row,
        wd.column_number well_column

    FROM seq20.library_quant_run a
    JOIN seq20.library_quant lq                 ON lq.quant_run_id = a.run_id AND  lq.is_archived = 'N'
    JOIN seq20.library_quant_type lqt           ON lqt.quant_type_id = a.quant_type_id
    JOIN seq20.seq_content sc                   ON sc.receptacle_id = lq.receptacle_id
    JOIN seq20.well_description wd              ON wd.well_description_id = lq.well_description_id
    WHERE
        a.run_date >= /*DELTA_START*/   --'1-jan-2012'
        AND a.run_date < /*DELTA_END*/
 ) /*,
lib_sample AS
(
    SELECT DISTINCT -- in case 1 sample has multiple molecular barcodes
        --PK
        l.NAME content_name,
        ngld_s.lsid,
        --
        s.sample_barcode plated_sample_id
    FROM slxre_library l
    JOIN seq20.seq_content_descr_set scds  ON scds.seq_content_id = l.library_id
    JOIN seq20.next_generation_library_descr ngld ON
        ngld.library_descr_id = scds.seq_content_descr_id
        AND (ngld.library_descr_id IS NULL OR (ngld.library_descr_id IS NOT NULL AND   ngld.sample_id IS NOT NULL ))
    JOIN seq20.lc_sample ngld_s            ON ngld_s.lc_sample_id = ngld.sample_id AND ngld_s.type_id <>14
    LEFT JOIN analytics.bsp_sample s ON s.sample_lsid = ngld_s.lsid
    WHERE l.creation_time >= /*DELTA_START* /-365   --'1-jan-2012'
      AND l.creation_time < / *DELTA_END* /
)
*/
SELECT --PK: content_name, lsid, quant_type
    content_name,
    lsid,
    lcset,
    quant_type,
    --

    quant_value,
    run_date,
    receptacle_barcode,

    well,
    well_row,
    well_column,
    plated_sample_id
FROM (
    SELECT
        qc.content_name,
        lib_sample.lsid lsid,
        qc.lcset,
        qc.quant_type,
        --
        qc.quant_value,
        qc.run_date,
        qc.receptacle_barcode,
        qc.well,
        qc.well_row,
        qc.well_column,
        lib_sample.plated_sample_id,
        row_number() over (PARTITION BY qc.quant_type , qc.content_name, lib_sample.lsid ORDER BY run_date desc) myrank

    FROM qc
    JOIN slxre_library_lcset lib_sample ON lib_sample.library_name = qc.content_name
--    JOIN lib_sample ON lib_sample.content_name = qc.content_name

)
WHERE myrank =1
) DELTA

ON (DELTA.content_name = qc.content_name AND
    DELTA.lsid = qc.lsid                 AND
    DELTA.quant_type = qc.quant_type)

WHEN NOT MATCHED THEN
INSERT  VALUES (
        DELTA.content_name,
        DELTA.lsid,
        DELTA.lcset,
        DELTA.quant_type,
        DELTA.quant_value,
        DELTA.run_date,
        DELTA.receptacle_barcode,
        DELTA.well,
        DELTA.well_row,
        DELTA.well_column,
        DELTA.plated_sample_id
)
WHEN MATCHED THEN UPDATE
SET
        qc.quant_value        = DELTA.quant_value,
        qc.run_date           = DELTA.run_date,
        qc.receptacle_barcode = DELTA.receptacle_barcode,
        qc.well               = DELTA.well,
        qc.well_row           = DELTA.well_row,
        qc.well_column        = DELTA.well_column
;

--STATEMENT
MERGE INTO sample_qc_metrics qc
USING ( --MERCURY source
    SELECT DISTINCT
        --PK
        a.vessel_barcode lab_vessel_name,
        s.sample_lsid lsid,
        lls.batch_name lcset,  -- not in PK
        decode(a.quant_type, 'POND_PICO', 'Pond Pico', 'CATCH_PICO', 'Catch Pico', 'ECO_QPCR', 'Eco qPCR') quant_type,
        --
        a.quant_value,
        a.run_date,
        a.vessel_barcode  receptacle_barcode,

        nvl(a.rack_position, 'NA') well,
        nvl(substr(a.rack_position, 1,1), '0') well_row,
        nvl(to_number(substr(a.rack_position, 2,1)), 0) well_column,
        lls.lcset_sample plated_sample_id

    FROM mercurydw.lab_metric a
    JOIN mercurydw.library_lcset_sample lls ON lls.library_label = a.vessel_barcode
    JOIN analytics.bsp_sample s ON s.sample_barcode = lls.lcset_sample   --s.sample_id = substr(lls.lcset_sample, 4)
    WHERE a.run_date >= /*DELTA_START*/   --sysdate-180
        AND a.run_date < /*DELTA_END*/
        AND a.quant_type IN ('POND_PICO',  'CATCH_PICO',  'ECO_QPCR')

) DELTA
ON (DELTA.lab_vessel_name = qc.content_name AND
    DELTA.lsid = qc.lsid                 AND
    DELTA.quant_type = qc.quant_type)

WHEN NOT MATCHED THEN
INSERT  VALUES (
        DELTA.lab_vessel_name,
        DELTA.lsid,
        DELTA.lcset,
        DELTA.quant_type,
        DELTA.quant_value,
        DELTA.run_date,
        DELTA.receptacle_barcode,
        DELTA.well,
        DELTA.well_row,
        DELTA.well_column,
        DELTA.plated_sample_id
)
WHEN MATCHED THEN UPDATE
SET
        qc.quant_value        = DELTA.quant_value,
        qc.run_date           = DELTA.run_date,
        qc.receptacle_barcode = DELTA.receptacle_barcode,
        qc.well               = DELTA.well,
        qc.well_row           = DELTA.well_row,
        qc.well_column        = DELTA.well_column

;
