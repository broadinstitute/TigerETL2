--STATEMENT
DELETE FROM target_mm 
WHERE sessionid=userenv('SESSIONID')
;

--STATEMENT
INSERT INTO target_mm(SESSIONID, STRING_FIELD1, STRING_FIELD2)
-- samples presented in pdo_star in non-terminal status
  SELECT userenv('SESSIONID'), a.pdo_name,a.pdo_sample_id
  FROM pdo_star5 a
  WHERE
    a.sample_is_billed='F'
    AND a.sample_delivery_status<>'ABANDONED'
  UNION  
  -- samples not presented yet in pdo_star
  SELECT DISTINCT userenv('SESSIONID'), pdos.pdo_name, pdos.pdo_sample_id
  FROM mercurydw_pdo_samples pdos
    JOIN pdo_star_interesting_products sip ON sip.product_part_number=pdos.product_part_number
    LEFT JOIN pdo_star5 ps ON ps.pdo_name=pdos.pdo_name AND ps.pdo_sample_id=pdos.pdo_sample_id
  WHERE
    pdos.pdo_name IS NOT NULL
    AND (ps.pdo_name IS NULL
         OR pdos.pdo_created_date>= /*DELTA_START*/ -- Limited full refresh
    )
/*  Special_refresh table is being cleaned up after moving the rows to PDO_STAR5  target
    UNION
  SELECT userenv('SESSIONID'), pdo_name, pdo_sample_id FROM pdo_star_special_refreshes
*/  
;

/*--STATEMENT
DELETE FROM pdo_star_special_refreshes 
WHERE (pdo_name, pdo_sample_id) IN (SELECT STRING_FIELD1, STRING_FIELD2 FROM target WHERE sessionid=userenv('SESSIONID'))
;
*/
--STATEMENT
DELETE FROM pdo_star2_mim 
WHERE (pdo_name,pdo_sample) IN (SELECT STRING_FIELD1, STRING_FIELD2 FROM target_mm WHERE sessionid=userenv('SESSIONID'))
;

--STATEMENT
INSERT INTO pdo_star2_mim 
WITH 
target AS (
SELECT /*+ MATERIALIZE */
    a.pdo_name, a.pdo_sample, a.sample_position, 
    a.research_project, a.product_name, a.data_type,
    a.product_part_number, a.research_project_key,
    a.pdo_created_date, a.pdo_title, a.pdo_owner, a.pdo_quote_id,
    a.organism_name, a.sample_delivery_status, a.sample_is_on_risk,
    a.sample_is_billed, a.sample_quote_id, a.squid_project_name,
    a.dcfm_proj, a.project_name, a.aggregation_project,
    a.external_id,a.translated_external_id,
    a.bsp_collaborator_sample_id, 
    --a.sample_barcode,
    a.pdo_sample_root, 
    --a.swr_sample_root,
    a.collaborator_participant_id, a.collection, a.uuid,
    a.work_request_id,
--     a.bsp_plating_request_receipt,
    a.swr_sample_id, 
--LSID to be added to PDO_STAR5    
--a.sample_lsid, 
--'from STAR5'     bsp_sample_lsid  ,   --a.bsp_sample_lsid,
--    a.stock_sample, 
    a.bsp_export_date, a.bsp_export_date_csv,

    a.sample_import_date, a.first_run_barcode, a.runs_start,
    a.runs_end, a.fpv2_lod_min, a.fpv2_lod_max,
    a.last_aggregation_date, a.bc_pf_reads, a.bc_total_reads,
    a.bc_total_pf_bases, a.run_fraction_sum, a.use_raw,
a.bsp_original_material_type, 
    a.bsp_root_material_type bsp_material_type,
    a.sample_type, a.pdo_status
    
, a.lane_fraction 
, a.n_seq_rg, a.n_blacklisted_rg
FROM star_face_mm a --pdo_star5 a
, target_mm 
WHERE target_mm.sessionid = userenv('SESSIONID')
AND target_mm.string_field1 = a.pdo_name
AND target_mm.string_field2 = a.pdo_sample 
AND a.pdo_name <> 'PDO-7427'  -- IgFit Validation PDO, initially aggregated in CRSP and then in research, First Contam i First Agg Date break the UK

),

 dcfm AS (
SELECT target.pdo_name, target.pdo_sample, target.sample_position,
    cfm.dcfm,
    cfm.rule_name
FROM target 
JOIN slxre_sample_cvrg_first_met cfm ON 
    cfm.product_part_number = target.product_part_number AND 
    cfm.pdo_name = target.pdo_name                       AND
    cfm.aggregation_project = target.project_name        AND
    cfm.external_sample_id = target.external_id 
),
desig AS (
SELECT 
    target.pdo_name, target.pdo_sample, target.sample_position,
    min(d.flowcell_designation_time) flowcell_designation_time
FROM target 
JOIN slxre2_designation_to_run_end d ON 
    d.pdo = target.pdo_name AND 
    d.pdo_sample_id = target.pdo_sample
WHERE 	
	NVL(d.instrument_model, 'Null')<>'MISEQ'
	AND NVL(d.is_cancelled, 0)<>1
	AND ( d.run_start_date IS NOT NULL   OR   d.flowcell_designation_time>(TRUNC(to_date(sysdate)))-5 )

GROUP BY     target.pdo_name, target.pdo_sample, target.sample_position
),

first_contam AS (
SELECT pdo_name, pdo_sample, sample_position,
    workflow_end_date, pct_contamination
FROM (
SELECT
    target.pdo_name, target.pdo_sample, target.sample_position,
    a.project, a.SAMPLE , 
    a.workflow_end_date,
    c.pct_contamination*100 pct_contamination,
    DENSE_RANK () OVER ( PARTITION BY a.project, a.sample  ORDER BY a.workflow_end_date ASC ) drank

FROM target 
, metrics.aggregation a 
, metrics.aggregation_contam c 
WHERE target.project_name = a.project
AND target.external_id = a.SAMPLE 
AND a.id= c.aggregation_id
AND target.pdo_created_date < a.workflow_start_date
AND target.runs_start < a.workflow_start_date
AND a.LIBRARY IS NULL 

UNION 

SELECT
    target.pdo_name, target.pdo_sample, target.sample_position,
    a.project, a.SAMPLE , 
    a.workflow_end_date,
    c.pct_contamination*100 pct_contamination,
    DENSE_RANK () OVER ( PARTITION BY a.project, a.sample  ORDER BY a.workflow_end_date ASC ) drank

FROM target 
, metrics.aggregation@crspreporting.crspprod a 
, metrics.aggregation_contam@crspreporting.crspprod c 
WHERE target.project_name = a.project
AND target.external_id = a.SAMPLE 
AND a.id= c.aggregation_id
AND target.pdo_created_date < a.workflow_start_date
AND target.runs_start < a.workflow_start_date
AND a.LIBRARY IS NULL 
)
WHERE drank=1
),

sample_agg AS (
SELECT 
    --PK
    target.pdo_name, target.pdo_sample, target.sample_position,
    sa.project, sa.SAMPLE,sa.data_type,
    --
    sa.analysis_id,
    sa.analysis_type, 
    sa.analysis_start, sa.analysis_end, 
    sa.version, 


    sa.gssr_id, 
    sa.bsp_original_material_type, sa.bsp_root_material_type,
    sa.lcset_type,sa.lcset_protocol, sa.lcset_seq_technology, sa.lcset_topoff,
    sa.pdo_title, 
    sa.last_run_end_date,
    sa.n_lanes,
    sa.n_aggregated_rg,
    sa.source, 
           
    sa.ap_mean_read_length,  sa.ap_pct_pf_reads,sa.ap_pct_pf_reads_aligned,sa.ap_pct_reads_aligned_in_pairs,
    sa.ap_pct_strand_balance, sa.ap_pf_aligned_bases, sa.ap_pf_hq_error_rate, sa.ap_pf_indel_rate, 
    sa.ap_pf_reads_aligned,sa.ap_reads_aligned_in_pairs,sa.ap_total_reads,
    sa.anp_pf_aligned_bases,  sa.anp_pf_hq_error_rate,sa.anp_pf_indel_rate,sa.anp_total_reads,
            
    sa.hs_bait_design_efficiency,
    sa.hs_bait_set, sa.hs_bait_territory, sa.hs_fold_80_base_penalty,
    sa.hs_fold_enrichment, sa.hs_genome_size, sa.hs_library_size,
    sa.hs_mean_bait_coverage, sa.hs_mean_target_coverage,
    sa.hs_near_bait_bases, sa.hs_off_bait_bases, sa.hs_on_bait_bases,
    sa.hs_on_bait_vs_selected, sa.hs_on_target_bases,
    sa.hs_pct_off_bait, sa.hs_pct_pf_reads, sa.hs_pct_pf_uq_reads,
    sa.hs_pct_pf_uq_reads_aligned, sa.hs_pct_selected_bases,
    sa.hs_pct_target_bases_10x, sa.hs_pct_target_bases_20x,
    sa.hs_pct_target_bases_2x, sa.hs_pct_target_bases_30x,
    sa.hs_pct_target_bases_40x, sa.hs_pct_target_bases_50x,
    sa.hs_pct_target_bases_100x, sa.hs_pct_usable_bases_on_bait,
    sa.hs_pct_usable_bases_on_target, sa.hs_pf_unique_reads,
    sa.hs_pf_uq_bases_aligned, sa.hs_pf_uq_reads_aligned,
    sa.hs_target_territory, sa.hs_total_reads,
    sa.hs_zero_cvg_targets_pct, 


    sa.snp_num_in_dbsnp, sa.snp_pct_dbsnp, sa.snp_total_snps,
    sa.hs_penalty_10x, sa.hs_penalty_20x,
    sa.hs_penalty_30x, sa.hs_penalty_40x, sa.hs_penalty_50x,
    sa.hs_penalty_100x, 
           
    sa.rna_pf_aligned_bases, sa.rna_ribosomal_bases,
    sa.rna_coding_bases, sa.rna_utr_bases, sa.rna_intronic_bases,
    sa.rna_intergenic_bases, sa.rna_correct_strand_reads,
    sa.rna_incorrect_strand_reads, sa.rna_pct_ribosomal_bases,
    sa.rna_pct_coding_bases, sa.rna_pct_utr_bases,
    sa.rna_pct_intronic_bases, sa.rna_pct_intergenic_bases,
    sa.rna_pct_mrna_bases, sa.rna_pct_correct_strand_reads,
    sa.rna_pf_bases, sa.rna_pct_usable_bases, sa.rna_median_cv_coverage,
    sa.rna_median_5prime_bias, sa.rna_median_3prime_bias,
    sa.rna_median_5prim_to_3prim_bias, 
           
    sa.ins_median_insert_size,
    sa.ins_min_insert_size, sa.ins_max_insert_size,
    sa.ins_mean_insert_size, sa.ins_standard_deviation,
    sa.ins_read_pairs, sa.ins_width_of_10_percent,
    sa.ins_width_of_20_percent, sa.ins_width_of_30_percent,
    sa.ins_width_of_40_percent, sa.ins_width_of_50_percent,
    sa.ins_width_of_60_percent, sa.ins_width_of_70_percent,
    sa.ins_width_of_80_percent, sa.ins_width_of_90_percent,
    sa.ins_width_of_99_percent, 
           
    sa.pcr_custom_amplicon_set, sa.pcr_genome_size,
    sa.pcr_amplicon_territory, sa.pcr_target_territory,
    sa.pcr_total_reads, sa.pcr_pf_reads, sa.pcr_pf_unique_reads,
    sa.pcr_pct_pf_reads, sa.pcr_pct_pf_uq_reads,
    sa.pcr_pf_uq_reads_aligned, sa.pcr_pct_pf_uq_reads_aligned,
    sa.pcr_pf_uq_bases_aligned, sa.pcr_on_amplicon_bases,
    sa.pcr_near_amplicon_bases, sa.pcr_off_amplicon_bases,
    sa.pcr_on_target_bases, sa.pcr_pct_amplified_bases,
    sa.pcr_pct_off_amplicon, sa.pcr_on_amplicon_vs_selected,
    sa.pcr_mean_amplicon_coverage, sa.pcr_mean_target_coverage,
    sa.pcr_fold_enrichment, sa.pcr_zero_cvg_targets_pct,
    sa.pcr_fold_80_base_penalty, sa.pcr_pct_target_bases_2x,
    sa.pcr_pct_target_bases_10x, sa.pcr_pct_target_bases_20x,
    sa.pcr_pct_target_bases_30x, sa.pcr_at_dropout, sa.pcr_gc_dropout,

    sa.pct_contamination, 
    sa.rrbs_reads_aligned, sa.rrbs_non_cpg_bases,
    sa.rrbs_non_cpg_converted_bases, sa.rrbs_pct_noncpg_bases_convertd,
    sa.rrbs_cpg_bases_seen, sa.rrbs_cpg_bases_converted,
    sa.rrbs_pct_cpg_bases_converted, sa.rrbs_mean_cpg_coverage,
    sa.rrbs_median_cpg_coverage, sa.rrbs_reads_with_no_cpg,
    sa.rrbs_reads_ignored_short, sa.rrbs_reads_ignored_mismatches,
           
    sa.bc_pf_reads, sa.bc_total_reads, sa.bc_pf_bases,
    sa.wgs_genome_territory, sa.wgs_mean_coverage, sa.wgs_sd_coverage,
    sa.wgs_median_coverage, sa.wgs_mad_coverage, sa.wgs_pct_exc_mapq,
    sa.wgs_pct_exc_dupe, sa.wgs_pct_exc_unpaired, sa.wgs_pct_exc_baseq,
    sa.wgs_pct_exc_overlap, sa.wgs_pct_exc_capped, sa.wgs_pct_exc_total,
    sa.wgs_pct_5x, sa.wgs_pct_10x, sa.wgs_pct_20x, sa.wgs_pct_30x,
    sa.wgs_pct_40x, sa.wgs_pct_50x, sa.wgs_pct_60x, sa.wgs_pct_70x,
    sa.wgs_pct_80x, sa.wgs_pct_90x, sa.wgs_pct_100x,   sa.wgs_pct_15x, sa.wgs_pct_25x,
           
    sa.rwgs_genome_territory, sa.rwgs_mean_coverage,
    sa.rwgs_sd_coverage, sa.rwgs_median_coverage, sa.rwgs_mad_coverage,
    sa.rwgs_pct_exc_mapq, sa.rwgs_pct_exc_dupe,
    sa.rwgs_pct_exc_unpaired, sa.rwgs_pct_exc_baseq,
    sa.rwgs_pct_exc_overlap, sa.rwgs_pct_exc_capped,
    sa.rwgs_pct_exc_total, sa.rwgs_pct_5x, sa.rwgs_pct_10x,
    sa.rwgs_pct_20x, sa.rwgs_pct_30x, sa.rwgs_pct_40x, sa.rwgs_pct_50x,
    sa.rwgs_pct_60x, sa.rwgs_pct_70x, sa.rwgs_pct_80x, sa.rwgs_pct_90x,
    sa.rwgs_pct_100x, sa.rwgs_pct_15x, sa.rwgs_pct_25x,

    qc.latest_plated_date, qc.latest_sage_qpcr_date, --qc.version sa_version,  -- named as "QC Version (QC)" but ref is replaced with "Picard Version"
    qc.latest_bsp_concentration, qc.latest_bsp_volume, qc.latest_sage_qpcr
FROM target 
JOIN slxre2_pagg_sample sa  ON sa.project = target.project_name -- this is the agg project from S5
    AND sa.SAMPLE = target.external_id 
LEFT JOIN slxre2_sample_agg_qc qc ON qc.project = sa.project AND
    qc.SAMPLE = sa.SAMPLE AND 
    qc.data_type = sa.data_type 
),

billing  AS (  -- there are no sum(illing_amount)>0 since jan-2015, even before then
SELECT
-- PK
target.pdo_name, target.pdo_sample, target.sample_position
--    b.pdo_number pdo, b.sample_name pdo_sample, b.sample_position
--
, MAX(b.billed_date) max_billed_date, MAX(b.work_complete_date) max_work_complete_date

FROM target 
JOIN billing_ledger b ON 
    b.pdo_number = target.pdo_name AND 
    b.sample_name = target.pdo_sample AND 
    b.sample_position = target.sample_position AND 
    b.price_item_type <>'ADD_ON_PRICE_ITEM'
GROUP BY --b.pdo_number, b.sample_name, b.sample_position
target.pdo_name, target.pdo_sample, target.sample_position
HAVING SUM(b.billed_amount)>0
),
min_agg_by_dcfm AS (
SELECT DISTINCT 
--PK
target.pdo_name, target.pdo_sample, target.sample_position,
min(ag.workflow_end_date) KEEP (DENSE_RANK FIRST ORDER BY ag.workflow_end_date) OVER (PARTITION BY target.pdo_name, target.dcfm_proj,target.external_id )  first_agg_end

FROM
target
JOIN metrics.aggregation ag ON 
    ag.project = target.dcfm_proj AND
    ag.SAMPLE = target.external_id AND
    ag.LIBRARY IS NULL 

WHERE
target.runs_end < ag.workflow_start_date
-- why filter on part number ?
AND target.product_part_number IN (
'P-EX-0001',
'P-EX-0002',
'P-EX-0003',
'P-EX-0005',
'P-EX-0006',
'P-EX-0007',
'P-EX-0008',
'P-EX-0009',
'P-EX-0010',
'P-EX-0012',
'P-EX-0013',
'P-EX-0014',
'P-EX-0015',
'P-EX-0016',
'P-VAL-0003')
UNION 

SELECT DISTINCT 
--PK
target.pdo_name, target.pdo_sample, target.sample_position,
min(ag.workflow_end_date) KEEP (DENSE_RANK FIRST ORDER BY ag.workflow_end_date) OVER (PARTITION BY target.pdo_name, target.dcfm_proj,target.external_id )  first_agg_end

FROM
target
JOIN metrics.aggregation@crspreporting.crspprod ag ON 
    ag.project = target.dcfm_proj AND
    ag.SAMPLE = target.external_id AND
    ag.LIBRARY IS NULL 

WHERE
target.runs_end < ag.workflow_start_date

),
min_agg_by_aggproj AS (
SELECT distinct 
--PK
target.pdo_name, target.pdo_sample, target.sample_position,
min(ag.workflow_end_date) KEEP (DENSE_RANK FIRST ORDER BY ag.workflow_end_date) OVER (PARTITION BY target.pdo_name, target.project_name,target.external_id )  first_agg_end

FROM
target
JOIN metrics.aggregation ag ON 
    ag.project = target.project_name AND
    ag.SAMPLE = target.external_id AND
    ag.LIBRARY IS NULL 

WHERE
target.runs_end < ag.workflow_start_date
UNION 

SELECT distinct 
--PK
target.pdo_name, target.pdo_sample, target.sample_position,
min(ag.workflow_end_date) KEEP (DENSE_RANK FIRST ORDER BY ag.workflow_end_date) OVER (PARTITION BY target.pdo_name, target.project_name,target.external_id )  first_agg_end

FROM
target
JOIN metrics.aggregation@crspreporting.crspprod ag ON 
    ag.project = target.project_name AND
    ag.SAMPLE = target.external_id AND
    ag.LIBRARY IS NULL 

WHERE
target.runs_end < ag.workflow_start_date
),

 lib_agg as (
SELECT
    --PK
    a.aggregation_project,
    a.external_id,
    a.pdo_name,
a.pdo_sample, a.sample_position,   
    a.data_type,
    --
    AVG(l.dup_percent_duplication) lib_avg_dup_pct,
    MAX(l.dup_percent_duplication) lib_max_dup_pct,
    MIN(l.dup_percent_duplication) lib_min_dup_pct,

    AVG(l.dup_estimated_library_size) lib_avg_est_lib_sz,
    MAX(l.dup_estimated_library_size) lib_max_est_lib_sz,
    MIN(l.dup_estimated_library_size) lib_min_est_lib_sz,

    AVG(l.ins_mean_insert_size) lib_avg_mean_ins_sz,
    MAX(l.ins_mean_insert_size) lib_max_mean_ins_sz,
    MIN(l.ins_mean_insert_size) lib_min_mean_ins_sz


FROM
    target  a,
    slxre2_pagg_library l
WHERE
    a.aggregation_project = l.project
    AND a.external_id = l.sample

GROUP BY
    a.aggregation_project,
    a.external_id,
    a.pdo_name,
a.pdo_sample, a.sample_position,
    a.data_type
),

lcsets AS (  -- and orhpans
SELECT 
    --PK
    pdo_name, 
    pdo_sample,
    --
    concat_string_csv(DISTINCT lcset)  lcset_csv,
    min(lcset)                         lcset_min,
    max(lcset)                         lcset_max,
    min(orphan_rate)                   relative_orphan_rate_min,
    max(orphan_rate)                   relative_orphan_rate_max,
    avg(orphan_rate)                   relative_orphan_rate_avg
FROM (    
    SELECT  
        --PK
        rgmd.flowcell_barcode, rgmd.lane, rgmd.molecular_indexing_scheme,
        --
        target.pdo_name, 
        target.pdo_sample,
        --
        rgmd.lcset,
        CASE 
        WHEN (lhqs.pf_bases_indexed<>lhqs.pf_bases) AND (NVL(lhqs.actual_index_length, 0) > 0 AND lhqs.pf_bases >0)
            THEN (1-lhqs.pf_bases_indexed/(lhqs.pf_bases- NVL(lhqs.ic_bases, 0)))
        ELSE 0.0
        END   orphan_rate     
    FROM target ,
        slxre_readgroup_metadata rgmd ,
        slxre2_lane_hqs lhqs 
    WHERE target.pdo_name = rgmd.product_order_key    
        AND target.pdo_sample = rgmd.product_order_sample
        AND rgmd.flowcell_barcode = lhqs.flowcell_barcode
        AND rgmd.lane = lhqs.lane 
        AND rgmd.setup_read_structure NOT IN ('8B', '8B8B')
)    
GROUP BY pdo_name, pdo_sample    
)

SELECT 
    --PK check if it is indeed the PK
    target.pdo_name, target.pdo_sample, target.sample_position, 
    --
    target.research_project, target.product_name, target.data_type,
    target.product_part_number, target.research_project_key,
    target.pdo_created_date, target.pdo_title, target.pdo_owner, target.pdo_quote_id,
    target.organism_name, target.sample_delivery_status, target.sample_is_on_risk,
    target.sample_is_billed, target.sample_quote_id, target.squid_project_name,
    target.dcfm_proj, 
	target.project_name, target.aggregation_project,  -- project_name is the customized aggregated project for some PDOs
    target.external_id,target.translated_external_id,
    target.bsp_collaborator_sample_id, 
--target.sample_barcode,
    target.pdo_sample_root, 
--    target.swr_sample_root,
    target.collaborator_participant_id, target.collection, target.uuid,
    target.work_request_id, 
    --target.bsp_plating_request_receipt,
    target.swr_sample_id, 
--    target.sample_lsid, 
--    target.bsp_sample_lsid,
--    target.stock_sample, 
    target.bsp_export_date, target.bsp_export_date_csv,

    target.sample_import_date, target.first_run_barcode, target.runs_start,
    target.runs_end, target.fpv2_lod_min, target.fpv2_lod_max,
    target.last_aggregation_date, target.bc_pf_reads, target.bc_total_reads,
    target.bc_total_pf_bases, target.run_fraction_sum, target.use_raw,
    target.bsp_original_material_type, target.bsp_material_type,
    target.sample_type, target.pdo_status,
 target.lane_fraction,  
 target.n_seq_rg, 
 target.n_blacklisted_rg , 
 
    s.gssr_id ,
    s.strain, 
--    s.o_material_type_name original_material_type,
    s.rin,
    s.tissue_site, s.tissue_site_detail,
    


    cfm.dcfm,
    cfm.rule_name,
    
    desig.flowcell_designation_time,
    
    first_contam.pct_contamination first_pct_contamination,

    billing.max_work_complete_date,--
    billing.max_billed_date,--
    
    min_agg_by_dcfm.first_agg_end dcfm_first_agg_end,
    min_agg_by_aggproj.first_agg_end ,
    
    bam.ncbi_status ,
    --
    sa.analysis_id,
    sa.analysis_type, 
    sa.analysis_start, sa.analysis_end, 
    sa.version, 


    sa.gssr_id sagg_gssr_ids, 
--    sa.bsp_original_material_type sag_bsp_original_material_type, sa.bsp_root_material_type,
    sa.lcset_type,sa.lcset_protocol, sa.lcset_seq_technology, sa.lcset_topoff,
--    'NULL' sagg_pdo_title, 
    sa.last_run_end_date,
    sa.n_lanes n_aggregated_lanes,
    sa.n_aggregated_rg,
    sa.source, 
    
    lcsets.lcset_csv,
    lcsets.lcset_min,
    lcsets.lcset_max,
    lcsets.relative_orphan_rate_min,
    lcsets.relative_orphan_rate_max,
    lcsets.relative_orphan_rate_avg,
           
    sa.ap_mean_read_length,  sa.ap_pct_pf_reads,sa.ap_pct_pf_reads_aligned,sa.ap_pct_reads_aligned_in_pairs,
    sa.ap_pct_strand_balance, sa.ap_pf_aligned_bases, sa.ap_pf_hq_error_rate, sa.ap_pf_indel_rate, 
    sa.ap_pf_reads_aligned,sa.ap_reads_aligned_in_pairs,sa.ap_total_reads,
    sa.anp_pf_aligned_bases,  sa.anp_pf_hq_error_rate,sa.anp_pf_indel_rate,sa.anp_total_reads,
            
    sa.hs_bait_design_efficiency,
    sa.hs_bait_set, sa.hs_bait_territory, sa.hs_fold_80_base_penalty,
    sa.hs_fold_enrichment, sa.hs_genome_size, sa.hs_library_size,
    sa.hs_mean_bait_coverage, sa.hs_mean_target_coverage,
    sa.hs_near_bait_bases, sa.hs_off_bait_bases, sa.hs_on_bait_bases,
    sa.hs_on_bait_vs_selected, sa.hs_on_target_bases,
    sa.hs_pct_off_bait, sa.hs_pct_pf_reads, sa.hs_pct_pf_uq_reads,
    sa.hs_pct_pf_uq_reads_aligned, sa.hs_pct_selected_bases,
    sa.hs_pct_target_bases_10x, sa.hs_pct_target_bases_20x,
    sa.hs_pct_target_bases_2x, sa.hs_pct_target_bases_30x,
    sa.hs_pct_target_bases_40x, sa.hs_pct_target_bases_50x,
    sa.hs_pct_target_bases_100x, sa.hs_pct_usable_bases_on_bait,
    sa.hs_pct_usable_bases_on_target, sa.hs_pf_unique_reads,
    sa.hs_pf_uq_bases_aligned, sa.hs_pf_uq_reads_aligned,
    sa.hs_target_territory, sa.hs_total_reads,
    sa.hs_zero_cvg_targets_pct, 


    sa.snp_num_in_dbsnp, sa.snp_pct_dbsnp, sa.snp_total_snps,
    sa.hs_penalty_10x, sa.hs_penalty_20x,
    sa.hs_penalty_30x, sa.hs_penalty_40x, sa.hs_penalty_50x,
    sa.hs_penalty_100x, 
           
    sa.rna_pf_aligned_bases, sa.rna_ribosomal_bases,
    sa.rna_coding_bases, sa.rna_utr_bases, sa.rna_intronic_bases,
    sa.rna_intergenic_bases, sa.rna_correct_strand_reads,
    sa.rna_incorrect_strand_reads, sa.rna_pct_ribosomal_bases,
    sa.rna_pct_coding_bases, sa.rna_pct_utr_bases,
    sa.rna_pct_intronic_bases, sa.rna_pct_intergenic_bases,
    sa.rna_pct_mrna_bases, sa.rna_pct_correct_strand_reads,
    sa.rna_pf_bases, sa.rna_pct_usable_bases, sa.rna_median_cv_coverage,
    sa.rna_median_5prime_bias, sa.rna_median_3prime_bias,
    sa.rna_median_5prim_to_3prim_bias, 
           
    sa.ins_median_insert_size,
    sa.ins_min_insert_size, sa.ins_max_insert_size,
    sa.ins_mean_insert_size, sa.ins_standard_deviation,
    sa.ins_read_pairs, sa.ins_width_of_10_percent,
    sa.ins_width_of_20_percent, sa.ins_width_of_30_percent,
    sa.ins_width_of_40_percent, sa.ins_width_of_50_percent,
    sa.ins_width_of_60_percent, sa.ins_width_of_70_percent,
    sa.ins_width_of_80_percent, sa.ins_width_of_90_percent,
    sa.ins_width_of_99_percent, 
           
    sa.pcr_custom_amplicon_set, sa.pcr_genome_size,
    sa.pcr_amplicon_territory, sa.pcr_target_territory,
    sa.pcr_total_reads, sa.pcr_pf_reads, sa.pcr_pf_unique_reads,
    sa.pcr_pct_pf_reads, sa.pcr_pct_pf_uq_reads,
    sa.pcr_pf_uq_reads_aligned, sa.pcr_pct_pf_uq_reads_aligned,
    sa.pcr_pf_uq_bases_aligned, sa.pcr_on_amplicon_bases,
    sa.pcr_near_amplicon_bases, sa.pcr_off_amplicon_bases,
    sa.pcr_on_target_bases, sa.pcr_pct_amplified_bases,
    sa.pcr_pct_off_amplicon, sa.pcr_on_amplicon_vs_selected,
    sa.pcr_mean_amplicon_coverage, sa.pcr_mean_target_coverage,
    sa.pcr_fold_enrichment, sa.pcr_zero_cvg_targets_pct,
    sa.pcr_fold_80_base_penalty, sa.pcr_pct_target_bases_2x,
    sa.pcr_pct_target_bases_10x, sa.pcr_pct_target_bases_20x,
    sa.pcr_pct_target_bases_30x, sa.pcr_at_dropout, sa.pcr_gc_dropout,

    sa.pct_contamination, 
    sa.rrbs_reads_aligned, sa.rrbs_non_cpg_bases,
    sa.rrbs_non_cpg_converted_bases, sa.rrbs_pct_noncpg_bases_convertd,
    sa.rrbs_cpg_bases_seen, sa.rrbs_cpg_bases_converted,
    sa.rrbs_pct_cpg_bases_converted, sa.rrbs_mean_cpg_coverage,
    sa.rrbs_median_cpg_coverage, sa.rrbs_reads_with_no_cpg,
    sa.rrbs_reads_ignored_short, sa.rrbs_reads_ignored_mismatches,
           
    sa.bc_pf_reads sagg_bc_pf_reads, sa.bc_total_reads sagg_bc_total_reads, sa.bc_pf_bases sagg_bc_pf_bases,
    sa.wgs_genome_territory, sa.wgs_mean_coverage, sa.wgs_sd_coverage,
    sa.wgs_median_coverage, sa.wgs_mad_coverage, sa.wgs_pct_exc_mapq,
    sa.wgs_pct_exc_dupe, sa.wgs_pct_exc_unpaired, sa.wgs_pct_exc_baseq,
    sa.wgs_pct_exc_overlap, sa.wgs_pct_exc_capped, sa.wgs_pct_exc_total,
    sa.wgs_pct_5x, sa.wgs_pct_10x, sa.wgs_pct_20x, sa.wgs_pct_30x,
    sa.wgs_pct_40x, sa.wgs_pct_50x, sa.wgs_pct_60x, sa.wgs_pct_70x,
    sa.wgs_pct_80x, sa.wgs_pct_90x, sa.wgs_pct_100x,   sa.wgs_pct_15x, sa.wgs_pct_25x,
           
    sa.rwgs_genome_territory, sa.rwgs_mean_coverage,
    sa.rwgs_sd_coverage, sa.rwgs_median_coverage, sa.rwgs_mad_coverage,
    sa.rwgs_pct_exc_mapq, sa.rwgs_pct_exc_dupe,
    sa.rwgs_pct_exc_unpaired, sa.rwgs_pct_exc_baseq,
    sa.rwgs_pct_exc_overlap, sa.rwgs_pct_exc_capped,
    sa.rwgs_pct_exc_total, sa.rwgs_pct_5x, sa.rwgs_pct_10x,
    sa.rwgs_pct_20x, sa.rwgs_pct_30x, sa.rwgs_pct_40x, sa.rwgs_pct_50x,
    sa.rwgs_pct_60x, sa.rwgs_pct_70x, sa.rwgs_pct_80x, sa.rwgs_pct_90x,
    sa.rwgs_pct_100x, sa.rwgs_pct_15x, sa.rwgs_pct_25x,

    sa.latest_plated_date, sa.latest_sage_qpcr_date,
    sa.latest_bsp_concentration, sa.latest_bsp_volume, sa.latest_sage_qpcr,

    la.lib_avg_dup_pct,
    la.lib_max_dup_pct,
    la. lib_min_dup_pct,

    la.lib_avg_est_lib_sz,
    la.lib_max_est_lib_sz,
    la.lib_min_est_lib_sz,

    la.lib_avg_mean_ins_sz,
    la.lib_max_mean_ins_sz,
    la.lib_min_mean_ins_sz

FROM 
    target,
    dcfm cfm,
    desig,
    first_contam,
    sample_agg sa , 
    lib_agg   la,
    billing,
    min_agg_by_dcfm, 
    min_agg_by_aggproj,
    lcsets ,
    ng_bam_files_mat bam,
    bsp.analytics_sample@analytics.gap_prod s

WHERE 
    target.pdo_name = cfm.pdo_name(+)
    AND target.pdo_sample = cfm.pdo_sample(+)
    AND target.sample_position = cfm.sample_position(+)

    AND target.pdo_name = desig.pdo_name(+)
    AND target.pdo_sample = desig.pdo_sample(+)
    AND target.sample_position = desig.sample_position(+)

    AND target.pdo_name = first_contam.pdo_name(+)
    AND target.pdo_sample = first_contam.pdo_sample(+)
    AND target.sample_position = first_contam.sample_position(+)

    AND target.pdo_name = sa.pdo_name(+)
    AND target.pdo_sample = sa.pdo_sample(+)
    AND target.sample_position = sa.sample_position(+)
	and (case when target.data_type = sa.data_type or nvl(sa.data_type , 'N/A') = 'N/A' then 1 else 0 end =1 )

    AND target.pdo_name = la.pdo_name(+)
    AND target.pdo_sample = la.pdo_sample(+)
    AND target.sample_position = la.sample_position(+)

    AND target.pdo_name = billing.pdo_name(+)
    AND target.pdo_sample = billing.pdo_sample(+)
    AND target.sample_position = billing.sample_position(+)

    AND target.pdo_name = min_agg_by_dcfm.pdo_name(+)
    AND target.pdo_sample = min_agg_by_dcfm.pdo_sample(+)
    AND target.sample_position = min_agg_by_dcfm.sample_position(+)

    AND target.pdo_name = min_agg_by_aggproj.pdo_name(+)
    AND target.pdo_sample = min_agg_by_aggproj.pdo_sample(+)
    AND target.sample_position = min_agg_by_aggproj.sample_position(+)

    AND target.pdo_name = lcsets.pdo_name(+)
    AND target.pdo_sample = lcsets.pdo_sample(+)
    
    AND target.project_name = bam.project(+)
    AND target.external_id = bam.sample(+)

    AND substr(target.pdo_sample, 4) = s.sample_id(+) -- fails when a GSSR ID is the PDO sample
;

--STATEMENT
DELETE FROM target_mm
WHERE sessionid=userenv('SESSIONID');
