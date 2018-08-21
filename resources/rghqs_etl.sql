--STATEMENT
DELETE FROM COGNOS.slxre_rghqs_targets_tmp ;

--STATEMENT
INSERT INTO COGNOS.slxre_rghqs_targets_tmp
SELECT DISTINCT flowcell_barcode, run_name, NULL /*max(rid)*/ AS target_rowid FROM (
SELECT tr.flowcell_barcode, tr.run_name/*, tr.rowid rid*/
FROM COGNOS.slxre_rghqs_targets tr
JOIN COGNOS.slxre_readgroup_metadata mtd ON mtd.flowcell_barcode=tr.flowcell_barcode
WHERE NVL(rghqs_timestamp, '1-jan-2000')<rgmd_timestamp AND blacklist_timestamp IS NULL
AND tr.flowcell_barcode IN (/*DELTA*/)
UNION ALL
SELECT rqc.flowcell_barcode, rqc.run_name/*, max(rqc.rowid) rid*/
FROM metrics.rapid_qc_readgroup_metrics/*DBLINK*/ rqc  
WHERE rqc.TYPE ='RAW'
AND rqc.analysis_date >= trunc(sysdate)-3
AND rqc.flowcell_barcode<>'H3VJ3BBXX' -- UGLY hack
--and rqc.flowcell_barcode <> 'HK3WWCCXY'
--GROUP BY rqc.flowcell_barcode, rqc.run_name 
)
--GROUP BY flowcell_barcode, run_name
 ;

--STATEMENT
DELETE FROM COGNOS.slxre_rghqs_picard_targets_tmp;

--STATEMENT
INSERT INTO  COGNOS.slxre_rghqs_picard_targets_tmp  
SELECT pa.* -- PK: id
FROM metrics.picard_analysis/*DBLINK*/ pa
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.flowcell_barcode=pa.flowcell_barcode
WHERE library_name<>'Solexa-IC'
;

--STATEMENT
DELETE FROM COGNOS.slxre2_rghqs nologging
WHERE flowcell_barcode IN (SELECT flowcell_barcode FROM COGNOS.slxre_rghqs_targets_tmp) ;

--STATEMENT
INSERT INTO COGNOS.slxre2_rghqs nologging
WITH
basecal AS (
SELECT ba.*, bm.*
-- PK: flowcell_barcode, lane, molecular_barcode_name,
FROM metrics.basecalling_analysis/*DBLINK*/ ba
JOIN metrics.basecalling_metrics/*DBLINK*/ bm ON bm.basecalling_analysis_id=ba.id
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.flowcell_barcode=ba.flowcell_barcode
WHERE ba.metrics_type IN ('Indexed', 'Unindexed')
),
/*
picard AS
(SELECT pa.* -- PK: id
FROM metrics.picard_analysis/*DBLINK* / pa
JOIN slxre_rghqs_targets_tmp tar ON tar.flowcell_barcode=pa.flowcell_barcode
WHERE library_name<>'Solexa-IC'
),
*/
al_reads AS (
  SELECT
    --PK
    al.picard_analysis_id,
--
CASE WHEN MAX(CATEGORY)='UNPAIRED' THEN 'UNPAIRED' ELSE 'PAIRED' END lane_type,

SUM(CASE WHEN CATEGORY IN ('FIRST_OF_PAIR','UNPAIRED')  THEN total_reads      END) READ1_total_reads,
SUM(CASE WHEN CATEGORY IN ('FIRST_OF_PAIR','UNPAIRED')  THEN pf_reads         END) READ1_pf_reads,
SUM(CASE WHEN CATEGORY IN ('FIRST_OF_PAIR','UNPAIRED')  THEN pf_reads_aligned END) READ1_pf_reads_aligned,
SUM(CASE WHEN CATEGORY IN ('FIRST_OF_PAIR','UNPAIRED')  THEN pf_hq_error_rate END) READ1_pf_hq_error_rate,
SUM(CASE WHEN CATEGORY IN ('FIRST_OF_PAIR','UNPAIRED')  THEN pf_aligned_bases END) READ1_pf_aligned_bases,
SUM(CASE WHEN CATEGORY IN ('FIRST_OF_PAIR','UNPAIRED')  THEN pf_mismatch_rate END) READ1_pf_mismatch_rate,

SUM(CASE WHEN CATEGORY='SECOND_OF_PAIR'                 THEN total_reads      END) READ2_total_reads,
SUM(CASE WHEN CATEGORY='SECOND_OF_PAIR'                 THEN pf_reads         END) READ2_pf_reads,
SUM(CASE WHEN CATEGORY='SECOND_OF_PAIR'                 THEN pf_reads_aligned END) READ2_pf_reads_aligned,
SUM(CASE WHEN CATEGORY='SECOND_OF_PAIR'                 THEN pf_hq_error_rate END) READ2_pf_hq_error_rate,
SUM(CASE WHEN CATEGORY='SECOND_OF_PAIR'                 THEN pf_aligned_bases END) READ2_pf_aligned_bases,
SUM(CASE WHEN CATEGORY='SECOND_OF_PAIR'                 THEN pf_mismatch_rate END) READ2_pf_mismatch_rate
FROM metrics.picard_alignment_summary/*DBLINK*/ al
JOIN COGNOS.slxre_rghqs_picard_targets_tmp picard ON picard.id=al.picard_analysis_id
WHERE nvl(al.category, 'NULL') IN ('FIRST_OF_PAIR', 'SECOND_OF_PAIR', 'UNPAIRED', 'NULL')
GROUP BY al.picard_analysis_id
),

ins AS (
  SELECT * FROM (
    SELECT
      --PK: picard_analysis_id
      ins.*,
  row_number() OVER(PARTITION BY picard.id ORDER BY ins.read_pairs desc) myrank

FROM metrics.picard_insert_size/*DBLINK*/ ins
JOIN COGNOS.slxre_rghqs_picard_targets_tmp picard ON picard.id=ins.picard_analysis_id
)
WHERE myrank=1
),

motif AS (
SELECT
    --PK
    motif.picard_analysis_id,
--
max(decode(motif.name, 'MOTIF0', relative_coverage, null)) MOTIF0_relative_coverage,
max(decode(motif.name, 'MOTIF1', relative_coverage, null)) MOTIF1_relative_coverage,
max(decode(motif.name, 'MOTIF2', relative_coverage, null)) MOTIF2_relative_coverage,
max(decode(motif.name, 'MOTIF3', relative_coverage, null)) MOTIF3_relative_coverage,
max(decode(motif.name, 'MOTIF4', relative_coverage, null)) MOTIF4_relative_coverage
FROM metrics.picard_bad_coverage_motifs/*DBLINK*/ motif
JOIN COGNOS.slxre_rghqs_picard_targets_tmp picard ON picard.id=motif.picard_analysis_id
GROUP BY motif.picard_analysis_id
)

SELECT
-- PK: rlbm.flowcell_barcode, lane, molecular_indexing_scheme
rgmd.flowcell_barcode,
to_number(rgmd.lane) lane ,
rgmd.molecular_indexing_scheme,
--

rgmd.rgmd_id,

rgmd.product_order_key product_order_name,
rgmd.product_order_sample,
rgmd.product,
rgmd.product_part_number,
rgmd.product_family    ,

null run_id,
rgmd.molecular_indexing_scheme barcode,
SYSDATE timestamp,

-- FK
  to_number(null) flowcell_lane_id,
to_number(null) run_lane_id,
--

null                readgroup_completion_date,
to_number(null)     partial_analysis,
to_number(null)       partial_analysis_end_base,

''                      fc_lane_comments,
rgmd.loading_concentration fc_lane_concentration,
''                      fc_comments,
null                    fc_registration_date,
rgmd.sequenced_library  solexa_library,
'Not Available'         solexa_library_type,
''                      solexa_library_descr,
rgmd.sequenced_library_creation solexa_library_creation_date,

'' bridgeamplified_station,
to_date(NULL) bridgeamplified_date,

to_number(null) metadata_clps_total,
to_number(null) metadata_clps_gssr_id,
to_number(null) metadata_clps_funding_source,

rgmd.project ,
'Not Available' seq_project_type,
'Not Available' funding_source,
rgmd.initiative,
'Not Available' quote_id,

rgmd.gssr_barcodes          gssr_id,
'Not Available'             collaborator ,
rgmd.collaborator_sample_id external_id,
rgmd.collaborator_sample_id collaborator_external_id,
bs.collaborator_pt_ID       individual_name,  -- rgm.collaborator_participant_id; could be bsp.analytics_sample@ANALYTICS_GAP_PROD.ORA01.collaborator_pt_id
'Not Available'             cell_line,
'Not Available'             method_of_immortalization,
'Not Available'             irb_number,
rgmd.lsid                   lsid,
bs.tissue_site              tissue_source,  -- bsp_sample.materila_type or original_material_type; most likely bsp.analytics_sample@ANALYTICS_GAP_PROD.ORA01.tissue_site

'Not Available' organism,
rgmd.organism_scientific_name ,
'Not Available' genus,
rgmd.species     species,
rgmd.strain,

rgmd.work_request_id        wr_id,
rgmd.work_request_type      wr_type,
rgmd.work_request_domain    wr_domain,
to_date(null)               wr_created_on,
--    rlbm.wr_status,
'Not Available' wr_priority,
'Not Available' wr_cycle_count,
'Not Available' wr_lanes_requested,
'Not Available' wr_comments,
rgmd.analysis_type,
rgmd.reference_sequence reference_sequence_name,
'Not Available' reference_sequence_filename,
'Not Available' reference_sequence_max_ver ,


r.run_NAME          run_name,
rgmd.run_barcode    run_barcode,
rgmd.sequencer      run_instrument,
r.instrument_model  run_instrument_model, -- rgm.sequencer_model
to_date(null)       run_registration_date,
r.run_date          run_date,
r.run_end_date      run_end_date,
'N/A'               run_status,
''                  run_description,
'Not Available'     run_operator_name,

-- Machine location
to_number(null) run_instrument_location_row,
to_number(null) run_instrument_location_column,
to_number(null) run_instrument_map_position_X,
to_number(null) run_instrument_map_position_Y,
to_number(null) run_instrument_block,

to_number(null) bcl_analysis_id,

r.read_type lane_type,

r.read_length avg_read_length,
CASE
WHEN r.read_length<=8 THEN '8'
WHEN r.read_length<=18 THEN '18'
WHEN r.read_length<=25 THEN '25'
WHEN r.read_length<=36 THEN '36'
WHEN r.read_length<=44 THEN '44'
WHEN r.read_length<=51 THEN '51'
WHEN r.read_length<=76 THEN '76'
WHEN r.read_length<=101 THEN '101'
WHEN r.read_length<=126 THEN '126'
WHEN r.read_length<=152 THEN '152'
WHEN r.read_length<=175 THEN '175'
WHEN r.read_length<=200 THEN '200'
WHEN r.read_length<=225 THEN '225'
WHEN r.read_length<=250 THEN '250'
WHEN r.read_length<=275 THEN '275'
WHEN r.read_length<=300 THEN '300'
WHEN r.read_length>300 THEN '>300'
ELSE 'UNKNOWN'
END avg_read_length_bin,

rgmd.run_fraction,

to_number(null) n_tiles,

--Basecalling
  to_number(null)                  bc_analysis_id,
basecal.workflow_end_date             bc_analysis_end_time,
basecal.mean_clusters_per_tile        bc_avg_clusters_per_tile,
basecal.mean_pf_clusters_per_tile     bc_avg_pf_clusters_per_tile,
100*basecal.mean_pct_pf_clusters_per_tile bc_pct_pf_clusters,
basecal.pf_bases                      bc_total_pf_bases,
''                               bc_version,

basecal.total_reads  bc_total_reads,
basecal.pf_reads     bc_pf_reads,
basecal.total_bases  bc_total_bases,

-- Gerald
  to_number(null)    grld_analysis_id,
to_date(null)      grld_analysis_end_time,
to_number(null)    grld_pct_align_pf      ,
to_number(null)    grld_avg_alignment_score,
to_number(null)    grld_pct_error_rate_pf   ,
to_number(null)    grld_pct_error_rate_raw   ,
to_number(null)    grld_equiv_perfect_clusters,
to_number(null)    grld_equiv_perfect_clusters_pf,
to_number(null)    grld_q20                       ,
to_number(null)    grld_q30                       ,
to_number(null)    grld_q40                       ,
to_number(null)    grld_pf_reads_aligned          ,
to_number(null)    grld_pf_bases_aligned          ,

basecal.library_name library,
to_number(null)     picard_analysis_id,
pa.workflow_end_date picard_analysis_end_time,

-- DUP metrics --
  --    0 dup_metrics_id,
to_number(null)               dup_metrics_id,
dup.unpaired_reads_examined   dup_unpaired_reads_examined,
dup.read_pairs_examined       dup_read_pairs_examined,
dup.unmapped_reads            dup_unmapped_reads,
dup.unpaired_read_duplicates  dup_unpaired_read_duplicates,
dup.read_pair_duplicates      dup_read_pair_duplicates,
100*dup.percent_duplication   dup_percent_duplication,
dup.estimated_library_size    dup_estimated_library_size,
dup.read_pair_optical_duplicates  dup_read_pair_optical_dups,

-- HS metrics --
  --    0 hs_metrics_id,
to_number(null)               hs_metrics_id,
hs.bait_set                   hs_bait_set,
hs.genome_size                hs_genome_size,
hs.bait_territory             hs_bait_territory,
hs.target_territory           hs_target_territory,
hs.bait_design_efficiency     hs_bait_design_efficiency,
hs.total_reads                hs_total_reads,
hs.pf_reads                   hs_pf_reads,
hs.pf_unique_reads            hs_pf_unique_reads,
100*hs.pct_pf_reads               hs_pct_pf_reads,
100*hs.pct_pf_uq_reads            hs_pct_pf_uq_reads,
hs.pf_uq_reads_aligned        hs_pf_uq_reads_aligned,
100*hs.pct_pf_uq_reads_aligned    hs_pct_pf_uq_reads_aligned,
hs.pf_uq_bases_aligned        hs_pf_uq_bases_aligned,
hs.on_bait_bases              hs_on_bait_bases,
hs.near_bait_bases            hs_near_bait_bases,
hs.off_bait_bases             hs_off_bait_bases,
hs.on_target_bases            hs_on_target_bases,
100*hs.pct_selected_bases         hs_pct_selected_bases,
100*hs.pct_off_bait               hs_pct_off_bait,
hs.on_bait_vs_selected        hs_on_bait_vs_selected,
hs.mean_bait_coverage         hs_mean_bait_coverage,
hs.mean_target_coverage       hs_mean_target_coverage,
hs.fold_enrichment            hs_fold_enrichment,
100*hs.zero_cvg_targets_pct       hs_zero_cvg_targets_pct,
hs.fold_80_base_penalty       hs_fold_80_base_penalty,
100*hs.pct_target_bases_2x        hs_pct_target_bases_2x,
100*hs.pct_target_bases_10x       hs_pct_target_bases_10x,
100*hs.pct_target_bases_20x       hs_pct_target_bases_20x,
100*hs.pct_target_bases_30x       hs_pct_target_bases_30x,
100*hs.pct_target_bases_40x     hs_pct_target_bases_40x,
100*hs.pct_target_bases_50x     hs_pct_target_bases_50x,
100*hs.pct_target_bases_100x    hs_pct_target_bases_100x,
100*hs.pct_usable_bases_on_bait   hs_pct_usable_bases_on_bait,
100*hs.pct_usable_bases_on_target hs_pct_usable_bases_on_target,
hs.hs_library_size            hs_library_size,
hs.hs_penalty_10x             hs_penalty_10x,
hs.hs_penalty_20x             hs_penalty_20x,
hs.hs_penalty_30x             hs_penalty_30x,
hs_penalty_40x      hs_penalty_40x,
hs_penalty_50x      hs_penalty_50x,
hs_penalty_100x     hs_penalty_100x,
hs.at_dropout                 hs_at_dropout,
hs.gc_dropout                 hs_gc_dropout,

-- ALIGN metrics --
  --    0 al_metrics_id,
to_number(null)                     al_metrics_id,
al.total_reads                      al_total_reads,
al.pf_reads                         al_pf_reads,
100*al.pct_pf_reads                 al_pct_pf_reads,
--
--nvl(al.pf_noise_reads, rqc.pf_noise_reads)                   al_pf_noise_reads,
al.pf_noise_reads,
--
--nvl(al.pf_reads_aligned, rqc.pf_reads_aligned)                 al_pf_reads_aligned,
al.pf_reads_aligned,
100*al.pct_pf_reads_aligned         al_pct_pf_reads_aligned,
al.pf_hq_aligned_reads              al_pf_hq_aligned_reads,
al.pf_hq_aligned_bases              al_pf_hq_aligned_bases,
al.pf_hq_aligned_q20_bases          al_pf_hq_aligned_q20_bases,
100*al.pf_hq_error_rate             al_pf_hq_error_rate,
al.mean_read_length                 al_mean_read_length,
--
--nvl(al.reads_aligned_in_pairs, 2*rqc.pf_read_pairs)           al_reads_aligned_in_pairs,
al.reads_aligned_in_pairs,
100*al.pct_reads_aligned_in_pairs al_pct_reads_aligned_in_pairs,
al.bad_cycles                       al_bad_cycles,
al.strand_balance                   al_pct_strand_balance,
al.pf_aligned_bases                 al_pf_aligned_bases,
100*al.pf_mismatch_rate             al_pf_mismatch_rate,
100*al.pf_indel_rate                al_pf_indel_rate,

al_reads.lane_type al_lane_type,
--100*nvl(al.pct_adapter  , decode(rqc.pf_read_pairs, 0, 0, rqc.PF_ADAPTER_READS /(2*rqc.pf_read_pairs) ) )                al_pct_adapter,
100*al.pct_adapter  al_pct_adapter,
al_reads.read1_total_reads          al_read1_total_reads,
al_reads.read1_pf_reads             al_read1_pf_reads,
al_reads.read1_pf_reads_aligned     al_read1_pf_reads_aligned,
100*al_reads.read1_pf_hq_error_rate al_read1_pf_hq_error_rate,
al_reads.read1_pf_aligned_bases     al_read1_pf_aligned_bases,
100*al_reads.read1_pf_mismatch_rate al_read1_pf_mismatch_rate,

al_reads.read2_total_reads          al_read2_total_reads,
al_reads.read2_pf_reads             al_read2_pf_reads,
al_reads.read2_pf_reads_aligned     al_read2_pf_reads_aligned,
100*al_reads.read2_pf_hq_error_rate al_read2_pf_hq_error_rate,
al_reads.read2_pf_aligned_bases     al_read2_pf_aligned_bases,
100*al_reads.read2_pf_mismatch_rate al_read2_pf_mismatch_rate,

-- DBSNP metrics - not at read group level - variant analysis
  to_number(null)             snp_metrics_id,
to_number(null)             snp_total_snps,
to_number(null)             snp_pct_dbsnp,
to_number(null)             snp_num_in_dbsnp,

-- Insert metrics --
  --    0 ins_metrics_id,
to_number(null)               ins_metrics_id,
ins.pair_orientation          ins_pair_orientation,
ins.median_insert_size        ins_median_insert_size,
ins.min_insert_size           ins_min_insert_size,
ins.max_insert_size           ins_max_insert_size,
--
--nvl(ins.mean_insert_size , rqc.mean_insert_size)         ins_mean_insert_size,
ins.mean_insert_size ,
ins.standard_deviation        ins_standard_deviation,
ins.read_pairs                ins_read_pairs,
ins.width_of_10_percent       ins_width_of_10_percent,
ins.width_of_20_percent       ins_width_of_20_percent,
ins.width_of_30_percent       ins_width_of_30_percent,
ins.width_of_40_percent       ins_width_of_40_percent,
ins.width_of_50_percent       ins_width_of_50_percent,
ins.width_of_60_percent       ins_width_of_60_percent,
ins.width_of_70_percent       ins_width_of_70_percent,
ins.width_of_80_percent       ins_width_of_80_percent,
ins.width_of_90_percent       ins_width_of_90_percent,
ins.width_of_99_percent       ins_width_of_99_percent,

-- Jumping lib metrics --
--    0 jmp_metrics_id,
to_number(null)                     jmp_metrics_id,
jmp.jump_pairs                jmp_jump_pairs,
jmp.jump_duplicate_pairs      jmp_jump_duplicate_pairs,
100*jmp.jump_duplicate_pct    jmp_jump_duplicate_pct,
jmp.jump_library_size         jmp_jump_library_size,
jmp.jump_mean_insert_size     jmp_jump_mean_insert_size,
jmp.jump_stdev_insert_size    jmp_jump_stdev_insert_size,
jmp.nonjump_pairs             jmp_nonjump_pairs,
jmp.nonjump_duplicate_pairs   jmp_nonjump_duplicate_pairs,
100*jmp.nonjump_duplicate_pct jmp_nonjump_duplicate_pct,
jmp.nonjump_library_size      jmp_nonjump_library_size,
jmp.nonjump_mean_insert_size  jmp_nonjump_mean_insert_size,
jmp.nonjump_stdev_insert_size jmp_nonjump_stdev_insert_size,
jmp.chimeric_pairs            jmp_chimeric_pairs,
jmp.fragments                 jmp_fragments,
100*jmp.pct_jumps             jmp_pct_jumps,
100*jmp.pct_nonjumps          jmp_pct_nonjumps,
100*jmp.pct_chimeras          jmp_pct_chimeras,

-- Fingerprint  metrics
to_number(null)                     fp_metric_id,
' '                                 fp_panel_name,
to_number(null)                     fp_panel_snps,
to_number(null)                     fp_confident_calls,
to_number(null)                     fp_confident_matching_snps,
to_number(null)                     fp_confident_called_pct,
to_number(null)                     fp_confident_matching_snps_pct,

-- Concordance lp metrics for category 'HOMOZYGOUS_NON_REFERENCE'
to_number(null)                     lpcncrd_metrics_id,
to_number(null)                     lpcncrd_reference,
to_number(null)                     lpcncrd_non_reference,
to_number(null)                     lpcncrd_pct_concordance,

-- Library quality yield metrics
to_number(null)               qyld_metrics_id,
--    0 qyld_metrics_id,
qyld.total_reads              qyld_total_reads,
qyld.pf_reads                 qyld_pf_reads,
qyld.read_length              qyld_read_length,
qyld.total_bases              qyld_total_bases,
--
nvl(qyld.pf_bases , decode(rqc.pf_bases, 0, NULL, rqc.pf_bases))       qyld_pf_bases,
qyld.q20_bases                			qyld_q20_bases,
qyld.pf_q20_bases             			qyld_pf_q20_bases,
qyld.q20_equivalent_yield     			qyld_q20_equivalent_yield,
qyld.pf_q20_equivalent_yield  			qyld_pf_q20_equivalent_yield,
qyld.q30_bases                			qyld_q30_bases,
nvl(qyld.pf_q30_bases , decode(rqc.pf_q30_bases, 0, NULL, rqc.pf_q30_bases)) qyld_pf_q30_bases,

-- Fingerprint Version2 metrics
  to_number(null)                       fpv2_metrics_id,
--    0 fpv2_metrics_id,
nvl(fingpn.lod_expected_sample , rqc.fingerprint_lod)           fpv2_lod,
fingpn.haplotypes_with_genotypes      fpv2_snps,
fingpn.haplotypes_confidently_checked fpv2_snps_checked,
fingpn.haplotypes_confidently_matchin fpv2_snps_matching,

-- GC Bias metrics
  to_number(null)               gcbias_metrics_id,
--    0 gcbias_metrics_id,
gcbias.window_size            gcbias_window_size,
gcbias.total_clusters         gcbias_total_clusters,
gcbias.aligned_reads          gcbias_aligned_reads,
gcbias.at_dropout             gcbias_at_dropout,
gcbias.gc_dropout             gcbias_gc_dropout,

-- Library Coverage metrics
  to_number(null)                       cvr_metrics_id,
--     0 cvr_metrics_id,
libcov.unambiguous_bases              cvr_ambiguous_bases,  ---??? values are the same in seq20 and metrics schema PIC-548???
  libcov.excluded_bases                 cvr_excluded_bases,
libcov.total_bases                    cvr_total_bases,
libcov.total_reads                    cvr_total_reads,
libcov.mapped_reads                   cvr_mapped_reads,
libcov.reads_per_base                 cvr_reads_per_base,
libcov.coverage_per_read_base         cvr_coverage_per_read_base,
libcov.deletions_per_read_base        cvr_deletions_per_read_base,
libcov.insertions_per_read_base       cvr_insertions_per_read_base,
libcov.clips_per_read_base            cvr_clips_per_read_base,
libcov.mean_coverage                  cvr_mean_coverage,
libcov.mean_quality                   cvr_mean_quality,
libcov.mean_mismatches_per_coverage   cvr_mean_mismatch_per_coverage,
libcov.mean_deletes_per_coverage      cvr_mean_deletes_per_coverage,
libcov.mean_inserts_per_coverage      cvr_mean_inserts_per_coverage,
libcov.mean_clips_per_coverage        cvr_mean_clips_per_coverage,
libcov.mean_read_starts               cvr_mean_read_starts,
libcov.var_read_starts                cvr_var_read_starts,

-- Rna Seq metrics
  to_number(null)                    rnaseq_metrics_id ,
--    0 rnaseq_metrics_id ,
rna.pf_aligned_bases               rnaseq_pf_aligned_bases,
rna.ribosomal_bases                rnaseq_ribosomal_bases,
rna.coding_bases                   rnaseq_coding_bases,
rna.utr_bases                      rnaseq_utr_bases,
rna.intronic_bases                 rnaseq_intronic_bases,
rna.intergenic_bases               rnaseq_intergenic_bases,
rna.correct_strand_reads           rnaseq_correct_strand_reads,
rna.incorrect_strand_reads         rnaseq_incorrect_strand_reads,
100*rna.pct_ribosomal_bases        rnaseq_pct_ribosomal_bases,
100*rna.pct_coding_bases           rnaseq_pct_coding_bases,
100*rna.pct_utr_bases              rnaseq_pct_utr_bases,
100*rna.pct_intronic_bases         rnaseq_pct_intronic_bases,
100*rna.pct_intergenic_bases       rnaseq_pct_intergenic_bases,
100*rna.pct_mrna_bases             rnaseq_pct_mrna_bases,
100*rna.pct_correct_strand_reads   rnaseq_pct_crrct_strand_reads,
rna.pf_bases                       rnaseq_pf_bases,
100*rna.pct_usable_bases           rnaseq_pct_usable_bases,
rna.median_cv_coverage             rnaseq_median_cv_coverage,
rna.median_5prime_bias             rnaseq_median_5prime_bias,
rna.median_3prime_bias             rnaseq_median_3prime_bias,
rna.median_5prime_to_3prime_bias   rnaseq_median_5pr_to_3pr_bias,

-- Motif metrics
motif.MOTIF0_relative_coverage,
motif.MOTIF1_relative_coverage,
motif.MOTIF2_relative_coverage,
motif.MOTIF3_relative_coverage,
motif.MOTIF4_relative_coverage,

-- PCR metrics
pcr.custom_amplicon_set         pcr_custom_amplicon_set,
pcr.genome_size                 pcr_genome_size,
pcr.amplicon_territory          pcr_amplicon_territory,
pcr.target_territory            pcr_target_territory,
pcr.total_reads                 pcr_total_reads,
pcr.pf_reads                    pcr_pf_reads,
pcr.pf_bases                    pcr_pf_bases,
pcr.pf_unique_reads             pcr_pf_unique_reads,
100*pcr.pct_pf_reads                pcr_pct_pf_reads,
100*pcr.pct_pf_uq_reads             pcr_pct_pf_uq_reads,
pcr.pf_uq_reads_aligned         pcr_pf_uq_reads_aligned,
pcr.pf_selected_pairs           pcr_pf_selected_pairs,
pcr.pf_selected_unique_pairs    pcr_pf_selected_unique_pairs,
100*pcr.pct_pf_uq_reads_aligned     pcr_pct_pf_uq_reads_aligned,
pcr.pf_uq_bases_aligned         pcr_pf_uq_bases_aligned,
pcr.on_amplicon_bases           pcr_on_amplicon_bases,
pcr.near_amplicon_bases         pcr_near_amplicon_bases,
pcr.off_amplicon_bases          pcr_off_amplicon_bases,
pcr.on_target_bases             pcr_on_target_bases,
pcr.on_target_from_pair_bases   pcr_on_target_from_pair_bases,
100*pcr.pct_amplified_bases         pcr_pct_amplified_bases,
100*pcr.pct_off_amplicon            pcr_pct_off_amplicon,
pcr.on_amplicon_vs_selected     pcr_on_amplicon_vs_selected,
pcr.mean_amplicon_coverage      pcr_mean_amplicon_coverage,
pcr.mean_target_coverage        pcr_mean_target_coverage,
pcr.fold_enrichment             pcr_fold_enrichment,
100*pcr.zero_cvg_targets_pct        pcr_zero_cvg_targets_pct,
pcr.fold_80_base_penalty        pcr_fold_80_base_penalty,
100*pcr.pct_target_bases_2x         pcr_pct_target_bases_2x,
100*pcr.pct_target_bases_10x        pcr_pct_target_bases_10x,
100*pcr.pct_target_bases_20x        pcr_pct_target_bases_20x,
100*pcr.pct_target_bases_30x        pcr_pct_target_bases_30x,
pcr.at_dropout                  pcr_at_dropout,
pcr.gc_dropout                  pcr_gc_dropout,

--RRBS metrics
rrbs.reads_aligned              rrbs_reads_aligned,
rrbs.non_cpg_bases              rrbs_non_cpg_bases,
rrbs.non_cpg_converted_bases    rrbs_non_cpg_converted_bases,
100*rrbs.pct_non_cpg_bases_converted rrbs_pct_non_cpg_bases_convrtd,
rrbs.cpg_bases_seen             rrbs_cpg_bases_seen,
rrbs.cpg_bases_converted        rrbs_cpg_bases_converted,
100*rrbs.pct_cpg_bases_converted    rrbs_pct_cpg_bases_converted,
rrbs.mean_cpg_coverage          rrbs_mean_cpg_coverage,
rrbs.median_cpg_coverage        rrbs_median_cpg_coverage,
rrbs.reads_with_no_cpg          rrbs_reads_with_no_cpg,
rrbs.reads_ignored_short        rrbs_reads_ignored_short,
rrbs.reads_ignored_mismatches   rrbs_reads_ignored_mismatches,

-- extra fields
'' run_instrument_location,

to_number(null) pass_id,
'' bsp_sample_id,
rgmd.lcset,

al.category         al_category,
--
--100*nvl(al.pct_chimeras, decode(rqc.PF_READ_PAIRS, 0, 0, rqc.PF_CHIMERIC_PAIRS/(2*rqc.PF_READ_PAIRS))) al_pct_chimeras,
100*al.pct_chimeras al_pct_chimeras,
/*SOURCE*/,
gcbias.gc_nc_0_19      gcbias_gc_nc_0_19,
gcbias.gc_nc_20_39     gcbias_gc_nc_20_39, 
gcbias.gc_nc_40_59     gcbias_gc_nc_40_59, 
gcbias.gc_nc_60_79     gcbias_gc_nc_60_79, 
gcbias.gc_nc_80_100    gcbias_gc_nc_80_100,

fingpn.het_as_hom       fpv2_het_as_hom,
fingpn.hom_as_het       fpv2_hom_as_het,
fingpn.hom_as_other_hom fpv2_hom_as_other_hom,

rgmd.research_project_id ,
rgmd.research_project_name,
rgmd.sample_id plated_sample_id

FROM COGNOS.slxre_readgroup_metadata rgmd
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.flowcell_barcode=rgmd.flowcell_barcode
JOIN COGNOS.slxre2_organic_run r ON rgmd.run_name = r.run_name
-- basecal metrics
LEFT JOIN basecal ON
  basecal.flowcell_barcode        =rgmd.flowcell_barcode AND
  basecal.lane                    =rgmd.lane AND
  (basecal.molecular_barcode_name =rgmd.molecular_indexing_scheme OR rgmd.is_greedy=1)

-- picard metrics
LEFT JOIN COGNOS.slxre_rghqs_picard_targets_tmp pa ON
pa.flowcell_barcode        =rgmd.flowcell_barcode AND
  pa.lane                    =rgmd.lane AND
  (pa.molecular_barcode_name =rgmd.molecular_indexing_scheme OR rgmd.is_greedy=1)

LEFT JOIN metrics.picard_alignment_summary/*DBLINK*/ al   ON       al.picard_analysis_id=pa.id AND nvl(al.category, 'NULL') IN ('PAIR','UNPAIRED', 'NULL')
LEFT JOIN al_reads                              ON al_reads.picard_analysis_id=pa.id
LEFT JOIN ins                                   ON      ins.picard_analysis_id=pa.id
LEFT JOIN motif                                 ON    motif.picard_analysis_id=pa.id

LEFT JOIN metrics.picard_duplication/*DBLINK*/ dup        ON      dup.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_hybrid_selection/*DBLINK*/ hs    ON       hs.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_jumping/*DBLINK*/ jmp            ON      jmp.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_fingerprint/*DBLINK*/ fingpn     ON   fingpn.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_quality_yield/*DBLINK*/ qyld     ON     qyld.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_gc_bias_summary/*DBLINK*/ gcbias ON   gcbias.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_bad_coverage/*DBLINK*/ libcov    ON   libcov.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_rna_seq/*DBLINK*/ rna            ON      rna.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_targeted_pcr/*DBLINK*/ pcr       ON      pcr.picard_analysis_id=pa.id
LEFT JOIN metrics.picard_rrbs_summary/*DBLINK*/ rrbs      ON     rrbs.picard_analysis_id=pa.id

LEFT JOIN metrics.rapid_qc_readgroup_metrics rqc          ON 
  rqc.flowcell_barcode        =rgmd.flowcell_barcode AND
  rqc.lane                    =rgmd.lane AND
  rqc.molecular_barcode_name  =rgmd.molecular_indexing_scheme AND 
  rqc.TYPE = 'RAW'

left join analytics.bsp_sample bs    ON bs.sample_barcode = rgmd.product_order_sample   --bs.sample_id = substr(rgmd.product_order_sample, 4)
WHERE
(basecal.flowcell_barcode IS NOT NULL OR pa.flowcell_barcode IS NOT NULL)
;

--STATEMENT
DELETE FROM COGNOS.slxre2_lane_hqs nologging
WHERE
run_name IN (SELECT run_name FROM COGNOS.slxre_rghqs_targets_tmp)
AND run_start_date >='1-jun-2013'
;

--STATEMENT
INSERT INTO COGNOS.slxre2_lane_hqs nologging
WITH

q30 AS (
SELECT rghqs.flowcell_barcode, rghqs.lane, 
    decode(min(nvl(rghqs.qyld_pf_bases,0)), 0, NULL,  sum(rghqs.qyld_pf_bases) )  qyld_pf_bases,
    decode(min(nvl(rghqs.qyld_pf_q30_bases,0)), 0, NULL,  sum(rghqs.qyld_pf_q30_bases) ) qyld_pf_q30_bases,
    100*sum(rghqs.qyld_pf_q30_bases)/cognos.utils.zero_to_null(sum(rghqs.qyld_pf_bases)) qyld_pct_pf_q30_bases 

FROM COGNOS.slxre2_rghqs rghqs,
    COGNOS.slxre_rghqs_targets_tmp t 
WHERE rghqs.flowcell_barcode = t.flowcell_barcode
GROUP BY rghqs.flowcell_barcode, rghqs.lane
),

basecal_rg_agg AS (
SELECT
--PK
ba.flowcell_barcode,
ba.lane,
--

sum(decode(ba.metrics_type, 'Unmatched', bcm.pf_bases                , 0))  pf_bases_Unmatched,
sum(decode(ba.metrics_type, 'Indexed'  , bcm.pf_bases, 'Unindexed',bcm.pf_bases  , 0))  pf_bases_Indexed,  --basecal_lane.pf_bases_Indexed,

sum(decode(ba.metrics_type, 'Unmatched', bcm.total_bases             , 0))  total_bases_Unmatched,
sum(decode(ba.metrics_type, 'Indexed'  , bcm.total_bases, 'Unindexed', bcm.total_bases, 0) ) total_bases_Indexed,   --basecal_lane.total_bases_Indexed,

sum(decode(ba.metrics_type, 'Unmatched', bcm.total_reads             , 0))  no_match_reads,              -- lane_barcodes
sum(decode(ba.metrics_type, 'Unmatched', bcm.pf_reads                , 0))  no_match_reads_pf,               --lane_barcodes

sum(decode(ba.metrics_type, 'Indexed'  , bbcm.pf_perfect_matches     , 0))  perfect_matches_pf, -- CLUSTER-BASED  --lane_perfect_matches
sum(decode(ba.metrics_type, 'Indexed'  , bbcm.pf_one_mismatch_matches, 0))  one_mismatch_matches_pf,         --lane_perfect_matches

COUNT(DISTINCT decode(ba.metrics_type, 'Indexed', ba.molecular_barcode_name, NULL )) num_barcodes,
sum(decode(ba.metrics_type, 'Unmatched', NULL,   decode(bbcm.total_reads , NULL, 0, 0, 0, 1) ) ) lane_num_barcodes_matched

FROM COGNOS.slxre_rghqs_targets_tmp tar
JOIN metrics.basecalling_analysis/*DBLINK*/ ba            ON          ba.flowcell_barcode = tar.flowcell_barcode-- per readgroup
JOIN metrics.basecalling_metrics/*DBLINK*/ bcm            ON  bcm.basecalling_analysis_id = ba.id     -- actual template bases and reads
JOIN metrics.basecalling_barcode_metrics/*DBLINK*/ bbcm   ON bbcm.basecalling_analysis_id = ba.id
WHERE ba.metrics_type IN ('Indexed', 'Unindexed', 'Unmatched')

GROUP BY ba.flowcell_barcode, ba.lane
),

picard_al AS (
SELECT
--PK
pa.flowcell_barcode,
pa.lane,
--
-- ALL metrics listed here
sum(decode(al.category, 'PAIR',  al.total_reads             , 'UNPAIRED', al.total_reads            , NULL  )) al_total_reads ,
sum(decode(al.category, 'PAIR',  al.pf_reads                , 'UNPAIRED', al.pf_reads               , NULL  )) al_pf_reads ,
sum(decode(al.category, 'PAIR',  al.pf_reads_aligned        , 'UNPAIRED', al.pf_reads_aligned       , NULL  )) al_pf_reads_aligned ,
sum(decode(al.category, 'PAIR',  al.pf_hq_aligned_reads     , 'UNPAIRED', al.pf_hq_aligned_reads    , NULL  )) al_pf_hq_aligned_reads ,
sum(decode(al.category, 'PAIR',  al.pf_noise_reads          , 'UNPAIRED', al.pf_noise_reads         , NULL  )) al_pf_noise_reads ,
sum(decode(al.category, 'PAIR',  al.reads_aligned_in_pairs  , 'UNPAIRED', al.reads_aligned_in_pairs , NULL  )) al_reads_aligned_in_pairs ,
sum(decode(al.category, 'PAIR',  al.pf_hq_aligned_bases     , 'UNPAIRED', al.pf_hq_aligned_bases    , NULL  )) al_pf_hq_aligned_bases ,
sum(decode(al.category, 'PAIR',  al.bad_cycles              , 'UNPAIRED', al.bad_cycles             , NULL  )) al_bad_cycles,

SUM(al.pf_reads                             )/cognos.utils.zero_to_null(SUM(al.total_reads       ))        al_pct_pf_reads ,
SUM(al.pf_reads_aligned                     )/cognos.utils.zero_to_null(SUM(al.pf_reads          ))        al_pct_pf_reads_aligned ,
SUM(al.reads_aligned_in_pairs               )/cognos.utils.zero_to_null(SUM(al.pf_reads_aligned  ))        al_pct_reads_aligned_in_pairs,
100*SUM(al.pf_hq_error_rate*al.pf_reads_aligned )/cognos.utils.zero_to_null(SUM(al.pf_reads_aligned  ))    al_pf_hq_error_rate,
100*SUM(al.total_reads*al.pct_adapter           )/cognos.utils.zero_to_null(SUM(al.total_reads       ))    al_pct_adapter,

--    r.lane_type al_lane_type,

sum(decode(al.category, 'FIRST_OF_PAIR', al.total_reads         , 'UNPAIRED', al.total_reads         , null)) al_read1_total_reads,
sum(decode(al.category, 'FIRST_OF_PAIR', al.pf_reads            , 'UNPAIRED', al.pf_reads            , null)) al_read1_pf_reads,
sum(decode(al.category, 'FIRST_OF_PAIR', al.pf_reads_aligned    , 'UNPAIRED', al.pf_reads_aligned    , null)) al_read1_pf_reads_aligned,
max(decode(al.category, 'FIRST_OF_PAIR', al.pct_pf_reads        , 'UNPAIRED', al.pct_pf_reads        , null)) al_read1_pct_pf_reads,
max(decode(al.category, 'FIRST_OF_PAIR', al.pct_pf_reads_aligned, 'UNPAIRED', al.pct_pf_reads_aligned, null)) al_read1_pct_pf_reads_aligned,
100*max(decode(al.category, 'FIRST_OF_PAIR', al.pf_hq_error_rate    , 'UNPAIRED', al.pf_hq_error_rate    , null)) al_read1_pf_hq_error_rate,

sum(decode(al.category, 'SECOND_OF_PAIR', al.total_reads         , null)) al_read2_total_reads,
sum(decode(al.category, 'SECOND_OF_PAIR', al.pf_reads            , null)) al_read2_pf_reads,
sum(decode(al.category, 'SECOND_OF_PAIR', al.pf_reads_aligned    , null)) al_read2_pf_reads_aligned,
max(decode(al.category, 'SECOND_OF_PAIR', al.pct_pf_reads        , null)) al_read2_pct_pf_reads,
max(decode(al.category, 'SECOND_OF_PAIR', al.pct_pf_reads_aligned, null)) al_read2_pct_pf_reads_aligned,
100*max(decode(al.category, 'SECOND_OF_PAIR', al.pf_hq_error_rate    , null)) al_read2_pf_hq_error_rate

FROM COGNOS.slxre_rghqs_targets_tmp tar
JOIN metrics.picard_analysis/*DBLINK*/ pa          ON   pa.flowcell_barcode = tar.flowcell_barcode  --per read group
JOIN metrics.picard_alignment_summary/*DBLINK*/ al ON al.picard_analysis_id = pa.id
WHERE pa.metrics_type IN ('Indexed', 'Unindexed', 'Unmatched')     -- why omitted Unindexed ????            --pa.library_name IS NOT NULL
AND pa.library_name <> 'Solexa-IC'
GROUP BY pa.flowcell_barcode, pa.lane
),

picard_qyld AS (
SELECT
--PK
pa.flowcell_barcode,
pa.lane,
--
-- ALL metrics listed here
-- will sum indexed + unmatched - check that
SUM(qyld.total_reads)                           qyld_total_reads,
--SUM(qyld.pf_bases)            					qyld_pf_bases,
SUM(qyld.q20_bases)                             qyld_q20_bases,
SUM(qyld.pf_q20_bases)                          qyld_pf_q20_bases,
SUM(qyld.q20_equivalent_yield)                  qyld_q20_equivalent_yield,
SUM(qyld.pf_q20_equivalent_yield)               qyld_pf_q20_equivalent_yield
--SUM(qyld.pf_q30_bases)   qyld_pf_q30_bases,
--SUM(qyld.pf_q30_bases)/cognos.utils.zero_to_null(SUM(qyld.pf_bases))  qyld_pct_pf_q30_bases

FROM COGNOS.slxre_rghqs_targets_tmp tar
LEFT JOIN metrics.picard_analysis/*DBLINK*/ pa        ON     pa.flowcell_barcode = tar.flowcell_barcode AND pa.metrics_type<> 'Lane' AND pa.LIBRARY_name <> 'Solexa-IC'
LEFT JOIN metrics.picard_quality_yield/*DBLINK*/ qyld ON qyld.picard_analysis_id = pa.id
GROUP BY pa.flowcell_barcode, pa.lane
),

picard_ic AS (
  SELECT
    --PK
    pa.flowcell_barcode,
    pa.lane,
--
ic.matches,
100*ic.pct_matches pct_matches,
100*ic.mean_read1_error_rate mean_read1_error_rate,
100*ic.mean_read2_error_rate mean_read2_error_rate
FROM COGNOS.slxre_rghqs_targets_tmp tar
JOIN metrics.picard_analysis/*DBLINK*/ pa ON pa.flowcell_barcode = tar.flowcell_barcode         --per lane
JOIN metrics.picard_ic_sum/*DBLINK*/ ic ON                 pa.id = ic.picard_analysis_id  AND ic.internal_control='Total'
WHERE pa.metrics_type = 'Lane'                --pa.library_name IS NULL
),

metadata AS (
SELECT
--PK
rgmd.flowcell_barcode ,
rgmd.lane,
--
max(rgmd.source)                                                source,
max(rgmd.run_name)                                              run_name,
max(rgmd.sequenced_library)                                     sequenced_library,
max(rgmd.sequenced_library_creation)                            lib_creation_date,
cognos.concat_string_csv(DISTINCT rgmd.collaborator_sample_id)  external_id,
cognos.concat_string_csv(DISTINCT rgmd.lcset)                   lcset,
cognos.concat_string_csv(DISTINCT jira.TYPE )                   lcset_type,
cognos.concat_string_csv(DISTINCT jira.topoffs)                 lcset_topoff,
cognos.concat_string_csv(DISTINCT jira.seq_technology)          lcset_seq_technology,
cognos.concat_string_csv(DISTINCT jira.protocol)                lcset_protocol,
cognos.concat_string_csv(DISTINCT rgmd.lab_workflow)            workflow,
min(rgmd.system_of_record)                                      system_of_record,

min(rgmd.loading_concentration)                                 fc_lane_concentration,

cognos.concat_string_csv(DISTINCT decode(rgmd.system_of_record, 'MERCURY', rgmd.product_order_key, rgmd.work_request_id )) wr_id,

cognos.concat_string_csv(DISTINCT decode(rgmd.system_of_record, 'MERCURY', rgmd.product, '' )) wr_type,
cognos.concat_string_csv(DISTINCT decode(rgmd.system_of_record, 'MERCURY', 'Production', '' )) wr_domain,

cognos.concat_string_csv(DISTINCT rgmd.analysis_type ) wr_analysis_type,
cognos.concat_string_csv(DISTINCT decode(rgmd.system_of_record, 'MERCURY', rgmd.research_project_id, rgmd.project )) project,
cognos.concat_string_csv(DISTINCT decode(rgmd.system_of_record, 'MERCURY', rgmd.research_project_name, rgmd.initiative )) initiative,

cognos.concat_string_csv(DISTINCT rgmd.product) product,
cognos.concat_string_csv(DISTINCT rgmd.product_order_key) product_order_name,
cognos.concat_string_csv(DISTINCT rgmd.product_part_number) product_part_number,
cognos.concat_string_csv(DISTINCT rgmd.research_project_id) research_project_id,
cognos.concat_string_csv(DISTINCT rgmd.research_project_name) research_project_name,

min(rgmd.setup_read_structure) setup_read_structure,
min(rgmd.actual_read_structure) actual_read_structure,
cognos.concat_string_csv(DISTINCT rgmd.bait_set_name) bait_set_name,
max(nvl(pdos.sample_is_on_risk, 'F')) on_risk,
max(rgmd.is_pool_test) is_pool_test

FROM COGNOS.slxre_readgroup_metadata rgmd
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.flowcell_barcode=rgmd.flowcell_barcode
LEFT JOIN reporting.loj_issue_lcset@loj_link.seqbldr jira ON jira.KEY = rgmd.lcset 
LEFT JOIN COGNOS.mercurydw_pdo_samples pdos ON pdos.pdo_name = rgmd.product_order_key AND pdos.pdo_sample_id = rgmd.product_order_sample
GROUP BY rgmd.flowcell_barcode ,     rgmd.lane
),

bc_seq AS (

SELECT
ba.run_name,
ba.lane,
--
MAX(decode(ph.type_name, 'FIRST' , ph.phasing_applied   )) r1_phasing_applied,
MAX(decode(ph.type_name, 'FIRST' , ph.prephasing_applied)) r1_prephasing_applied,

MAX(decode(ph.type_name, 'SECOND', ph.phasing_applied   )) r2_phasing_applied,
MAX(decode(ph.type_name, 'SECOND', ph.prephasing_applied)) r2_prephasing_applied

FROM COGNOS.slxre_rghqs_targets_tmp tar
JOIN metrics.basecalling_analysis/*DBLINK*/ ba         ON        ba.flowcell_barcode = tar.flowcell_barcode 
JOIN metrics.basecalling_phasing_metrics/*DBLINK*/ ph  ON ph.basecalling_analysis_id = ba.id
GROUP BY BA.RUN_NAME, BA.LANE
)

SELECT
-- PK: run_name, lane
to_number(null) run_id ,
to_number(ba.lane) lane,
--

to_number(null) flowcell_lane_id,
to_number(null) library_id ,   -- check if it is used anywhere and remove from view if not used

r.flowcell_barcode,

null readgroup_completion_date,

-- Run info
r.read_type         lane_type,
r.run_name          run_name,
''                  run_status,
''                  run_descr,
r.instrument        run_instrument,
r.instrument_model  run_instrument_model,
to_number(null)     n_tiles,
r.run_date          run_start_date,
r.run_end_date      run_end_date,
to_date(null)       run_registration_date,

r.index_length      actual_index_length,

-- Machine location
to_number(null)     run_instrument_location_row,
to_number(null)     run_instrument_location_column,
to_number(null)     run_instrument_map_position_X,
to_number(null)     run_instrument_map_position_Y,
to_number(null)     run_instrument_block,
--

-- Library info
metadata.sequenced_library  lib_name,
''                          lib_type,
''                          lib_descr,
metadata.lib_creation_date  lib_creation_date,
metadata.lcset,
metadata.workflow           workflow,

-- FC info
to_date(null)   fc_registration_date,
''              fc_comments,
''              fc_lane_comments,
metadata.fc_lane_concentration,
--    metadata.fc_lane_concentration,  -- initialized with NULL in metadata subquery; waiting for GPLIM-1878
''              fc_desig_comments,

-- Lims info: temporarily patched from Squid LIMS and Mercury Event DM
fclp.fcl_load_station bridgeamplified_station,
fclp.event_time       bridgeamplified_date,

-- Metadata
''                      gssr_id,
metadata.external_id    external_id,
''                      cell_line,
''                      organism_name ,
''                      organism_scientific_name,
''                      organism_strain,
metadata.wr_id          wr_id ,
metadata.wr_type        wr_type ,
metadata.wr_domain      wr_domain ,
''                      wr_status ,
''                      wr_cycle_count ,
''                      wr_lanes_requested ,
metadata.wr_analysis_type wr_analysis_type,
metadata.project        project,
metadata.initiative     initiative,

metadata.product,
metadata.product_order_name,
metadata.product_part_number,
metadata.research_project_id,
metadata.research_project_name,

-- Basecal analysis info
  ba.id                   bc_analysis_id,
ba.workflow_start_date  bc_analysis_start_time,
ba.workflow_end_date    bc_analysis_end_time,

''                      bc_version,

-- Basecall
CASE
WHEN r.read_length<=8 THEN '8'
WHEN r.read_length<=18 THEN '18'
WHEN r.read_length<=25 THEN '25'
WHEN r.read_length<=36 THEN '36'
WHEN r.read_length<=44 THEN '44'
WHEN r.read_length<=51 THEN '51'
WHEN r.read_length<=76 THEN '76'
WHEN r.read_length<=101 THEN '101'
WHEN r.read_length<=126 THEN '126'
WHEN r.read_length<=152 THEN '152'
WHEN r.read_length<=175 THEN '175'
WHEN r.read_length<=200 THEN '200'
WHEN r.read_length<=225 THEN '225'
WHEN r.read_length<=250 THEN '250'
WHEN r.read_length<=275 THEN '275'
WHEN r.read_length<=300 THEN '300'
WHEN r.read_length>300 THEN '>300'
ELSE 'UNKNOWN'
END                           avg_read_length_bin,
r.read_length                 avg_read_length,

--Basecal
  bcm.mean_clusters_per_tile    avg_clusters_per_tile,
bcm.mean_pf_clusters_per_tile avg_clusters_pf_per_tile,  -- this is also avg clusters pf per tile
  bcm.total_clusters            total_clusters,
bcm.pf_clusters               pf_clusters,

bcm.pf_bases,
bcm.total_bases,

basecal_rg_agg.pf_bases_Indexed,
basecal_rg_agg.total_bases_Indexed,

basecal_rg_agg.pf_bases_Unmatched,
basecal_rg_agg.total_bases_Unmatched,

100*bcm.mean_pct_pf_clusters_per_tile pct_pf_clusters,

-- temporary reported from Squid until metrics are generated in the new metrics schema
  to_number(null)              r1_read_length,
to_number(null)              r1_avg_1st_cycle_int,
to_number(null)              r1_pct_int_after_20_cycle,
bc_seq.r1_phasing_applied    r1_pct_phasing_applied,
bc_seq.r1_prephasing_applied r1_prephasing_applied,
to_number(null)              r2_read_length,
to_number(null)              r2_avg_1st_cycle_int,
to_number(null)              r2_pct_int_after_20_cycle,
bc_seq.r2_phasing_applied    r2_pct_phasing_applied,
bc_seq.r2_prephasing_applied r2_prephasing_applied,

-- Gerald
NULL grld_q20_total,                -- gerald.q20_total              grld_q20_total,
NULL grld_q30_total,                --gerald.q30_total              grld_q30_total,
NULL grld_q40_total,                --gerald.q40_total              grld_q40_total,

NULL grld_pct_align_pf,             --gerald.pct_align_pf           grld_pct_align_pf,
NULL grld_avg_alignment_score,      --gerald.avg_alignment_score    grld_avg_alignment_score,
NULL grld_pct_error_rate_pf,        --gerald.pct_error_rate_pf      grld_pct_error_rate_pf,
NULL grld_pct_error_rate_raw,       --gerald.pct_error_rate_raw     grld_pct_error_rate_raw,

NULL grld_r1_q20,                   --gerald.r1_q20                 grld_r1_q20,
NULL grld_r1_avg_alignment_score,   --gerald.r1_avg_alignment_score grld_r1_avg_alignment_score,
NULL grld_r1_pct_error_rate,        --gerald.r1_pct_error_rate_raw  grld_r1_pct_error_rate,
NULL grld_r1_pct_error_rate_pf,     --gerald.r1_pct_error_rate_pf   grld_r1_pct_error_rate_pf,

NULL grld_r2_q20,                   --gerald.r2_q20                 grld_r2_q20,
NULL grld_r2_avg_alignment_score,   --gerald.r2_avg_alignment_score grld_r2_avg_alignment_score,
NULL grld_r2_pct_error_rate,        --gerald.r2_pct_error_rate_raw  grld_r2_pct_error_rate,
NULL grld_r2_pct_error_rate_pf,     --gerald.r2_pct_error_rate_pf   grld_r2_pct_error_rate_pf,

bcl.cluster_density,

-- Picard
  picard_al.al_total_reads ,
picard_al.al_pf_reads ,
--nvl(picard_al.al_pf_reads_aligned , rqc.pf_reads_aligned) al_pf_reads_aligned,
picard_al.al_pf_reads_aligned ,
picard_al.al_pf_hq_aligned_reads ,
--nvl(picard_al.al_pf_noise_reads , rqc.pf_noise_reads) al_pf_noise_reads,
picard_al.al_pf_noise_reads,
--nvl(picard_al.al_reads_aligned_in_pairs , rqc.pf_reads_aligned_in_pairs) al_reads_aligned_in_pairs,
picard_al.al_reads_aligned_in_pairs ,
picard_al.al_pf_hq_aligned_bases ,
picard_al.al_bad_cycles,

100*picard_al.al_pct_pf_reads               al_pct_pf_reads ,
100*picard_al.al_pct_pf_reads_aligned       al_pct_pf_reads_aligned ,
100*picard_al.al_pct_reads_aligned_in_pairs al_pct_reads_aligned_in_pairs,
picard_al.al_pf_hq_error_rate,
--nvl(picard_al.al_pct_adapter,rqc.pct_adapter) al_pct_adapter, 
picard_al.al_pct_adapter,

r.read_type al_lane_type,
picard_al.al_read1_total_reads,
picard_al.al_read1_pf_reads,
picard_al.al_read1_pf_reads_aligned,
100*picard_al.al_read1_pct_pf_reads         al_read1_pct_pf_reads,
100*picard_al.al_read1_pct_pf_reads_aligned al_read1_pct_pf_reads_aligned,
picard_al.al_read1_pf_hq_error_rate,

picard_al.al_read2_total_reads,
picard_al.al_read2_pf_reads,
picard_al.al_read2_pf_reads_aligned,
100*picard_al.al_read2_pct_pf_reads             al_read2_pct_pf_reads,
100*picard_al.al_read2_pct_pf_reads_aligned     al_read2_pct_pf_reads_aligned,
picard_al.al_read2_pf_hq_error_rate,

picard_qyld.qyld_total_reads,
q30.qyld_pf_bases,
picard_qyld.qyld_q20_bases,
picard_qyld.qyld_pf_q20_bases,
picard_qyld.qyld_q20_equivalent_yield,
picard_qyld.qyld_pf_q20_equivalent_yield,
q30.qyld_pf_q30_bases,
q30.qyld_pct_pf_q30_bases,

ic.matches                                                  ic_matches,
ic.pct_matches                                              ic_pct_matches,
ic.mean_read1_error_rate                                    ic_pct_mean_rd1_err_rate,
ic.mean_read2_error_rate                                    ic_pct_mean_rd2_err_rate,
ic.matches*r.read_length*decode(r.read_type, 'Paired', 2,1) ic_bases,

bcm.total_reads                             lb_total_reads,
basecal_rg_agg.no_match_reads               lb_no_match_reads,
basecal_rg_agg.no_match_reads_pf            lb_no_match_reads_pf,
basecal_rg_agg.num_barcodes                 lb_num_barcodes,
basecal_rg_agg.lane_num_barcodes_matched    lb_num_barcodes_matched,

basecal_rg_agg.perfect_matches_pf,
basecal_rg_agg.one_mismatch_matches_pf,

--New columns
metadata.system_of_record,

-- metrics for HiSeqX
pffail.reads                        pffail_total_reads,
pffail.pf_fail_reads                pffail_reads,
100*pffail.pct_pf_fail_reads        pffail_pct_reads,
pffail.pf_fail_empty                pffail_empty,
100*pffail.pct_pf_fail_empty        pffail_pct_empty,
pffail.pf_fail_polyclonal           pffail_polyclonal,
100*pffail.pct_pf_fail_polyclonal   pffail_pct_polyclonal,
pffail.pf_fail_misaligned           pffail_misaligned,
100*pffail.pct_pf_fail_misaligned   pffail_pct_misaligned,
pffail.pf_fail_unknown              pffail_unknown,
100*pffail.pct_pf_fail_unknown      pffail_pct_unknown,

metadata.source,
metadata.lcset_type,
metadata.lcset_topoff,
metadata.lcset_seq_technology,
metadata.lcset_protocol,
metadata.setup_read_structure,
metadata.actual_read_structure,
metadata.bait_set_name,
metadata.on_risk,
metadata.is_pool_test

FROM metrics.basecalling_analysis/*DBLINK*/ ba        -- per lane
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.flowcell_barcode=ba.flowcell_barcode
LEFT JOIN metadata                                  ON              metadata.flowcell_barcode= ba.flowcell_barcode AND        metadata.lane=ba.lane
JOIN COGNOS.slxre2_organic_run r                           ON                             r.run_name= ba.run_name
JOIN metrics.basecalling_metrics/*DBLINK*/ bcm                ON            bcm.basecalling_analysis_id= ba.id
JOIN metrics.basecalling_lane_metrics/*DBLINK*/ bcl           ON            bcl.basecalling_analysis_id= ba.id
-- new lane metrics for HiSeqX
-- Need LEFT JOIN until Pipeline team backfills for older lanes
LEFT JOIN metrics.basecalling_pffail_metrics/*DBLINK*/ pffail ON         pffail.basecalling_analysis_id= ba.id               AND pffail.tile = 'All'
LEFT JOIN basecal_rg_agg                            ON basecal_rg_agg.flowcell_barcode       = ba.flowcell_barcode AND basecal_rg_agg.lane = ba.lane
LEFT JOIN picard_al                                 ON      picard_al.flowcell_barcode       = ba.flowcell_barcode AND      picard_al.lane = ba.lane
LEFT JOIN picard_qyld                               ON    picard_qyld.flowcell_barcode       = ba.flowcell_barcode AND    picard_qyld.lane = ba.lane
LEFT JOIN picard_ic ic                              ON             ic.flowcell_barcode       = ba.flowcell_barcode AND             ic.lane = ba.lane

LEFT JOIN bc_seq                            ON                       bc_seq.run_name = ba.run_name         AND         bc_seq.lane = ba.lane
-- temporary until phasing metrics are generated in the metrics schema
--LEFT JOIN slxre2_mt_bclane_seq_view bc_seq  ON                       bc_seq.run_name = ba.run_name AND                 bc_seq.lane = ba.lane

-- temporary until FlowcellTransfer info for Squid and Mercury is provided by a single source
LEFT JOIN COGNOS.slxre2_fclane_prep fclp            ON                 fclp.flowcell_barcode = ba.flowcell_barcode

LEFT JOIN q30 ON q30.flowcell_barcode = ba.flowcell_barcode AND q30.lane = ba.lane 

WHERE ba.metrics_type = 'Lane'
AND r.run_date >='1-jun-2013'
;

--STATEMENT
DELETE FROM COGNOS.slxre2_run_hqs nologging
WHERE run_name IN (SELECT run_name FROM COGNOS.slxre_rghqs_targets_tmp)
AND run_start_date >='1-jun-2013'
;

--STATEMENT
INSERT INTO COGNOS.slxre2_run_hqs nologging
WITH
lhqs AS (
SELECT
-- PK
a.run_name,
--
max(a.setup_read_structure) 	  setup_read_structure,
max(a.actual_read_structure) 	  actual_read_structure,
min(a.flowcell_barcode          ) flowcell_barcode,
min(a.source                    ) source,

SUM(a.total_bases               ) total_bases,
SUM(a.pf_bases                  ) pf_bases,

SUM(a.total_bases_indexed       ) total_bases_indexed,
SUM(a.pf_bases_indexed          ) pf_bases_indexed,

SUM(a.total_bases_Unmatched     ) total_bases_Unmatched,
SUM(a.pf_bases_Unmatched        ) pf_bases_Unmatched,

SUM(a.qyld_pf_q30_bases         ) qyld_pf_q30_bases,
STATS_MODE(a.avg_read_length    ) avg_read_length,
STATS_MODE(a.avg_read_length_bin) avg_read_length_bin,
sum(a.ic_bases                  ) ic_bases,
avg(a.al_read1_pf_hq_error_rate ) al_read1_pf_hq_error_rate,
avg(a.al_read2_pf_hq_error_rate ) al_read2_pf_hq_error_rate,
MIN(a.lane_type                 ) lane_type,
MAX(a.fc_desig_comments         ) fc_desig_comments,
max(system_of_record)         system_of_record
FROM COGNOS.slxre2_lane_hqs a
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.run_name=a.run_name
GROUP BY a.run_name
),

--replaces zamboni subquery
picard AS (
SELECT
--PK
pa.run_name,
--
min(pa.id) picard_workflow_id,
min(pa.workflow_start_date) picard_start_time,
max(pa.workflow_end_date) picard_end_time
FROM metrics.picard_analysis/*DBLINK*/ pa
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.run_name=pa.run_name
GROUP BY pa.run_name
)

SELECT
-- PK run_name
to_number(null) run_id,
--
r.run_date         run_start_date,
r.run_end_date     run_end_date,
r.instrument       run_instrument,

r.instrument_model run_instrument_model,
r.run_name         run_name,
NULL               run_barcode,
r.flowcell_barcode fc_barcode,
r.index_length     actual_index_length,
r.is_cancelled     cancelled,

lhqs.total_bases,
lhqs.pf_bases,

lhqs.total_bases_indexed,
lhqs.pf_bases_indexed,

lhqs.total_bases_Unmatched,
lhqs.pf_bases_Unmatched,

lhqs.qyld_pf_q30_bases,
lhqs.avg_read_length,
lhqs.avg_read_length_bin,
lhqs.ic_bases,
lhqs.al_read1_pf_hq_error_rate,
lhqs.al_read2_pf_hq_error_rate,
lhqs.lane_type,
lhqs.fc_desig_comments,

to_number(null) completed_run_fraction,
to_date(null)  RGCD,

picard.picard_workflow_id,
picard.picard_start_time,
picard.picard_end_time,

-- New columns
lhqs.system_of_record,
r.run_type organic_run_type,  -- walk up, CRSP, production
lhqs.source,
lhqs.setup_read_structure,
lhqs.actual_read_structure

FROM COGNOS.slxre2_organic_run r
JOIN COGNOS.slxre_rghqs_targets_tmp tar ON tar.run_name=r.run_name
JOIN lhqs        ON    lhqs.run_name=r.run_name
LEFT JOIN picard ON  picard.run_name=r.run_name
WHERE r.run_date >='1-jun-2013'
;

--STATEMENT:fix legacy fields
BEGIN
FOR rec IN (
SELECT
--PK
hqs.flowcell_barcode,
hqs.lane,
hqs.molecular_indexing_scheme,
--
rgmd.product_order_key      wr_id,
rgmd.product                wr_type,
'Production'                wr_domain,
pdo.created_date            wr_created_on,
p.product_family_name       seq_project_type,
rgmd.research_project_id    project,
rgmd.research_project_name  initiative,
--bs.organism_scientific_name  -- bsp.analytics_sample@ANALYTICS_GAP_PROD.ORA01.species?
bs.species  organism_scientific_name

FROM COGNOS.slxre2_rghqs hqs
JOIN COGNOS.slxre_rghqs_targets_tmp tr     ON tr.flowcell_barcode=hqs.flowcell_barcode
JOIN COGNOS.slxre_readgroup_metadata rgmd  ON
  rgmd.flowcell_barcode           = hqs.flowcell_barcode AND
  rgmd.lane                       = hqs.lane AND
  rgmd.molecular_indexing_scheme  = hqs.molecular_indexing_scheme
LEFT JOIN mercurydwro.product_order pdo ON pdo.jira_ticket_key = hqs.product_order_name
LEFT JOIN mercurydwro.product p         ON p.product_name = rgmd.product
--LEFT JOIN bsp_sample bs                 ON bs.sample_id=rgmd.product_order_sample
LEFT JOIN analytics.bsp_sample bs ON bs.sample_barcode = rgmd.product_order_sample   --bs.sample_id=substr(rgmd.product_order_sample, 4)
WHERE rgmd.system_of_record = 'MERCURY'
--            AND rownum<100
) LOOP
UPDATE COGNOS.slxre2_rghqs a
SET
a.wr_id             =rec.wr_id,
a.wr_type           =rec.wr_type,
a.wr_domain         =rec.wr_domain,
a.wr_created_on     =rec.wr_created_on,
a.seq_project_type  =rec.seq_project_type,
a.project           =rec.project,
a.initiative        =rec.initiative,
a.organism_scientific_name=rec.organism_scientific_name
WHERE
a.flowcell_barcode           = rec.flowcell_barcode AND
  a.lane                       = rec.lane AND
  a.molecular_indexing_scheme  = rec.molecular_indexing_scheme
;
END LOOP ;
END ;
-- fix legacy fields: END

--STATEMENT
UPDATE COGNOS.slxre_rghqs_targets nologging
SET rghqs_timestamp=SYSDATE
WHERE flowcell_barcode IN (SELECT flowcell_barcode FROM COGNOS.slxre_rghqs_targets_tmp);