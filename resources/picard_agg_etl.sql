--STATEMENT
DELETE FROM COGNOS.SLXRE2_PAGG_SAMPLE  
NOLOGGING WHERE analysis_id NOT IN (SELECT id FROM metrics.aggregation/*DBLINK*/) AND source=/*SOURCE*/
;

--STATEMENT
DELETE FROM COGNOS.SLXRE2_PAGG_LIBRARY 
NOLOGGING WHERE analysis_id NOT IN (SELECT id FROM metrics.aggregation/*DBLINK*/) AND source=/*SOURCE*/
;

--STATEMENT
MERGE INTO COGNOS.slxre2_pagg_sample tr
USING (
WITH

rgan AS
(SELECT
    --PK
    ba.flowcell_barcode,
    ba.lane,
    decode(ba.molecular_barcode_name, 'N/A', 'NULL', ba.molecular_barcode_name)  molecular_barcode_name
    --
FROM metrics.basecalling_analysis/*DBLINK*/ ba
WHERE
    ba.metrics_type IN ('Indexed', 'Unindexed')
    AND greatest(ba.modified_at, ba.workflow_end_date) >= /*DELTA_START*/
    AND greatest(ba.modified_at, ba.workflow_end_date) < /*DELTA_END*/

UNION
SELECT
    --PK
    pa.flowcell_barcode,
    pa.lane,
    decode(pa.molecular_barcode_name, 'N/A', 'NULL', pa.molecular_barcode_name)  molecular_barcode_name
    --
FROM metrics.picard_analysis/*DBLINK*/ pa
WHERE
    pa.metrics_type IN ('Indexed', 'Unindexed')
    AND pa.library_name <> 'Solexa-IC'
    AND greatest(pa.modified_at, pa.workflow_end_date) >= /*DELTA_START*/
    AND greatest(pa.modified_at, pa.workflow_end_date) < /*DELTA_END*/
),

agg AS (
SELECT * FROM metrics.aggregation/*DBLINK*/ a WHERE a.LIBRARY IS NULL
AND a.SAMPLE IN
   (SELECT DISTINCT  a.SAMPLE FROM metrics.aggregation/*DBLINK*/ a
   WHERE a.LIBRARY IS NULL
     AND nvl(a.modified_at, a.workflow_end_date) >= /*DELTA_START*/
     AND nvl(a.modified_at, a.workflow_end_date) < /*DELTA_END*/
   )

UNION

SELECT --PK: agg.id
     DISTINCT  agg.*
FROM rgan
JOIN COGNOS.slxre_readgroup_metadata rgmd ON
        rgmd.flowcell_barcode           = rgan.flowcell_barcode AND
        rgmd.lane                       = rgan.lane             AND
        (rgmd.molecular_indexing_scheme = rgan.molecular_barcode_name
         OR rgmd.is_greedy = 1)

JOIN metrics.aggregation/*DBLINK*/ agg ON
    agg.SAMPLE  = rgmd.collaborator_sample_id AND -- can return more than 1 aggregation per sample, aggregated under different projects; This is OK because we want to update aggregations;
    agg.LIBRARY IS NULL 

WHERE  rgmd.collaborator_sample_id NOT IN ('K-562', 'NA12878')  -- these are some kind of control samples and they are aggregated under many projects

),

target_rg AS (
SELECT
    --PK
    target_agg.project, target_agg.SAMPLE, target_agg.data_type, target_agg.version,
    rg.flowcell_barcode,
    rg.lane,
    rg.molecular_barcode_name,

    --UK: target_agg.id, flowcell, lane, barcode
    target_agg.id,
	rg.library_name

FROM agg target_agg
JOIN metrics.aggregation_read_group/*DBLINK*/ rg ON rg.aggregation_id = target_agg.id
),

align AS (
SELECT al.aggregation_id,
    MAX(DECODE(CATEGORY, 'PAIR', al.total_reads                     )) ap_total_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_reads                        )) ap_pf_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_pf_reads                    )) ap_pct_pf_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_noise_reads                  )) ap_pf_noise_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_reads_aligned                )) ap_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_pf_reads_aligned            )) ap_pct_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_aligned_reads             )) ap_pf_hq_aligned_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_aligned_bases             )) ap_pf_hq_aligned_bases,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_aligned_q20_bases         )) ap_pf_hq_aligned_q20_bases,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_median_mismatches         )) ap_pf_hq_median_mismatches,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_error_rate                )) ap_pf_hq_error_rate,
    MAX(DECODE(CATEGORY, 'PAIR', al.mean_read_length                )) ap_mean_read_length,
    MAX(DECODE(CATEGORY, 'PAIR', al.reads_aligned_in_pairs          )) ap_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_reads_aligned_in_pairs      )) ap_pct_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'PAIR', al.bad_cycles                      )) ap_bad_cycles,
    MAX(DECODE(CATEGORY, 'PAIR', al.strand_balance                  )) ap_pct_strand_balance,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_adapter                     )) ap_pct_adapter,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_aligned_bases                )) ap_pf_aligned_bases,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_mismatch_rate                )) ap_pf_mismatch_rate,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_chimeras                    )) ap_pct_chimeras,
	MAX(DECODE(CATEGORY, 'PAIR', al.pf_indel_rate                    )) ap_pf_indel_rate,

    MAX(DECODE(CATEGORY, 'UNPAIRED', al.total_reads                 )) anp_total_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_reads                    )) anp_pf_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_pf_reads                )) anp_pct_pf_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_noise_reads              )) anp_pf_noise_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_reads_aligned            )) anp_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_pf_reads_aligned        )) anp_pct_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_aligned_reads         )) anp_pf_hq_aligned_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_aligned_bases         )) anp_pf_hq_aligned_bases,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_aligned_q20_bases     )) anp_pf_hq_aligned_q20_bases,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_median_mismatches     )) anp_pf_hq_median_mismatches,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_error_rate            )) anp_pf_hq_error_rate,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.mean_read_length            )) anp_mean_read_length,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.reads_aligned_in_pairs      )) anp_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_reads_aligned_in_pairs  )) anp_pct_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.bad_cycles                  )) anp_bad_cycles,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.strand_balance              )) anp_pct_strand_balance,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_adapter                 )) anp_pct_adapter,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_aligned_bases            )) anp_pf_aligned_bases,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_mismatch_rate            )) anp_pf_mismatch_rate,
	MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_indel_rate               )) anp_pf_indel_rate

-- there are no anp_pct_chimeras because chimerism is when  both ends are in different chromosomes => pct chimeras does not apply to UNPAIRED reads
FROM metrics.aggregation_alignment/*DBLINK*/ al
JOIN agg ON agg.id=al.aggregation_id
WHERE category IN ('PAIR', 'UNPAIRED')

GROUP BY al.aggregation_id
),

ins AS (
SELECT
-- PK: aggregation_id, pair_orientation
    ins.*,
    ROW_NUMBER() over (PARTITION BY ins.aggregation_id ORDER BY ins.read_pairs desc) read_pair_rank
FROM metrics.aggregation_insert_size/*DBLINK*/ ins
JOIN agg ON agg.id=ins.aggregation_id
),

metadata AS (
SELECT
    --PK
    rg.Id sample_agg_id,
    --
    --UK
    rg.project,
    rg.SAMPLE,
    rg.data_type,
    rg.version,
    --
    cognos.concat_string_csv(DISTINCT rgmd.product_order_key      ) product_order_key,
    cognos.concat_string_csv(DISTINCT rgmd.product                ) product,
    cognos.concat_string_csv(DISTINCT rgmd.product_part_number    ) product_part_number,
    cognos.concat_string_csv(DISTINCT rgmd.product_order_sample   ) product_order_sample,
    cognos.concat_string_csv(DISTINCT rgmd.lab_workflow           ) lab_workflow,
    cognos.concat_string_csv(DISTINCT rgmd.analysis_type          ) analysis_type,

    cognos.concat_string_csv(DISTINCT rgmd.lcset                  ) lcset,
    cognos.concat_string_csv(DISTINCT jira.type                   ) lcset_type,
    cognos.concat_string_csv(DISTINCT jira.protocol               ) lcset_protocol,
    cognos.concat_string_csv(DISTINCT jira.seq_technology         ) lcset_seq_technology,
    cognos.concat_string_csv(DISTINCT jira.topoffs                ) lcset_topoff,
	max(jira.created									   ) lcset_creation_max,
    cognos.concat_string_csv(DISTINCT rgmd.research_project_name  ) research_project_name ,
    cognos.concat_string_csv(DISTINCT rgmd.research_project_id    ) research_project_number,
    cognos.concat_string_csv(DISTINCT rgmd.initiative             ) initiative,

    cognos.concat_string_csv(DISTINCT rgmd.sample_id              ) sample_ids,
    cognos.concat_string_csv(DISTINCT rgmd.lsid                   ) lsid,
    'Archived column'                                        root_lsid,
    'Archived column'                                        collection,

    decode(count(DISTINCT rgmd.collaborator_participant_id), 1, max(rgmd.collaborator_participant_id), 'Multiple')  individual_name,
    decode(count(DISTINCT pdos.on_risk), 1, min(pdos.on_risk), 0, 'N/A', 'T')                                       on_risk,  -- if sample is duplacted in PDO at least one of its positions is deemed on risk, then we call it on risk
    cognos.concat_string_csv(DISTINCT pdos.risk_types)                                                                     risk_types,
    COUNT(DISTINCT rg.flowcell_barcode ||'/'||rg.lane)                                                              n_lanes,
    cognos.concat_string_csv(DISTINCT rg.flowcell_barcode ||'/'||rg.lane)                                                  lanes,
	count(DISTINCT rg.flowcell_barcode ||'/'||rg.lane||'/'||rg.library_name)										n_rg,
    sum(rgmd.is_pool_test) 																							n_hiseq_pool_test_lanes,

    cognos.concat_string_csv(DISTINCT rgmd.work_request_id ) wr_id,
    cognos.concat_string_csv(DISTINCT rgmd.gssr_barcodes ) gssr_ids,  -- needed for Seq Only product !!!
    --max(rgmd.work_request_id) keep (dense_rank first ORDER BY rgmd.run_date) over (PARTITION BY agg.id) last_wr_sequenced,
    MAX(r.run_end_date) last_run_end_date,
    
    cognos.concat_string_csv(DISTINCT pdo.title      ) pdo_title,
	cognos.concat_string_csv(DISTINCT sa.o_material_type_name  ) bsp_original_material_type,
	cognos.concat_string_csv(DISTINCT saroot.material_type_name) bsp_root_material_type,
	cognos.concat_string_csv(DISTINCT sa.sample_type  			) bsp_sample_type,
    cognos.concat_string_csv(DISTINCT 
    CASE 
        WHEN instr(nvl(rgmd.setup_read_structure, rgmd.actual_read_structure), '8B8B')>0 THEN 'Dual'
        ELSE 'Single'
    END)       index_type    
	
	


FROM target_rg rg
--metrics.aggregation_read_group/*DBLINK*/ rg
--JOIN agg ON agg.id = rg.aggregation_id --AND agg.is_latest <> 0   --=1

JOIN COGNOS.slxre2_organic_run r ON r.flowcell_barcode = rg.flowcell_barcode --  AND r.is_cancelled = 0  -- commented to include cancelled but aggregated runs

JOIN COGNOS.slxre_readgroup_metadata rgmd ON
    rgmd.flowcell_barcode           = rg.flowcell_barcode AND
    rgmd.lane                       = rg.lane             AND
    (rgmd.molecular_indexing_scheme = NVL(rg.molecular_barcode_name, 'NULL') OR rgmd.is_greedy = 1) 
	
	/* Not needed and RP aggs are done for non Exome
	AND

    CASE WHEN (substr(rg.project, 1, 3)= 'RP-' AND nvl(rgmd.data_type, 'Exome') = rg.data_type )  -- RPT-2197 rgmd.data_type is null for 22 aggregated runs
                OR substr(rg.project, 1, 3)<> 'RP-'
        THEN 1
        ELSE 0
    END =1
	*/
LEFT JOIN mercurydwro.product_order pdo ON pdo.jira_ticket_key = rgmd.product_order_key
LEFT JOIN mercurydwro.product_order_sample pdos ON pdos.product_order_id = pdo.product_order_id AND pdos.sample_name = rgmd.product_order_sample
LEFT JOIN jiradwh.loj_issue_lcset@loj_LINK.seqbldr jira ON jira.KEY = rgmd.lcset

LEFT JOIN analytics.bsp_sample sa ON sa.sample_id=substr(rgmd.product_order_sample, 4)
LEFT JOIN analytics.bsp_sample saroot ON saroot.sample_id=sa.root_sample_id

--LEFT JOIN sample_attribute_bsp@analytics.gap_prod sa ON sa.sample_id=substr(rgmd.product_order_sample, 4)
--LEFT JOIN sample_attribute_bsp@analytics.gap_prod saroot ON saroot.sample_id=sa.root_sample_id

WHERE NVL(rgmd.setup_read_structure, 'NULL') NOT IN ('8B', '8T', '8B8B')
GROUP BY rg.id, rg.project, rg.SAMPLE, rg.data_type, rg.version
),

fcl_isgreedy AS
(SELECT -- PK: flowcell_barcode, lane
    DISTINCT rgmd.flowcell_barcode, rgmd.lane,
    --
    rgmd.is_greedy
FROM COGNOS.slxre_readgroup_Metadata rgmd
JOIN target_rg ON target_rg.flowcell_barcode=rgmd.flowcell_barcode AND target_rg.lane=rgmd.lane
),

bc AS (
SELECT
    --PK
    target_rg.Id sample_agg_id,
    --
    sum(bcm.pf_reads)     bc_pf_reads,
    sum(bcm.total_reads)  bc_total_reads,
    sum(bcm.pf_bases)     bc_pf_bases

FROM target_rg
--metrics.aggregation_read_group/*DBLINK*/ rg
--JOIN agg ON agg.id = rg.aggregation_id AND agg.is_latest<>0  -- =1

JOIN fcl_isgreedy ON
    fcl_isgreedy.flowcell_barcode   = target_rg.flowcell_barcode AND
    fcl_isgreedy.lane               = target_rg.lane

JOIN metrics.basecalling_analysis/*DBLINK*/ ba ON
    ba.flowcell_barcode       = target_rg.flowcell_barcode       AND
    ba.lane                   = target_rg.lane                   AND
    (CASE
        WHEN ba.molecular_barcode_name = NVL(target_rg.molecular_barcode_name, 'N/A') OR fcl_isgreedy.is_greedy =1 THEN 1
        ELSE 0
    END) = 1 AND
    ba.metrics_type IN ('Indexed', 'Unindexed')
JOIN metrics.basecalling_metrics/*DBLINK*/ bcm ON bcm.basecalling_analysis_id = ba.id
GROUP BY target_rg.id
),

fp AS (
SELECT
    --PK
    target_rg.Id sample_agg_id,
    --
    min(fp.lod_expected_sample)  min_lod,
    max(fp.lod_expected_sample)  max_lod

FROM target_rg
--metrics.aggregation_read_group/*DBLINK*/ rg
--JOIN agg ON agg.id = rg.aggregation_id AND agg.is_latest<>0  -- =1

JOIN fcl_isgreedy ON
    fcl_isgreedy.flowcell_barcode   = target_rg.flowcell_barcode AND
    fcl_isgreedy.lane               = target_rg.lane

JOIN metrics.picard_analysis/*DBLINK*/ pa ON
    pa.flowcell_barcode       = target_rg.flowcell_barcode       AND
    pa.lane                   = target_rg.lane                   AND
    (CASE
        WHEN pa.molecular_barcode_name = NVL(target_rg.molecular_barcode_name, 'N/A') OR fcl_isgreedy.is_greedy=1 THEN 1
        ELSE 0
    END) = 1 AND
    pa.metrics_type IN ('Indexed', 'Unindexed')
JOIN metrics.picard_fingerprint/*DBLINK*/ fp ON fp.picard_analysis_id = pa.id
WHERE pa.library_name <> 'Solexa-IC'
GROUP BY target_rg.id
)

SELECT
    -- PK
    agg.project,
    agg.SAMPLE,
    agg.data_type,
	--

    agg.processing_location,
    metadata.product_order_key,
    metadata.product,
    metadata.product_part_number,
    metadata.product_order_sample,
    metadata.research_project_name ,
    metadata.research_project_number,
    metadata.individual_name,


    metadata.on_risk,
    metadata.risk_types,
    metadata.sample_ids ,
    metadata.lsid,
    metadata.lcset,
    metadata.lcset_type,
    metadata.lcset_protocol,
    metadata.lcset_seq_technology,
    metadata.lcset_topoff,
    metadata.lab_workflow workflow,
    metadata.analysis_type,

    agg.id                  analysis_id,
    agg.workflow_start_date analysis_start,
    agg.workflow_end_date   analysis_end,
    agg.version             version,
    metadata.n_lanes        n_lanes,
    metadata.lanes          aggregated_lanes,
    metadata.wr_id,
    metadata.gssr_ids,  -- needed for Seq Only product !!!
    metadata.last_run_end_date,
	metadata.bsp_original_material_type,
	metadata.bsp_root_material_type, 
	

    ---Align metrics from both(Paired and Unpaired) lanes
    NVL(ap_total_reads            ,0)+NVL(anp_total_reads            ,0) al_total_reads,
    NVL(ap_pf_reads               ,0)+NVL(anp_pf_reads               ,0) al_pf_reads,
    NVL(ap_pf_noise_reads         ,0)+NVL(anp_pf_noise_reads         ,0) al_pf_noise_reads,
    NVL(ap_pf_reads_aligned       ,0)+NVL(anp_pf_reads_aligned       ,0) al_pf_reads_aligned,
    NVL(ap_pf_hq_aligned_reads    ,0)+NVL(anp_pf_hq_aligned_reads    ,0) al_pf_hq_aligned_reads,
    NVL(ap_pf_hq_aligned_bases    ,0)+NVL(anp_pf_hq_aligned_bases    ,0) al_pf_hq_aligned_bases,
    NVL(ap_pf_hq_aligned_q20_bases,0)+NVL(anp_pf_hq_aligned_q20_bases,0) al_pf_hq_aligned_q20_bases,
    100*decode(nvl(ap_pf_reads, 0)+nvl(anp_pf_reads, 0), 0, 0,
      (nvl(ap_pf_reads * ap_pct_adapter, 0) + nvl(anp_pf_reads * anp_pct_adapter, 0))/(nvl(ap_pf_reads, 0)+nvl(anp_pf_reads, 0))
    ) al_pct_adapter,
    NVL(ap_pf_aligned_bases       ,0)+NVL(anp_pf_aligned_bases       ,0) al_pf_aligned_bases,
    NVL(ap_pf_mismatch_rate       ,0)+NVL(anp_pf_mismatch_rate       ,0) al_pf_mismatch_rate,
    100*  ap_pct_chimeras                                                      al_pct_chimeras,  -- there are no anp_pct_chimeras because chimerism is when  both ends are in different chromosomes => pct chimeras does not apply to UNPAIRED reads

    ---Align Paired metrics
    al.AP_BAD_CYCLES,
    al.AP_MEAN_READ_LENGTH,
    100*al.AP_PCT_PF_READS                 AP_PCT_PF_READS,
    100*al.AP_PCT_PF_READS_ALIGNED         AP_PCT_PF_READS_ALIGNED,
    100*al.AP_PCT_READS_ALIGNED_IN_PAIRS   AP_PCT_READS_ALIGNED_IN_PAIRS,
    100*al.AP_PCT_STRAND_BALANCE           AP_PCT_STRAND_BALANCE,
    al.AP_PF_HQ_ALIGNED_BASES,
    al.AP_PF_HQ_ALIGNED_Q20_BASES,
    al.AP_PF_HQ_ALIGNED_READS,
    al.AP_PF_HQ_MEDIAN_MISMATCHES,
    al.ap_pf_hq_error_rate,
    al.AP_PF_NOISE_READS,
    al.AP_PF_READS,
    al.AP_PF_READS_ALIGNED,
    al.AP_READS_ALIGNED_IN_PAIRS,
    al.AP_TOTAL_READS,
    al.AP_PF_ALIGNED_BASES,
    al.AP_PF_MISMATCH_RATE,
    100*al.ap_pct_adapter                ap_pct_adapter,

    al.anp_bad_cycles,
    al.anp_mean_read_length,
    100*al.anp_pct_pf_reads              anp_pct_pf_reads,
    100*al.anp_pct_pf_reads_aligned      anp_pct_pf_reads_aligned,
    100*al.anp_pct_reads_aligned_in_pairs  anp_pct_reads_aligned_in_pairs,
    100*al.anp_pct_strand_balance        anp_pct_strand_balance,
    al.anp_pf_hq_aligned_bases,
    al.anp_pf_hq_aligned_q20_bases,
    al.anp_pf_hq_aligned_reads,
    al.anp_pf_hq_median_mismatches,
    al.anp_pf_hq_error_rate,
    al.anp_pf_noise_reads,
    al.anp_pf_reads,
    al.anp_pf_reads_aligned,
    al.anp_reads_aligned_in_pairs,
    al.anp_total_reads,
    al.anp_pf_aligned_bases,
    al.anp_pf_mismatch_rate,
    100*al.anp_pct_adapter       anp_pct_adapter,

----HS Metrics
    hs.PF_READS PF_READS,
    hs.BAIT_DESIGN_EFFICIENCY HS_BAIT_DESIGN_EFFICIENCY,
    hs.BAIT_SET HS_BAIT_SET,
    hs.BAIT_TERRITORY HS_BAIT_TERRITORY,
    hs.FOLD_80_BASE_PENALTY HS_FOLD_80_BASE_PENALTY,
    hs.FOLD_ENRICHMENT HS_FOLD_ENRICHMENT,
    hs.GENOME_SIZE HS_GENOME_SIZE,
    hs.HS_LIBRARY_SIZE,
    hs.MEAN_BAIT_COVERAGE HS_MEAN_BAIT_COVERAGE,
    hs.MEAN_TARGET_COVERAGE HS_MEAN_TARGET_COVERAGE,
    hs.NEAR_BAIT_BASES HS_NEAR_BAIT_BASES,
    hs.OFF_BAIT_BASES HS_OFF_BAIT_BASES,
    hs.ON_BAIT_BASES HS_ON_BAIT_BASES,
    hs.ON_BAIT_VS_SELECTED          HS_ON_BAIT_VS_SELECTED,
    hs.ON_TARGET_BASES              HS_ON_TARGET_BASES,
    100*hs.PCT_OFF_BAIT                 HS_PCT_OFF_BAIT,
    100*hs.PCT_PF_READS                 HS_PCT_PF_READS,
    100*hs.PCT_PF_UQ_READS              HS_PCT_PF_UQ_READS,
    100*hs.PCT_PF_UQ_READS_ALIGNED      HS_PCT_PF_UQ_READS_ALIGNED,
    100*hs.PCT_SELECTED_BASES           HS_PCT_SELECTED_BASES,
    100*hs.PCT_TARGET_BASES_10X         HS_PCT_TARGET_BASES_10X,
    100*hs.PCT_TARGET_BASES_20X         HS_PCT_TARGET_BASES_20X,
    100*hs.PCT_TARGET_BASES_2X          HS_PCT_TARGET_BASES_2X,
    100*hs.PCT_TARGET_BASES_30X         HS_PCT_TARGET_BASES_30X,
    100*hs.pct_target_bases_40x     hs_pct_target_bases_40x,
    100*hs.pct_target_bases_50x     hs_pct_target_bases_50x,
    100*hs.pct_target_bases_100x    hs_pct_target_bases_100x,

    100*hs.PCT_USABLE_BASES_ON_BAIT     HS_PCT_USABLE_BASES_ON_BAIT,
    100*hs.PCT_USABLE_BASES_ON_TARGET   HS_PCT_USABLE_BASES_ON_TARGET,
    hs.PF_UNIQUE_READS              HS_PF_UNIQUE_READS,
    hs.PF_UQ_BASES_ALIGNED          HS_PF_UQ_BASES_ALIGNED,
    hs.PF_UQ_READS_ALIGNED          HS_PF_UQ_READS_ALIGNED,
    hs.TARGET_TERRITORY             HS_TARGET_TERRITORY,
    hs.TOTAL_READS                  HS_TOTAL_READS,
    100*hs.zero_cvg_targets_pct         HS_ZERO_CVG_TARGETS_PCT,
    100*trunc(hs.PCT_TARGET_BASES_20X)  sample_HS_PCT_TARGET_BASES_20X,

---SNP Metrics
    dbsnp.num_in_db_snp SNP_NUM_IN_DBSNP,
    100*dbsnp.PCT_DBSNP     SNP_PCT_DBSNP,
    dbsnp.TOTAL_SNPS    SNP_TOTAL_SNPS,

    metadata.initiative initiative_name,

----  HS Penalty Metrics -- CG added 12/3/2009
    hs.hs_penalty_10x,
    hs.hs_penalty_20x,
    hs.hs_penalty_30x,
    hs.hs_penalty_40x,
    hs.hs_penalty_50x,
    hs.hs_penalty_100x,

    -- RNASeq
    rna.pf_aligned_bases            rna_pf_aligned_bases,
    rna.ribosomal_bases             rna_ribosomal_bases,
    rna.coding_bases                rna_coding_bases,
    rna.utr_bases                   rna_utr_bases,
    rna.intronic_bases              rna_intronic_bases,
    rna.intergenic_bases            rna_intergenic_bases,
    rna.correct_strand_reads        rna_correct_strand_reads,
    rna.incorrect_strand_reads      rna_incorrect_strand_reads,
    100*rna.pct_ribosomal_bases         rna_pct_ribosomal_bases,
    100*rna.pct_coding_bases            rna_pct_coding_bases,
    100*rna.pct_utr_bases               rna_pct_utr_bases,
    100*rna.pct_intronic_bases          rna_pct_intronic_bases,
    100*rna.pct_intergenic_bases        rna_pct_intergenic_bases,
    100*rna.pct_mrna_bases              rna_pct_mrna_bases,
    100*rna.pct_correct_strand_reads    rna_pct_correct_strand_reads,
    rna.pf_bases                    rna_pf_bases,
    100*rna.pct_usable_bases            rna_pct_usable_bases,
    rna.median_cv_coverage          rna_median_cv_coverage,
    rna.median_5prime_bias          rna_median_5prime_bias,
    rna.median_3prime_bias          rna_median_3prime_bias,
    rna.median_5prime_to_3prime_bias  rna_median_5prim_to_3prim_bias,

    --INS
    ins.MEDIAN_INSERT_SIZE  INS_MEDIAN_INSERT_SIZE,
    ins.MIN_INSERT_SIZE     INS_MIN_INSERT_SIZE,
    ins.MAX_INSERT_SIZE     INS_MAX_INSERT_SIZE,
    ins.MEAN_INSERT_SIZE    INS_MEAN_INSERT_SIZE,
    ins.STANDARD_DEVIATION  INS_STANDARD_DEVIATION,
    ins.READ_PAIRS          INS_READ_PAIRS,
    ins.WIDTH_OF_10_PERCENT INS_WIDTH_OF_10_PERCENT,
    ins.WIDTH_OF_20_PERCENT INS_WIDTH_OF_20_PERCENT,
    ins.WIDTH_OF_30_PERCENT INS_WIDTH_OF_30_PERCENT,
    ins.WIDTH_OF_40_PERCENT INS_WIDTH_OF_40_PERCENT,
    ins.WIDTH_OF_50_PERCENT INS_WIDTH_OF_50_PERCENT,
    ins.WIDTH_OF_60_PERCENT INS_WIDTH_OF_60_PERCENT,
    ins.WIDTH_OF_70_PERCENT INS_WIDTH_OF_70_PERCENT,
    ins.WIDTH_OF_80_PERCENT INS_WIDTH_OF_80_PERCENT,
    ins.WIDTH_OF_90_PERCENT INS_WIDTH_OF_90_PERCENT,
    ins.WIDTH_OF_99_PERCENT INS_WIDTH_OF_99_PERCENT,
    ins.pair_orientation    ins_pair_orientation,

-- PCR metrics
    pcr.custom_amplicon_set         pcr_custom_amplicon_set,
    pcr.genome_size                 pcr_genome_size,
    pcr.amplicon_territory          pcr_amplicon_territory,
    pcr.target_territory            pcr_target_territory,
    pcr.total_reads                 pcr_total_reads,
    pcr.pf_reads                    pcr_pf_reads,
    pcr.pf_unique_reads             pcr_pf_unique_reads,
    100*pcr.pct_pf_reads                pcr_pct_pf_reads,
    100*pcr.pct_pf_uq_reads             pcr_pct_pf_uq_reads,
    pcr.pf_uq_reads_aligned         pcr_pf_uq_reads_aligned,
    100*pcr.pct_pf_uq_reads_aligned     pcr_pct_pf_uq_reads_aligned,
    pcr.pf_uq_bases_aligned         pcr_pf_uq_bases_aligned,
    pcr.on_amplicon_bases           pcr_on_amplicon_bases,
    pcr.near_amplicon_bases         pcr_near_amplicon_bases,
    pcr.off_amplicon_bases          pcr_off_amplicon_bases,
    pcr.on_target_bases             pcr_on_target_bases,
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
    100*c.PCT_CONTAMINATION             PCT_CONTAMINATION,

-- RRBS metrics -- from new schema
    rrbs_agg.reads_aligned                  rrbs_reads_aligned,
    rrbs_agg.non_cpg_bases                  rrbs_non_cpg_bases,
    rrbs_agg.non_cpg_converted_bases        rrbs_non_cpg_converted_bases,
    100*rrbs_agg.pct_non_cpg_bases_converted    rrbs_pct_noncpg_bases_convertd,
    rrbs_agg.cpg_bases_seen                 rrbs_cpg_bases_seen,
    rrbs_agg.cpg_bases_converted            rrbs_cpg_bases_converted,
    100*rrbs_agg.pct_cpg_bases_converted        rrbs_pct_cpg_bases_converted,
    rrbs_agg.mean_cpg_coverage              rrbs_mean_cpg_coverage,
    rrbs_agg.median_cpg_coverage            rrbs_median_cpg_coverage,
    rrbs_agg.reads_with_no_cpg              rrbs_reads_with_no_cpg,
    rrbs_agg.reads_ignored_short            rrbs_reads_ignored_short,
    rrbs_agg.reads_ignored_mismatches       rrbs_reads_ignored_mismatches,

    bc_pf_reads,
    bc_total_reads,
    bc_pf_bases,

    -- WGS
    wgs.genome_territory        wgs_genome_territory,
    wgs.mean_coverage           wgs_mean_coverage ,
    wgs.sd_coverage             wgs_sd_coverage,
    wgs.median_coverage         wgs_median_coverage,
    wgs.mad_coverage            wgs_mad_coverage,
    100*wgs.pct_exc_mapq        wgs_pct_exc_mapq ,
    100*wgs.pct_exc_dupe        wgs_pct_exc_dupe,
    100*wgs.pct_exc_unpaired    wgs_pct_exc_unpaired   ,
    100*wgs.pct_exc_baseq       wgs_pct_exc_baseq ,
    100*wgs.pct_exc_overlap     wgs_pct_exc_overlap,
    100*wgs.pct_exc_capped      wgs_pct_exc_capped ,
    100*wgs.pct_exc_total       wgs_pct_exc_total,
    100*wgs.pct_5x              wgs_pct_5x,
    100*wgs.pct_10x             wgs_pct_10x,
    100*wgs.pct_20x             wgs_pct_20x,
    100*wgs.pct_30x             wgs_pct_30x,
    100*wgs.pct_40x             wgs_pct_40x,
    100*wgs.pct_50x             wgs_pct_50x,
    100*wgs.pct_60x             wgs_pct_60x,
    100*wgs.pct_70x             wgs_pct_70x,
    100*wgs.pct_80x             wgs_pct_80x,
    100*wgs.pct_90x             wgs_pct_90x,
    100*wgs.pct_100x            wgs_pct_100x,
    100*wgs.pct_15x             wgs_pct_15x,
    100*wgs.pct_25x             wgs_pct_25x,

    -- Raw WGS
    rwgs.genome_territory        rwgs_genome_territory,
    rwgs.mean_coverage           rwgs_mean_coverage ,
    rwgs.sd_coverage             rwgs_sd_coverage,
    rwgs.median_coverage         rwgs_median_coverage,
    rwgs.mad_coverage            rwgs_mad_coverage,
    100*rwgs.pct_exc_mapq        rwgs_pct_exc_mapq ,
    100*rwgs.pct_exc_dupe        rwgs_pct_exc_dupe,
    100*rwgs.pct_exc_unpaired    rwgs_pct_exc_unpaired   ,
    100*rwgs.pct_exc_baseq       rwgs_pct_exc_baseq ,
    100*rwgs.pct_exc_overlap     rwgs_pct_exc_overlap,
    100*rwgs.pct_exc_capped      rwgs_pct_exc_capped ,
    100*rwgs.pct_exc_total       rwgs_pct_exc_total,
    100*rwgs.pct_5x              rwgs_pct_5x,
    100*rwgs.pct_10x             rwgs_pct_10x,
    100*rwgs.pct_20x             rwgs_pct_20x,
    100*rwgs.pct_30x             rwgs_pct_30x,
    100*rwgs.pct_40x             rwgs_pct_40x,
    100*rwgs.pct_50x             rwgs_pct_50x,
    100*rwgs.pct_60x             rwgs_pct_60x,
    100*rwgs.pct_70x             rwgs_pct_70x,
    100*rwgs.pct_80x             rwgs_pct_80x,
    100*rwgs.pct_90x             rwgs_pct_90x,
    100*rwgs.pct_100x            rwgs_pct_100x,
    100*rwgs.pct_15x             rwgs_pct_15x,
    100*rwgs.pct_25x             rwgs_pct_25x,

    fp.min_lod,
    fp.max_lod,
    metadata.pdo_title,    
    ap_pf_indel_rate,
    anp_pf_indel_rate,
	metadata.n_hiseq_pool_test_lanes,
	metadata.n_rg n_aggregated_rg,
	
	100*wgs.pct_1x				wgs_pct_1x,
	100*wgs.het_snp_sensitivity 	wgs_het_snp_sensitivity,
	wgs.het_snp_q				wgs_het_snp_q,
	metadata.bsp_sample_type	sample_type,
	metadata.index_type,
	safp.lod_expected_sample sample_lod,
	hs.at_dropout hs_at_dropout,
	metadata.lcset_creation_max,
	
    agg.is_latest

FROM agg
JOIN metrics.aggregation_hybrid_selection/*DBLINK*/ hs   ON       hs.aggregation_id = agg.id
JOIN align al                                  ON       al.aggregation_id = agg.id
JOIN metrics.aggregation_dbsnp/*DBLINK*/ dbsnp           ON    dbsnp.aggregation_id = agg.id
JOIN ins                                       ON      ins.aggregation_id = agg.id AND read_pair_rank = 1
JOIN metrics.aggregation_rna_seq/*DBLINK*/ rna           ON      rna.aggregation_id = agg.id
JOIN metrics.aggregation_pcr/*DBLINK*/ pcr               ON      pcr.aggregation_id = agg.id
LEFT JOIN metrics.aggregation_rrbs_summary/*DBLINK*/ rrbs_agg ON rrbs_agg.aggregation_id = agg.id
JOIN metrics.aggregation_contam/*DBLINK*/ c              ON        c.aggregation_id = agg.id
LEFT JOIN metrics.aggregation_oxog/*DBLINK*/ oxog        ON     oxog.aggregation_id = agg.id AND oxog.CONTEXT ='CCG'  -- not all aggregations have rows context='CCG'

LEFT JOIN metadata                             ON metadata.sample_agg_id  = agg.id
LEFT JOIN bc                                   ON       bc.sample_agg_id  = agg.id
LEFT JOIN fp                                   ON       fp.sample_agg_id  = agg.id
JOIN metrics.aggregation_wgs/*DBLINK*/ wgs               ON      wgs.aggregation_id = agg.id
LEFT JOIN metrics.aggregation_raw_wgs/*DBLINK*/ rwgs     ON     rwgs.aggregation_id = agg.id
LEFT JOIN metrics.aggregation_fingerprint safp           ON safp.aggregation_id = agg.id 
) DELTA

ON (tr.analysis_id=DELTA.analysis_id AND tr.source=/*SOURCE*/)

WHEN NOT MATCHED THEN
INSERT values(
-- PK
delta.project, delta.sample, delta.data_type,
--
sysdate, --timestamp

delta.product_order_key, delta.product, delta.product_part_number,
delta.product_order_sample, delta.research_project_name, delta.research_project_number,
delta.gssr_ids,
delta.last_run_end_date,

delta.individual_name,
delta.on_risk, delta.risk_types, delta.sample_ids, delta.lsid,
delta.lcset, delta.workflow,
delta.analysis_type,
delta.analysis_id,
delta.analysis_start, delta.analysis_end, delta.version, delta.n_lanes, delta.aggregated_lanes, delta.wr_id,

delta.al_total_reads, delta.al_pf_reads, delta.al_pf_noise_reads, delta.al_pf_reads_aligned,
delta.al_pf_hq_aligned_reads, delta.al_pf_hq_aligned_bases, delta.al_pf_hq_aligned_q20_bases,
delta.al_pf_aligned_bases, delta.al_pf_mismatch_rate,
delta.al_pct_chimeras,
delta.al_pct_adapter,
delta.ap_bad_cycles, delta.ap_mean_read_length,
delta.ap_pct_pf_reads, delta.ap_pct_pf_reads_aligned, delta.ap_pct_reads_aligned_in_pairs,
delta.ap_pct_strand_balance, delta.ap_pf_hq_aligned_bases, delta.ap_pf_hq_aligned_q20_bases,
delta.ap_pf_hq_aligned_reads, delta.ap_pf_hq_median_mismatches, delta.ap_pf_hq_error_rate,
delta.ap_pf_noise_reads, delta.ap_pf_reads, delta.ap_pf_reads_aligned, delta.ap_reads_aligned_in_pairs,
delta.ap_total_reads, delta.ap_pf_aligned_bases, delta.ap_pf_mismatch_rate,
delta.ap_pct_adapter,
delta.anp_bad_cycles,
delta.anp_mean_read_length, delta.anp_pct_pf_reads, delta.anp_pct_pf_reads_aligned, delta.anp_pct_reads_aligned_in_pairs,
delta.anp_pct_strand_balance, delta.anp_pf_hq_aligned_bases, delta.anp_pf_hq_aligned_q20_bases,
delta.anp_pf_hq_aligned_reads, delta.anp_pf_hq_median_mismatches, delta.anp_pf_hq_error_rate,
delta.anp_pf_noise_reads, delta.anp_pf_reads, delta.anp_pf_reads_aligned, delta.anp_reads_aligned_in_pairs,
delta.anp_total_reads, delta.anp_pf_aligned_bases, delta.anp_pf_mismatch_rate,
delta.anp_pct_adapter,
delta.pf_reads,
delta.hs_bait_design_efficiency, delta.hs_bait_set, delta.hs_bait_territory, delta.hs_fold_80_base_penalty,
delta.hs_fold_enrichment, delta.hs_genome_size, delta.hs_library_size, delta.hs_mean_bait_coverage,
delta.hs_mean_target_coverage, delta.hs_near_bait_bases, delta.hs_off_bait_bases, delta.hs_on_bait_bases,
delta.hs_on_bait_vs_selected, delta.hs_on_target_bases, delta.hs_pct_off_bait, delta.hs_pct_pf_reads,
delta.hs_pct_pf_uq_reads, delta.hs_pct_pf_uq_reads_aligned, delta.hs_pct_selected_bases,
delta.hs_pct_target_bases_10x, delta.hs_pct_target_bases_20x, delta.hs_pct_target_bases_2x,
delta.hs_pct_target_bases_30x,
delta.hs_pct_target_bases_40x,
delta.hs_pct_target_bases_50x,
delta.hs_pct_target_bases_100x,
delta.hs_pct_usable_bases_on_bait, delta.hs_pct_usable_bases_on_target,
delta.hs_pf_unique_reads, delta.hs_pf_uq_bases_aligned, delta.hs_pf_uq_reads_aligned, delta.hs_target_territory,
delta.hs_total_reads, delta.hs_zero_cvg_targets_pct, delta.sample_hs_pct_target_bases_20x,
delta.snp_num_in_dbsnp, delta.snp_pct_dbsnp, delta.snp_total_snps,
delta.initiative_name,

delta.hs_penalty_10x, delta.hs_penalty_20x,
delta.hs_penalty_30x,
delta.hs_penalty_40x,
delta.hs_penalty_50x,
delta.hs_penalty_100x,
delta.rna_pf_aligned_bases, delta.rna_ribosomal_bases, delta.rna_coding_bases,
delta.rna_utr_bases, delta.rna_intronic_bases, delta.rna_intergenic_bases, delta.rna_correct_strand_reads,
delta.rna_incorrect_strand_reads, delta.rna_pct_ribosomal_bases, delta.rna_pct_coding_bases,
delta.rna_pct_utr_bases, delta.rna_pct_intronic_bases, delta.rna_pct_intergenic_bases, delta.rna_pct_mrna_bases,
delta.rna_pct_correct_strand_reads, delta.rna_pf_bases, delta.rna_pct_usable_bases, delta.rna_median_cv_coverage,
delta.rna_median_5prime_bias, delta.rna_median_3prime_bias, delta.rna_median_5prim_to_3prim_bias,
delta.ins_median_insert_size, delta.ins_min_insert_size, delta.ins_max_insert_size, delta.ins_mean_insert_size,
delta.ins_standard_deviation, delta.ins_read_pairs, delta.ins_width_of_10_percent, delta.ins_width_of_20_percent,
delta.ins_width_of_30_percent, delta.ins_width_of_40_percent, delta.ins_width_of_50_percent,
delta.ins_width_of_60_percent, delta.ins_width_of_70_percent, delta.ins_width_of_80_percent,
delta.ins_width_of_90_percent, delta.ins_width_of_99_percent, delta.ins_pair_orientation,
delta.pcr_custom_amplicon_set, delta.pcr_genome_size, delta.pcr_amplicon_territory, delta.pcr_target_territory,
delta.pcr_total_reads, delta.pcr_pf_reads, delta.pcr_pf_unique_reads, delta.pcr_pct_pf_reads, delta.pcr_pct_pf_uq_reads,
delta.pcr_pf_uq_reads_aligned, delta.pcr_pct_pf_uq_reads_aligned, delta.pcr_pf_uq_bases_aligned,
delta.pcr_on_amplicon_bases, delta.pcr_near_amplicon_bases, delta.pcr_off_amplicon_bases,
delta.pcr_on_target_bases, delta.pcr_pct_amplified_bases, delta.pcr_pct_off_amplicon, delta.pcr_on_amplicon_vs_selected,
delta.pcr_mean_amplicon_coverage, delta.pcr_mean_target_coverage, delta.pcr_fold_enrichment,
delta.pcr_zero_cvg_targets_pct, delta.pcr_fold_80_base_penalty, delta.pcr_pct_target_bases_2x,
delta.pcr_pct_target_bases_10x, delta.pcr_pct_target_bases_20x, delta.pcr_pct_target_bases_30x,
delta.pcr_at_dropout, delta.pcr_gc_dropout, delta.pct_contamination, delta.rrbs_reads_aligned,
delta.rrbs_non_cpg_bases, delta.rrbs_non_cpg_converted_bases, delta.rrbs_pct_noncpg_bases_convertd,
delta.rrbs_cpg_bases_seen, delta.rrbs_cpg_bases_converted, delta.rrbs_pct_cpg_bases_converted,
delta.rrbs_mean_cpg_coverage, delta.rrbs_median_cpg_coverage, delta.rrbs_reads_with_no_cpg,
delta.rrbs_reads_ignored_short, delta.rrbs_reads_ignored_mismatches,
delta.bc_pf_reads,
delta.bc_total_reads,
delta.bc_pf_bases,
delta.wgs_genome_territory,
delta.wgs_mean_coverage ,
delta.wgs_sd_coverage,
delta.wgs_median_coverage,
delta.wgs_mad_coverage,
delta.wgs_pct_exc_mapq ,
delta.wgs_pct_exc_dupe,
delta.wgs_pct_exc_unpaired   ,
delta.wgs_pct_exc_baseq ,
delta.wgs_pct_exc_overlap,
delta.wgs_pct_exc_capped ,
delta.wgs_pct_exc_total,
delta.wgs_pct_5x,
delta.wgs_pct_10x,
delta.wgs_pct_20x,
delta.wgs_pct_30x,
delta.wgs_pct_40x,
delta.wgs_pct_50x,
delta.wgs_pct_60x,
delta.wgs_pct_70x,
delta.wgs_pct_80x,
delta.wgs_pct_90x,
delta.wgs_pct_100x,
delta.min_lod,
delta.max_lod,
delta.lcset_type,
delta.wgs_pct_15x,
delta.wgs_pct_25x,

delta.rwgs_genome_territory,
delta.rwgs_mean_coverage ,
delta.rwgs_sd_coverage,
delta.rwgs_median_coverage,
delta.rwgs_mad_coverage,
delta.rwgs_pct_exc_mapq ,
delta.rwgs_pct_exc_dupe,
delta.rwgs_pct_exc_unpaired   ,
delta.rwgs_pct_exc_baseq ,
delta.rwgs_pct_exc_overlap,
delta.rwgs_pct_exc_capped ,
delta.rwgs_pct_exc_total,
delta.rwgs_pct_5x,
delta.rwgs_pct_10x,
delta.rwgs_pct_20x,
delta.rwgs_pct_30x,
delta.rwgs_pct_40x,
delta.rwgs_pct_50x,
delta.rwgs_pct_60x,
delta.rwgs_pct_70x,
delta.rwgs_pct_80x,
delta.rwgs_pct_90x,
delta.rwgs_pct_100x,
delta.rwgs_pct_15x,
delta.rwgs_pct_25x,
delta.lcset_protocol,
delta.lcset_seq_technology,
delta.lcset_topoff,

/*SOURCE*/
, DELTA.pdo_title,
DELTA.ap_pf_indel_rate,
DELTA.anp_pf_indel_rate,
DELTA.bsp_original_material_type,
DELTA.bsp_root_material_type,
DELTA.n_hiseq_pool_test_lanes,
DELTA.n_aggregated_rg,
DELTA.wgs_pct_1x,
DELTA.wgs_het_snp_sensitivity,
DELTA.wgs_het_snp_q,
DELTA.processing_location ,
DELTA.sample_type,
DELTA.index_type,
DELTA.sample_lod,
DELTA.hs_at_dropout,
delta.lcset_creation_max


)
WHERE DELTA.is_latest<>0  --=1

WHEN MATCHED THEN
UPDATE
SET tr.timestamp=SYSDATE,
    tr.product_order_key       		= decode(delta.is_latest, 0, 'DELETE', delta.product_order_key),
    tr.pdo_title               		= delta.pdo_title,
    tr.product                 		= delta.product,
    tr.product_part_number     		= delta.product_part_number,
    tr.product_order_sample    		= delta.product_order_sample,
    tr.research_project_name   		= delta.research_project_name,
    tr.research_project_number 		= delta.research_project_number,
    tr.individual_name         		= delta.individual_name,
    tr.on_risk                 		= delta.on_risk,
    tr.risk_types              		= delta.risk_types,
    tr.sample_ids              		= delta.sample_ids,
	tr.bsp_original_material_type 	= delta.bsp_original_material_type,
	tr.bsp_root_material_type		= delta.bsp_root_material_type,
	tr.sample_type					= delta.sample_type,
	tr.index_type					= delta.index_type,

    tr.lsid                    		= delta.lsid,
    tr.lcset                   		= delta.lcset,
    tr.lcset_type              		= delta.lcset_type,
    tr.lcset_protocol          		= delta.lcset_protocol,
    tr.lcset_seq_technology    		= delta.lcset_seq_technology,
    tr.lcset_topoff            		= delta.lcset_topoff,
	tr.lcset_creation_max			= delta.lcset_creation_max,
    tr.workflow                		= delta.workflow,
    tr.analysis_type           		= delta.analysis_type,
    tr.n_lanes                 		= delta.n_lanes,
	tr.n_aggregated_rg				= delta.n_aggregated_rg,
    tr.aggregated_lanes        		= delta.aggregated_lanes,
    tr.n_hiseq_pool_test_lanes      = delta.n_hiseq_pool_test_lanes,
    tr.wr_id                   		= delta.wr_id,
    tr.initiative_name         		= delta.initiative_name,
    tr.min_lod                 		= delta.min_lod,
    tr.max_lod                 		= delta.max_lod,

    tr.gssr_id                 		= delta.gssr_ids,
    tr.last_run_end_date       		= delta.last_run_end_date,
	tr.processing_location			= delta.processing_location
DELETE WHERE tr.product_order_key = 'DELETE'
;

--STATEMENT
MERGE INTO COGNOS.slxre2_pagg_library tr
USING (
WITH

rgan AS
(SELECT
    --PK
    ba.flowcell_barcode,
    ba.lane,
    decode(ba.molecular_barcode_name, 'N/A', 'NULL', ba.molecular_barcode_name)  molecular_barcode_name
    --
FROM metrics.basecalling_analysis/*DBLINK*/ ba
WHERE
    ba.metrics_type IN ('Indexed', 'Unindexed')
    AND greatest(ba.modified_at, ba.workflow_end_date) >= /*DELTA_START*/
    AND greatest(ba.modified_at, ba.workflow_end_date) < /*DELTA_END*/

UNION
SELECT
    --PK
    pa.flowcell_barcode,
    pa.lane,
    decode(pa.molecular_barcode_name, 'N/A', 'NULL', pa.molecular_barcode_name)  molecular_barcode_name
    --
FROM metrics.picard_analysis/*DBLINK*/ pa
WHERE
    pa.metrics_type IN ('Indexed', 'Unindexed')
    AND pa.library_name <> 'Solexa-IC'
    AND greatest(pa.modified_at, pa.workflow_end_date) >= /*DELTA_START*/
    AND greatest(pa.modified_at, pa.workflow_end_date) < /*DELTA_END*/
),

agg AS (
SELECT * FROM metrics.aggregation/*DBLINK*/ a WHERE a.LIBRARY IS NOT NULL
AND (a.SAMPLE) IN
  (SELECT a.SAMPLE FROM metrics.aggregation/*DBLINK*/ a
  WHERE a.LIBRARY IS NOT NULL
     AND nvl(a.modified_at, a.workflow_end_date) >= /*DELTA_START*/
     AND nvl(a.modified_at, a.workflow_end_date) < /*DELTA_END*/
  )
UNION

SELECT --PK: agg.id
     DISTINCT  agg.*
FROM rgan
JOIN COGNOS.slxre_readgroup_metadata rgmd ON
        rgmd.flowcell_barcode           = rgan.flowcell_barcode AND
        rgmd.lane                       = rgan.lane             AND
        (rgmd.molecular_indexing_scheme = rgan.molecular_barcode_name
         OR rgmd.is_greedy = 1)

JOIN metrics.aggregation/*DBLINK*/ agg ON
    agg.SAMPLE  = rgmd.collaborator_sample_id AND -- can return more than 1 aggregation per sample, aggregated under different projects; This is OK because we want to update aggregations;
    agg.LIBRARY IS NOT NULL

WHERE  rgmd.collaborator_sample_id NOT IN ('K-562', 'NA12878')  -- these are some kind of control samples and they are aggregated under many projects
),

lagg_rg AS (
SELECT
    --PK
    agg.project, agg.SAMPLE, agg.LIBRARY, agg.data_type, agg.version, agg.processing_location,
    rg.flowcell_barcode, rg.lane, rg.molecular_barcode_name,
    --
    agg.id lagg_analysis_id,
    rg.aggregation_id sagg_analysis_id

FROM metrics.aggregation_read_group/*DBLINK*/ rg
JOIN metrics.aggregation/*DBLINK*/  a  ON a.id = rg.aggregation_id --AND a.is_latest<>0  -- =1

JOIN agg ON
    agg.project = a.project AND
    agg.SAMPLE = a.SAMPLE AND
    agg.LIBRARY = rg.library_name AND
    agg.data_type = a.data_type AND
    agg.version = a.version AND 
    agg.processing_location = a.processing_location
),

metadata AS (
SELECT
    --PK
    rg.project,
    rg.SAMPLE,
    rg.LIBRARY,
    rg.data_type,
    rg.version,
    rg.processing_location,
--
    cognos.concat_string_csv(DISTINCT rgmd.initiative                  ) initiative,
    cognos.concat_string_csv(DISTINCT rgmd.lab_workflow                ) lab_workflow,
    cognos.concat_string_csv(DISTINCT rgmd.analysis_type               ) analysis_type,
    cognos.concat_string_csv(DISTINCT rgmd.product_order_key           ) product_order_key,
    cognos.concat_string_csv(DISTINCT rgmd.product                     ) product,
    cognos.concat_string_csv(DISTINCT rgmd.product_part_number         ) product_part_number,
    cognos.concat_string_csv(DISTINCT rgmd.product_order_sample        ) product_order_sample,
    cognos.concat_string_csv(DISTINCT rgmd.research_project_name       ) research_project_name ,
    cognos.concat_string_csv(DISTINCT rgmd.research_project_id         ) research_project_number,


    cognos.concat_string_csv(DISTINCT rgmd.gssr_barcodes               ) gssr_id,
    cognos.concat_string_csv(DISTINCT rgmd.sample_id                   ) sample_id,
    cognos.concat_string_csv(DISTINCT rgmd.organism_scientific_name    ) organism_scientific_name,
    cognos.concat_string_csv(DISTINCT rgmd.molecular_indexing_Scheme   ) molecular_barcode_name,
    cognos.concat_string_csv(DISTINCT rgmd.work_request_type           ) wr_type,
    cognos.concat_string_csv(DISTINCT rgmd.work_request_domain         ) wr_domain,

    cognos.concat_string_csv(DISTINCT COALESCE(ancestry.pond , ancestry.pcr_free_pond, ancestry.pcr_plus_pond, ancestry.nextera)) pond,
    cognos.concat_string_csv(DISTINCT COALESCE(ancestry.catch , ancestry.nexome_catch                                          )) catch,

    cognos.concat_string_csv(DISTINCT rgmd.lcset                       ) lcset,
    cognos.concat_string_csv(DISTINCT jira.type                        ) lcset_type,
    cognos.concat_string_csv(DISTINCT jira.protocol                    ) lcset_protocol,
    cognos.concat_string_csv(DISTINCT jira.seq_technology              ) lcset_seq_technology,
    cognos.concat_string_csv(DISTINCT jira.topoffs                     ) lcset_topoff,

    cognos.concat_string_csv(DISTINCT rg.flowcell_barcode||'/'||rg.lane) lanes,

    COUNT(DISTINCT rg.flowcell_barcode||'/'||rg.lane)                                     n_lanes,
    COUNT(DISTINCT rg.flowcell_barcode||'/'||rg.lane||'/'||rg.molecular_barcode_name)     n_readgroups,
    sum(rgmd.is_pool_test) 															  n_hiseq_pool_test_lanes,

    min(rgmd.library_creation_date)                                                       library_creation_date,
    decode(count(DISTINCT pdos.on_risk), 1, min(pdos.on_risk), 0, 'N/A', 'T') on_risk,  -- if sample is duplacted in PDO at least one of its positions is deemed on risk, then we call it on risk
    cognos.concat_string_csv(DISTINCT pdos.risk_types                              ) risk_types,  -- use for on_risk reason
    max(ru.run_end_date) last_run_end_date,
    cognos.concat_string_csv(DISTINCT pdo.title                                    ) pdo_title,
	
	cognos.concat_string_csv(DISTINCT sa.o_material_type_name  ) bsp_original_material_type,
	cognos.concat_string_csv(DISTINCT saroot.material_type_name) bsp_root_material_type,
	cognos.concat_string_csv(DISTINCT sa.sample_type  			) bsp_sample_type,
    cognos.concat_string_csv(DISTINCT 

	CASE 
		WHEN instr(rgmd.setup_read_structure, 'B', 1, 2)>0 THEN 'Dual'
		WHEN instr(rgmd.setup_read_structure, 'B', 1, 2)=0 AND instr(rgmd.setup_read_structure, 'B', 1, 1)>0  THEN 'Single'
		ELSE 'Unknown'
	END) index_type

--    CASE 
--        WHEN instr(nvl(rgmd.setup_read_structure, rgmd.actual_read_structure), '8B8B')>0 THEN 'Dual'
--        ELSE 'Single'
--    END)       index_type    
	


FROM  lagg_rg rg

JOIN COGNOS.slxre_readgroup_metadata rgmd ON
    rgmd.flowcell_barcode          = rg.flowcell_barcode AND
    rgmd.lane                      = rg.lane AND
    (CASE
        WHEN rgmd.molecular_indexing_scheme = nvl(rg.molecular_barcode_name,'NULL') OR rgmd.is_greedy = 1 THEN 1
        ELSE 0
    END) = 1 
	
	/* Not needed and RP aggs are done for non Exome
	AND

    CASE WHEN (substr(rg.project,1, 3)= 'RP-' AND nvl(rgmd.data_type, 'Exome') = rg.data_type)  -- some runs do not have rgmd.data_type populated
            OR  (substr(rg.project,1, 3)= 'RP-' AND rg.data_type= 'RNA')  -- special case agg for RP-265
            OR (substr(rg.project,1, 3)<> 'RP-' AND rg.data_type ='N/A')
         THEN 1
         ELSE 0
    END =1
	*/

-- (pdo.jira_ticket_key, pdos.sample_name) is not unique because a PDO sample can occur on multiple positions in PDO
LEFT JOIN mercurydwro.product_order pdo ON pdo.jira_ticket_key = rgmd.product_order_key
LEFT JOIN mercurydwro.product_order_sample pdos ON pdos.product_order_id = pdo.product_order_id AND pdos.sample_name = rgmd.product_order_sample

LEFT JOIN COGNOS.slxre2_rg_ancestry ancestry ON
    ancestry.flowcell_barcode           = rgmd.flowcell_barcode AND
    ancestry.lane                       = rgmd.lane             AND
    ancestry.molecular_indexing_scheme  = rgmd.molecular_indexing_scheme    -- ancestry.molecular_indexing_Scheme comes from RGMD

JOIN COGNOS.slxre2_organic_run ru ON ru.run_name=rgmd.run_name --AND ru.is_cancelled = 0
LEFT JOIN jiradwh.loj_issue_lcset@loj_LINK.seqbldr jira ON jira.KEY = rgmd.lcset

LEFT JOIN analytics.bsp_sample sa ON sa.sample_id=substr(rgmd.product_order_sample, 4)
LEFT JOIN analytics.bsp_sample saroot ON saroot.sample_id=sa.root_sample_id

--LEFT JOIN sample_attribute_bsp@analytics.gap_prod sa ON sa.sample_id=substr(rgmd.product_order_sample, 4)
--LEFT JOIN sample_attribute_bsp@analytics.gap_prod saroot ON saroot.sample_id=sa.root_sample_id

GROUP BY rg.project, rg.SAMPLE, rg.LIBRARY, rg.data_type, rg.version,    rg.processing_location
),

fcl_isgreedy AS (
SELECT -- PK: flowcell_barcode, lane
    DISTINCT rgmd.flowcell_barcode, rgmd.lane,
    --
    rgmd.is_greedy
FROM COGNOS.slxre_readgroup_Metadata rgmd
JOIN lagg_rg ON lagg_rg.flowcell_barcode=rgmd.flowcell_barcode AND lagg_rg.lane=rgmd.lane
),

agg_bc AS (
SELECT
    --PK
    rg.project,
    rg.SAMPLE,
    rg.LIBRARY ,
    rg.data_type,
    rg.version,
	rg.processing_location,
    --
   sum(bcm.pf_bases) pf_bases
FROM lagg_rg rg

JOIN fcl_isgreedy ON
    fcl_isgreedy.flowcell_barcode   = rg.flowcell_barcode AND
    fcl_isgreedy.lane               = rg.lane

JOIN metrics.basecalling_analysis/*DBLINK*/ ba ON
    ba.flowcell_barcode       = rg.flowcell_barcode AND
    ba.lane                   = rg.lane AND

    (CASE
        WHEN ba.molecular_barcode_name = nvl(rg.molecular_barcode_name, 'N/A') OR fcl_isgreedy.is_greedy =1 THEN 1
        ELSE 0
    END) =1  AND
    ba.metrics_type IN ('Indexed', 'Unindexed')

JOIN metrics.basecalling_metrics/*DBLINK*/ bcm ON bcm.basecalling_analysis_id = ba.id

GROUP BY rg.project, rg.SAMPLE, rg.library, rg.data_type, rg.version, rg.processing_location
),

agg_fp AS (
SELECT
    --PK
    rg.project,
    rg.SAMPLE,
    rg.LIBRARY ,
    rg.data_type,
    rg.version,
    rg.processing_location,
--
   min(fp.lod_expected_sample) min_lod,
   max(fp.lod_expected_sample) max_lod
FROM lagg_rg rg

JOIN fcl_isgreedy ON
    fcl_isgreedy.flowcell_barcode   = rg.flowcell_barcode AND
    fcl_isgreedy.lane               = rg.lane

JOIN metrics.picard_analysis/*DBLINK*/ pa ON
    pa.flowcell_barcode       = rg.flowcell_barcode AND
    pa.lane                   = rg.lane AND
    (CASE
        WHEN pa.molecular_barcode_name = nvl(rg.molecular_barcode_name, 'N/A') OR fcl_isgreedy.is_greedy =1 THEN 1
        ELSE 0
    END) =1 AND

    pa.metrics_type IN ('Indexed', 'Unindexed')

JOIN metrics.picard_fingerprint/*DBLINK*/ fp ON fp.picard_analysis_id = pa.id
WHERE pa.library_name <> 'Solexa-IC'
GROUP BY rg.project, rg.SAMPLE, rg.library, rg.data_type, rg.version,    rg.processing_location
),

align AS (
SELECT al.aggregation_id,
    MAX(DECODE(CATEGORY, 'PAIR', al.total_reads                     )) ap_total_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_reads                        )) ap_pf_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_pf_reads                    )) ap_pct_pf_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_noise_reads                  )) ap_pf_noise_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_reads_aligned                )) ap_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_pf_reads_aligned            )) ap_pct_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_aligned_reads             )) ap_pf_hq_aligned_reads,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_aligned_bases             )) ap_pf_hq_aligned_bases,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_aligned_q20_bases         )) ap_pf_hq_aligned_q20_bases,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_median_mismatches         )) ap_pf_hq_median_mismatches,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_hq_error_rate                )) ap_pf_hq_error_rate,
    MAX(DECODE(CATEGORY, 'PAIR', al.mean_read_length                )) ap_mean_read_length,
    MAX(DECODE(CATEGORY, 'PAIR', al.reads_aligned_in_pairs          )) ap_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_reads_aligned_in_pairs      )) ap_pct_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'PAIR', al.bad_cycles                      )) ap_bad_cycles,
    MAX(DECODE(CATEGORY, 'PAIR', al.strand_balance                  )) ap_pct_strand_balance,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_adapter                     )) ap_pct_adapter,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_aligned_bases                )) ap_pf_aligned_bases,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_mismatch_rate                )) ap_pf_mismatch_rate,
    MAX(DECODE(CATEGORY, 'PAIR', al.pct_chimeras                    )) ap_pct_chimeras,
    MAX(DECODE(CATEGORY, 'PAIR', al.pf_indel_rate                   )) ap_pf_indel_rate,

    MAX(DECODE(CATEGORY, 'UNPAIRED', al.total_reads                 )) anp_total_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_reads                    )) anp_pf_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_pf_reads                )) anp_pct_pf_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_noise_reads              )) anp_pf_noise_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_reads_aligned            )) anp_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_pf_reads_aligned        )) anp_pct_pf_reads_aligned,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_aligned_reads         )) anp_pf_hq_aligned_reads,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_aligned_bases         )) anp_pf_hq_aligned_bases,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_aligned_q20_bases     )) anp_pf_hq_aligned_q20_bases,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_median_mismatches     )) anp_pf_hq_median_mismatches,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_hq_error_rate            )) anp_pf_hq_error_rate,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.mean_read_length            )) anp_mean_read_length,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.reads_aligned_in_pairs      )) anp_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_reads_aligned_in_pairs  )) anp_pct_reads_aligned_in_pairs,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.bad_cycles                  )) anp_bad_cycles,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.strand_balance              )) anp_pct_strand_balance,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_adapter                 )) anp_pct_adapter,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_aligned_bases            )) anp_pf_aligned_bases,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_mismatch_rate            )) anp_pf_mismatch_rate,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pct_chimeras                )) anp_pct_chimeras,
    MAX(DECODE(CATEGORY, 'UNPAIRED', al.pf_indel_rate               )) anp_pf_indel_rate

FROM metrics.aggregation_alignment/*DBLINK*/ al
JOIN agg ON agg.id=al.aggregation_id
WHERE category IN ('PAIR', 'UNPAIRED')

GROUP BY al.aggregation_id
),

ins AS (
SELECT
    ins.*,
    ROW_NUMBER() over (PARTITION BY ins.aggregation_id ORDER BY ins.read_pairs desc) read_pair_rank
FROM metrics.aggregation_insert_size/*DBLINK*/ ins
JOIN agg ON agg.id=ins.aggregation_id
),

oxog AS (
SELECT
    --PK
    aggregation_id,
    --
    max(total_sites)           total_sites,
    max(total_bases)           total_bases,
    max(ref_nonoxo_bases)      ref_nonoxo_bases,
    max(ref_oxo_bases)         ref_oxo_bases,
    max(ref_total_bases)       ref_total_bases,
    max(alt_nonoxo_bases)      alt_nonoxo_bases,
    max(alt_oxo_bases)         alt_oxo_bases,
    max(oxidation_error_rate)  oxidation_error_rate,
    max(oxidation_q)           oxidation_q,

    min(worst_oxo_q_context)   worst_oxo_q_context,
    min(worst_c_ref_oxo_q)     worst_c_ref_oxo_q
FROM (
    SELECT
        --pk
        agg.id aggregation_id,
        oxog.CONTEXT,
        --
        decode(oxog.CONTEXT, 'CCG', oxog.total_sites)           total_sites,
        decode(oxog.CONTEXT, 'CCG', oxog.total_bases)           total_bases,
        decode(oxog.CONTEXT, 'CCG', oxog.ref_nonoxo_bases)      ref_nonoxo_bases,
        decode(oxog.CONTEXT, 'CCG', oxog.ref_oxo_bases)         ref_oxo_bases,
        decode(oxog.CONTEXT, 'CCG', oxog.ref_total_bases)       ref_total_bases,
        decode(oxog.CONTEXT, 'CCG', oxog.alt_nonoxo_bases)      alt_nonoxo_bases,
        decode(oxog.CONTEXT, 'CCG', oxog.alt_oxo_bases)         alt_oxo_bases,
        decode(oxog.CONTEXT, 'CCG', oxog.oxidation_error_rate)  oxidation_error_rate,
        decode(oxog.CONTEXT, 'CCG', oxog.oxidation_q)           oxidation_q,
        CASE
            WHEN oxog.c_ref_oxo_q IS NULL THEN ''
            ELSE
                min(oxog.context) keep (dense_rank first ORDER BY oxog.c_ref_oxo_q ASC ) over (PARTITION BY oxog.aggregation_id)
        END worst_oxo_q_context,
        min(oxog.c_ref_oxo_q) keep (dense_rank first ORDER BY oxog.c_ref_oxo_q ASC ) over (PARTITION BY oxog.aggregation_id) worst_c_ref_oxo_q
    FROM agg
    LEFT JOIN metrics.aggregation_oxog/*DBLINK*/ oxog ON oxog.aggregation_id = agg.id
)
GROUP BY aggregation_id
)


SELECT
    --PK
    agg.project,
    agg.SAMPLE,
    agg.LIBRARY,
    agg.data_type,
    --

	agg.processing_location,
    agg.id analysis_id,
    agg.workflow_start_date analysis_start,
    nvl(agg.workflow_end_date, agg.created_at) analysis_end,
    agg.version,

    metadata.library_creation_date,
    metadata.on_risk,
    metadata.risk_types,
    metadata.initiative,
    metadata.lcset,
    metadata.lcset_type,
    metadata.lcset_protocol,
    metadata.lcset_seq_technology,
    metadata.lcset_topoff,
    metadata.lab_workflow,
    metadata.analysis_type,
    metadata.product_order_key,
    metadata.product,
    metadata.product_part_number,
    metadata.product_order_sample,
    metadata.research_project_name,
    metadata.research_project_number,

    metadata.gssr_id,
    metadata.sample_id,
    metadata.organism_scientific_name,
    metadata.molecular_barcode_name,
    metadata.bsp_original_material_type,
	metadata.bsp_root_material_type, 
	metadata.wr_type,
    metadata.wr_domain,

    metadata.n_lanes  n_aggregated_lanes,
    metadata.lanes    aggregated_lanes,
    agg_bc.pf_bases   bc_total_pf_bases,

    -- DUP metrics --
    dup.unpaired_reads_examined   dup_unpaired_reads_examined,
    dup.read_pairs_examined       dup_read_pairs_examined,
    dup.unmapped_reads            dup_unmapped_reads,
    dup.unpaired_read_duplicates  dup_unpaired_read_duplicates,
    dup.read_pair_duplicates      dup_read_pair_duplicates,
    100*dup.percent_duplication       dup_percent_duplication,
    dup.estimated_library_size    dup_estimated_library_size,
    dup.read_pair_optical_duplicates dup_read_pair_optical_dups,

    -- HS metrics --
    hs.bait_set                   hs_bait_set,
    hs.genome_size                hs_genome_size,
    hs.bait_territory             hs_bait_territory,
    hs.target_territory           hs_target_territory,
    hs.bait_design_efficiency     hs_bait_design_efficiency,
    hs.total_reads                hs_total_reads,
    hs.pf_reads                   pf_reads,
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
    100*hs.pct_target_bases_40x       hs_pct_target_bases_40x,
    100*hs.pct_target_bases_50x       hs_pct_target_bases_50x,
    100*hs.pct_target_bases_100x      hs_pct_target_bases_100x,
    100*hs.pct_usable_bases_on_bait   hs_pct_usable_bases_on_bait,
    100*hs.pct_usable_bases_on_target hs_pct_usable_bases_on_target,
    hs.hs_library_size            hs_library_size,
    hs.hs_penalty_10x             hs_penalty_10x,
    hs.hs_penalty_20x             hs_penalty_20x,
    hs.hs_penalty_30x             hs_penalty_30x,
    hs.hs_penalty_40x             hs_penalty_40x,
    hs.hs_penalty_50x             hs_penalty_50x,
    hs.hs_penalty_100x            hs_penalty_100x,


    -- ALIGN metrics of all lanes --
    NVL(ap_total_reads            ,0)+NVL(anp_total_reads            ,0) al_total_reads,
    NVL(ap_pf_reads               ,0)+NVL(anp_pf_reads               ,0) al_pf_reads,
    NVL(ap_pf_noise_reads         ,0)+NVL(anp_pf_noise_reads         ,0) al_pf_noise_reads,
    NVL(ap_pf_reads_aligned       ,0)+NVL(anp_pf_reads_aligned       ,0) al_pf_reads_aligned,
    NVL(ap_pf_hq_aligned_reads    ,0)+NVL(anp_pf_hq_aligned_reads    ,0) al_pf_hq_aligned_reads,
    NVL(ap_pf_hq_aligned_bases    ,0)+NVL(anp_pf_hq_aligned_bases    ,0) al_pf_hq_aligned_bases,
    NVL(ap_pf_hq_aligned_q20_bases,0)+NVL(anp_pf_hq_aligned_q20_bases,0) al_pf_hq_aligned_q20_bases,
    100*decode(nvl(ap_pf_reads, 0)+nvl(anp_pf_reads, 0), 0, 0,
    (nvl(ap_pf_reads * ap_pct_adapter, 0) + nvl(anp_pf_reads * anp_pct_adapter, 0))/(nvl(ap_pf_reads, 0)+nvl(anp_pf_reads, 0))
    ) al_pct_adapter,
    NVL(ap_pf_aligned_bases       ,0)+NVL(anp_pf_aligned_bases       ,0) al_pf_aligned_bases,
    NVL(ap_pf_mismatch_rate       ,0)+NVL(anp_pf_mismatch_rate       ,0) al_pf_mismatch_rate,
    100*ap_pct_chimeras                                                      al_pct_chimeras,  -- there are no anp_pct_chimeras because chimerism is when  both ends are in different chromosomes => pct chimeras does not apply to UNPAIRED reads

    -- ALIGN metrics of pair lanes --
    ap_total_reads,
    ap_pf_reads,
    100*ap_pct_pf_reads  ap_pct_pf_reads,
    ap_pf_noise_reads,
    ap_pf_reads_aligned,
    100*ap_pct_pf_reads_aligned  ap_pct_pf_reads_aligned,
    ap_pf_hq_aligned_reads,
    ap_pf_hq_aligned_bases,
    ap_pf_hq_aligned_q20_bases,
    ap_pf_hq_median_mismatches,
    ap_pf_hq_error_rate,
    ap_mean_read_length,
    ap_reads_aligned_in_pairs,
    100*ap_pct_reads_aligned_in_pairs  ap_pct_reads_aligned_in_pairs,
    ap_bad_cycles,
    100*ap_pct_strand_balance  ap_pct_strand_balance,
    ap_pf_aligned_bases,
    ap_pf_mismatch_rate,
    100*ap_pct_chimeras    ap_pct_chimeras,

    -- ALIGN metrics of unpaired lanes --
    anp_total_reads,
    anp_pf_reads,
    100*anp_pct_pf_reads     anp_pct_pf_reads,
    anp_pf_noise_reads,
    anp_pf_reads_aligned,
    100*anp_pct_pf_reads_aligned   anp_pct_pf_reads_aligned,
    anp_pf_hq_aligned_reads,
    anp_pf_hq_aligned_bases,
    anp_pf_hq_aligned_q20_bases,
    anp_pf_hq_median_mismatches,
    anp_pf_hq_error_rate,
    anp_mean_read_length,
    anp_reads_aligned_in_pairs,
    100*anp_pct_reads_aligned_in_pairs     anp_pct_reads_aligned_in_pairs,
    anp_bad_cycles,
    100*anp_pct_strand_balance           anp_pct_strand_balance,
    anp_pf_aligned_bases,
    anp_pf_mismatch_rate,
    100*anp_pct_chimeras             anp_pct_chimeras,

    -- DBSNP metrics--
    dbsnp.total_snps                snp_total_snps,
    100*dbsnp.pct_dbsnp                 snp_pct_dbsnp,
    dbsnp.num_in_db_snp             snp_num_in_dbsnp,

    ins.median_insert_size        ins_median_insert_size,
    ins.min_insert_size           ins_min_insert_size,
    ins.max_insert_size           ins_max_insert_size,
    ins.mean_insert_size          ins_mean_insert_size,
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
    ins.pair_orientation          ins_pair_orientation,

    -- RNA Seq
    rnaseq.pf_aligned_bases rna_pf_aligned_bases,
    rnaseq.ribosomal_bases        rna_ribosomal_bases,
    rnaseq.coding_bases           rna_coding_bases,
    rnaseq.utr_bases              rna_utr_bases,
    rnaseq.intronic_bases         rna_intronic_bases,
    rnaseq.intergenic_bases       rna_intergenic_bases,
    rnaseq.correct_strand_reads   rna_correct_strand_reads,
    rnaseq.incorrect_strand_reads rna_incorrect_strand_reads,
    100*rnaseq.pct_ribosomal_bases    rna_pct_ribosomal_bases,
    100*rnaseq.pct_coding_bases       rna_pct_coding_bases,
    100*rnaseq.pct_utr_bases          rna_pct_utr_bases,
    100*rnaseq.pct_intronic_bases     rna_pct_intronic_bases,
    100*rnaseq.pct_intergenic_bases   rna_pct_intergenic_bases,
    100*rnaseq.pct_mrna_bases         rna_pct_mrna_bases,
    100*rnaseq.pct_correct_strand_reads rna_pct_correct_strand_reads,
    rnaseq.pf_bases               rna_pf_bases,
    100*rnaseq.pct_usable_bases       rna_pct_usable_bases,
    rnaseq.median_cv_coverage     rna_median_cv_coverage,
    rnaseq.median_5prime_bias     rna_median_5prime_bias,
    rnaseq.median_3prime_bias     rna_median_3prime_bias,
    rnaseq.median_5prime_to_3prime_bias rna_median_5prim_to_3prim_bias,

    pcr.custom_amplicon_set           pcr_custom_amplicon_set,
    pcr.genome_size                   pcr_genome_size,
    pcr.amplicon_territory            pcr_amplicon_territory,
    pcr.target_territory              pcr_target_territory,
    pcr.total_reads                   pcr_total_reads,
    pcr.pf_reads                      pcr_pf_reads,
    pcr.pf_unique_reads               pcr_pf_unique_reads,
    100*pcr.pct_pf_reads                  pcr_pct_pf_reads,
    100*pcr.pct_pf_uq_reads               pcr_pct_pf_uq_reads,
    pcr.pf_uq_reads_aligned           pcr_pf_uq_reads_aligned,
    100*pcr.pct_pf_uq_reads_aligned       pcr_pct_pf_uq_reads_aligned,
    pcr.pf_uq_bases_aligned           pcr_pf_uq_bases_aligned,
    pcr.on_amplicon_bases             pcr_on_amplicon_bases,
    pcr.near_amplicon_bases           pcr_near_amplicon_bases,
    pcr.off_amplicon_bases            pcr_off_amplicon_bases,
    pcr.on_target_bases               pcr_on_target_bases,
    100*pcr.pct_amplified_bases           pcr_pct_amplified_bases,
    100*pcr.pct_off_amplicon              pcr_pct_off_amplicon,
    pcr.on_amplicon_vs_selected       pcr_on_amplicon_vs_selected,
    pcr.mean_amplicon_coverage        pcr_mean_amplicon_coverage,
    pcr.mean_target_coverage          pcr_mean_target_coverage,
    pcr.fold_enrichment               pcr_fold_enrichment,
    100*pcr.zero_cvg_targets_pct          pcr_zero_cvg_targets_pct,
    pcr.fold_80_base_penalty          pcr_fold_80_base_penalty,
    100*pcr.pct_target_bases_2x           pcr_pct_target_bases_2x,
    100*pcr.pct_target_bases_10x          pcr_pct_target_bases_10x,
    100*pcr.pct_target_bases_20x          pcr_pct_target_bases_20x,
    100*pcr.pct_target_bases_30x          pcr_pct_target_bases_30x,
    pcr.at_dropout                    pcr_at_dropout,
    pcr.gc_dropout                    pcr_gc_dropout,

--    oxog.CONTEXT                      oxog_context ,
    oxog.total_sites                  oxogccg_total_sites,
    oxog.total_bases                  oxogccg_total_bases,
    oxog.ref_nonoxo_bases             oxogccg_ref_nonoxo_bases,
    oxog.ref_oxo_bases                oxogccg_ref_oxo_bases,
    oxog.ref_total_bases              oxogccg_ref_total_bases,
    oxog.alt_nonoxo_bases             oxogccg_alt_nonoxo_bases,
    oxog.alt_oxo_bases                oxogccg_alt_oxo_bases,
    oxog.oxidation_error_rate         oxogccg_oxidation_error_rate,
    oxog.oxidation_q                  oxogccg_oxidation_q,
    oxog.worst_oxo_q_context         oxog_worst_oxo_q_context,
    oxog.worst_c_ref_oxo_q           oxog_worst_c_ref_oxo_q,

-- RRBS metrics -- from new schema
    rrbs_agg.reads_aligned                  rrbs_reads_aligned,
    rrbs_agg.non_cpg_bases                  rrbs_non_cpg_bases,
    rrbs_agg.non_cpg_converted_bases        rrbs_non_cpg_converted_bases,
    100*rrbs_agg.pct_non_cpg_bases_converted    rrbs_pct_noncpg_bases_convertd,
    rrbs_agg.cpg_bases_seen                 rrbs_cpg_bases_seen,
    rrbs_agg.cpg_bases_converted            rrbs_cpg_bases_converted,
    100*rrbs_agg.pct_cpg_bases_converted        rrbs_pct_cpg_bases_converted,
    rrbs_agg.mean_cpg_coverage              rrbs_mean_cpg_coverage,
    rrbs_agg.median_cpg_coverage            rrbs_median_cpg_coverage,
    rrbs_agg.reads_with_no_cpg              rrbs_reads_with_no_cpg,
    rrbs_agg.reads_ignored_short            rrbs_reads_ignored_short,
    rrbs_agg.reads_ignored_mismatches       rrbs_reads_ignored_mismatches,

    metadata.pond,
    metadata.catch,

    agg_fp.min_lod,
    agg_fp.max_lod,
    metadata.last_run_end_date,
    metadata.pdo_title,
    ap_pf_indel_rate,
    anp_pf_indel_rate,

-- Artifact metrics
    --Bait_Bias Cref artifact
    CASE WHEN bb.ref_base IS NULL OR bb.alt_base IS NULL THEN NULL 
        ELSE bb.ref_base ||'>'||bb.alt_base
    END  cref_ref_alt_base,
    bb.total_qscore             cref_total_qscore,
    bb.worst_cxt                cref_worst_cxt,
    bb.worst_cxt_qscore         cref_worst_cxt_qscore, 
    bb.worst_pre_cxt            cref_worst_pre_cxt,
    bb.worst_pre_cxt_qscore     cref_worst_pre_cxt_qscore, 
    bb.worst_post_cxt           cref_worst_post_cxt,
    bb.worst_post_cxt_qscore    cref_worst_post_cxt_qscore,
    -- Pre_adapter OxoG artifact
    CASE WHEN pa.ref_base IS NULL  OR  pa.alt_base IS NULL THEN NULL
        ELSE pa.ref_base||'>'|| pa.alt_base 
    END oxog_ref_alt_base, 
    pa.total_qscore             oxog_total_qscore,
    pa.worst_cxt                oxog_worst_cxt, 
    pa.worst_cxt_qscore         oxog_worst_cxt_qscore, 
    pa.worst_pre_cxt            oxog_worst_pre_cxt,
    pa.worst_pre_cxt_qscore     oxog_worst_pre_cxt_qscore, 
    pa.worst_post_cxt           oxog_worst_post_cxt,
    pa.worst_post_cxt_qscore    oxog_worst_post_cxt_qscore,

    metadata.n_hiseq_pool_test_lanes,
	metadata.n_readgroups n_aggregated_rg,
	metadata.bsp_sample_type sample_type,
	metadata.index_type,
    hs.at_dropout hs_at_dropout,
    CASE WHEN dup.read_pairs_examined = 0 THEN NULL 
    ELSE trunc(100*dup.read_pair_optical_duplicates / dup.read_pairs_examined,4) 
    END dup_pct_optical_duplication,
    
    CASE WHEN dup.unpaired_reads_examined + dup.read_pairs_examined*2 = 0 THEN NULL 
    ELSE trunc(100*(dup.unpaired_read_duplicates + dup.read_pair_duplicates*2 - dup.read_pair_optical_duplicates*2) / (dup.unpaired_reads_examined + dup.read_pairs_examined*2),4) 
    END dup_pct_nonoptical_duplication,

    agg.is_latest

FROM agg
JOIN metrics.aggregation_duplication/*DBLINK*/ dup    ON        dup.aggregation_id = agg.id
JOIN metrics.aggregation_hybrid_selection/*DBLINK*/ hs ON        hs.aggregation_id = agg.id
JOIN align al                               ON         al.aggregation_id = agg.id
JOIN metrics.aggregation_dbsnp/*DBLINK*/ dbsnp        ON      dbsnp.aggregation_id = agg.id
JOIN ins                                    ON        ins.aggregation_id = agg.id AND ins.read_pair_rank = 1
JOIN metrics.aggregation_rna_seq/*DBLINK*/ rnaseq     ON     rnaseq.aggregation_id = agg.id
JOIN metrics.aggregation_pcr/*DBLINK*/ pcr            ON        pcr.aggregation_id = agg.id
LEFT JOIN metrics.aggregation_rrbs_summary/*DBLINK*/ rrbs_agg ON rrbs_agg.aggregation_id = agg.id
--LEFT JOIN metrics.aggregation_oxog/*DBLINK*/ oxog     ON       oxog.aggregation_id = agg.id AND oxog.CONTEXT ='CCG'  -- not all aggregations have rows context='CCG'
--outer join is done in oxog subquery and we don't need only CCG context anymore, it is accounted in the subquery
JOIN oxog     ON       oxog.aggregation_id = agg.id -- AND oxog.CONTEXT ='CCG'

-- NEW
LEFT JOIN metrics.aggregation_bait_bias/*DBLINK*/ bb   ON bb.aggregation_id = agg.id AND bb.artifact_name = 'Cref'  
LEFT JOIN metrics.aggregation_pre_adapter/*DBLINK*/ pa ON pa.aggregation_id = agg.id AND pa.artifact_name = 'OxoG'

LEFT JOIN metadata ON
    metadata.project    = agg.project AND
    metadata.SAMPLE     = agg.SAMPLE  AND
    metadata.LIBRARY    = agg.LIBRARY AND
    metadata.data_type  = agg.data_type AND
    metadata.version    = agg.version AND
    metadata.processing_location = agg.processing_location

LEFT JOIN agg_bc ON
    agg_bc.project      = agg.project AND
    agg_bc.SAMPLE       = agg.SAMPLE  AND
    agg_bc.LIBRARY      = agg.LIBRARY AND
    agg_bc.data_type    = agg.data_type AND
    agg_bc.version      = agg.version AND
    agg_bc.processing_location = agg.processing_location

LEFT JOIN agg_fp ON
    agg_fp.project      = agg.project AND
    agg_fp.SAMPLE       = agg.SAMPLE  AND
    agg_fp.LIBRARY      = agg.LIBRARY AND
    agg_fp.data_type    = agg.data_type AND
    agg_fp.version      = agg.version AND
    agg_fp.processing_location = agg.processing_location
) DELTA
ON (tr.analysis_id=DELTA.analysis_id AND tr.source=/*SOURCE*/)

WHEN NOT MATCHED THEN
INSERT values(
--PK
delta.project, delta.sample, delta.library, delta.data_type,
--
SYSDATE, -- timestamp
delta.analysis_id,
       delta.analysis_start, delta.analysis_end, delta.version,
       delta.library_creation_date, delta.on_risk, delta.risk_types, delta.initiative,
       delta.lcset, delta.lab_workflow, delta.analysis_type, delta.product_order_key,
       delta.product, delta.product_part_number, delta.product_order_sample,
       delta.research_project_name, delta.research_project_number, delta.gssr_id,
       delta.sample_id, delta.organism_scientific_name,
       delta.molecular_barcode_name, delta.wr_type, delta.wr_domain,
       delta.n_aggregated_lanes, delta.aggregated_lanes, delta.bc_total_pf_bases,
       delta.dup_unpaired_reads_examined, delta.dup_read_pairs_examined,
       delta.dup_unmapped_reads, delta.dup_unpaired_read_duplicates,
       delta.dup_read_pair_duplicates, delta.dup_percent_duplication,
       delta.dup_estimated_library_size, delta.dup_read_pair_optical_dups,
       delta.hs_bait_set, delta.hs_genome_size, delta.hs_bait_territory,
       delta.hs_target_territory, delta.hs_bait_design_efficiency,
       delta.hs_total_reads, delta.pf_reads, delta.hs_pf_unique_reads,
       delta.hs_pct_pf_reads, delta.hs_pct_pf_uq_reads,
       delta.hs_pf_uq_reads_aligned, delta.hs_pct_pf_uq_reads_aligned,
       delta.hs_pf_uq_bases_aligned, delta.hs_on_bait_bases,
       delta.hs_near_bait_bases, delta.hs_off_bait_bases, delta.hs_on_target_bases,
       delta.hs_pct_selected_bases, delta.hs_pct_off_bait,
       delta.hs_on_bait_vs_selected, delta.hs_mean_bait_coverage,
       delta.hs_mean_target_coverage, delta.hs_fold_enrichment,
       delta.hs_zero_cvg_targets_pct, delta.hs_fold_80_base_penalty,
       delta.hs_pct_target_bases_2x, delta.hs_pct_target_bases_10x,
       delta.hs_pct_target_bases_20x, delta.hs_pct_target_bases_30x,
       delta.hs_pct_target_bases_40x, delta.hs_pct_target_bases_50x,
       delta.hs_pct_target_bases_100x, delta.hs_pct_usable_bases_on_bait,
       delta.hs_pct_usable_bases_on_target, delta.hs_library_size,
       delta.hs_penalty_10x, delta.hs_penalty_20x, delta.hs_penalty_30x,
       delta.hs_penalty_40x, delta.hs_penalty_50x, delta.hs_penalty_100x,
       delta.al_total_reads, delta.al_pf_reads, delta.al_pf_noise_reads,
       delta.al_pf_reads_aligned, delta.al_pf_hq_aligned_reads,
       delta.al_pf_hq_aligned_bases, delta.al_pf_hq_aligned_q20_bases,
       delta.al_pct_adapter, delta.al_pf_aligned_bases, delta.al_pf_mismatch_rate,
       delta.al_pct_chimeras, delta.ap_total_reads, delta.ap_pf_reads,
       delta.ap_pct_pf_reads, delta.ap_pf_noise_reads, delta.ap_pf_reads_aligned,
       delta.ap_pct_pf_reads_aligned, delta.ap_pf_hq_aligned_reads,
       delta.ap_pf_hq_aligned_bases, delta.ap_pf_hq_aligned_q20_bases,
       delta.ap_pf_hq_median_mismatches, delta.ap_pf_hq_error_rate,
       delta.ap_mean_read_length, delta.ap_reads_aligned_in_pairs,
       delta.ap_pct_reads_aligned_in_pairs, delta.ap_bad_cycles,
       delta.ap_pct_strand_balance, delta.ap_pf_aligned_bases,
       delta.ap_pf_mismatch_rate, delta.ap_pct_chimeras, delta.anp_total_reads,
       delta.anp_pf_reads, delta.anp_pct_pf_reads, delta.anp_pf_noise_reads,
       delta.anp_pf_reads_aligned, delta.anp_pct_pf_reads_aligned,
       delta.anp_pf_hq_aligned_reads, delta.anp_pf_hq_aligned_bases,
       delta.anp_pf_hq_aligned_q20_bases, delta.anp_pf_hq_median_mismatches,
       delta.anp_pf_hq_error_rate, delta.anp_mean_read_length,
       delta.anp_reads_aligned_in_pairs, delta.anp_pct_reads_aligned_in_pairs,
       delta.anp_bad_cycles, delta.anp_pct_strand_balance,
       delta.anp_pf_aligned_bases, delta.anp_pf_mismatch_rate,
       delta.anp_pct_chimeras, delta.snp_total_snps, delta.snp_pct_dbsnp,
       delta.snp_num_in_dbsnp, delta.ins_median_insert_size,
       delta.ins_min_insert_size, delta.ins_max_insert_size,
       delta.ins_mean_insert_size, delta.ins_standard_deviation,
       delta.ins_read_pairs, delta.ins_width_of_10_percent,
       delta.ins_width_of_20_percent, delta.ins_width_of_30_percent,
       delta.ins_width_of_40_percent, delta.ins_width_of_50_percent,
       delta.ins_width_of_60_percent, delta.ins_width_of_70_percent,
       delta.ins_width_of_80_percent, delta.ins_width_of_90_percent,
       delta.ins_width_of_99_percent, delta.ins_pair_orientation,
       delta.rna_pf_aligned_bases, delta.rna_ribosomal_bases,
       delta.rna_coding_bases, delta.rna_utr_bases, delta.rna_intronic_bases,
       delta.rna_intergenic_bases, delta.rna_correct_strand_reads,
       delta.rna_incorrect_strand_reads, delta.rna_pct_ribosomal_bases,
       delta.rna_pct_coding_bases, delta.rna_pct_utr_bases,
       delta.rna_pct_intronic_bases, delta.rna_pct_intergenic_bases,
       delta.rna_pct_mrna_bases, delta.rna_pct_correct_strand_reads,
       delta.rna_pf_bases, delta.rna_pct_usable_bases, delta.rna_median_cv_coverage,
       delta.rna_median_5prime_bias, delta.rna_median_3prime_bias,
       delta.rna_median_5prim_to_3prim_bias, delta.pcr_custom_amplicon_set,
       delta.pcr_genome_size, delta.pcr_amplicon_territory,
       delta.pcr_target_territory, delta.pcr_total_reads, delta.pcr_pf_reads,
       delta.pcr_pf_unique_reads, delta.pcr_pct_pf_reads, delta.pcr_pct_pf_uq_reads,
       delta.pcr_pf_uq_reads_aligned, delta.pcr_pct_pf_uq_reads_aligned,
       delta.pcr_pf_uq_bases_aligned, delta.pcr_on_amplicon_bases,
       delta.pcr_near_amplicon_bases, delta.pcr_off_amplicon_bases,
       delta.pcr_on_target_bases, delta.pcr_pct_amplified_bases,
       delta.pcr_pct_off_amplicon, delta.pcr_on_amplicon_vs_selected,
       delta.pcr_mean_amplicon_coverage, delta.pcr_mean_target_coverage,
       delta.pcr_fold_enrichment, delta.pcr_zero_cvg_targets_pct,
       delta.pcr_fold_80_base_penalty, delta.pcr_pct_target_bases_2x,
       delta.pcr_pct_target_bases_10x, delta.pcr_pct_target_bases_20x,
       delta.pcr_pct_target_bases_30x, delta.pcr_at_dropout, delta.pcr_gc_dropout,
        delta.oxogccg_total_sites, delta.oxogccg_total_bases,
       delta.oxogccg_ref_nonoxo_bases, delta.oxogccg_ref_oxo_bases,
       delta.oxogccg_ref_total_bases, delta.oxogccg_alt_nonoxo_bases,
       delta.oxogccg_alt_oxo_bases, delta.oxogccg_oxidation_error_rate,
       delta.oxogccg_oxidation_q,
DELTA.oxog_worst_oxo_q_context,
DELTA.oxog_worst_c_ref_oxo_q,

       delta.rrbs_reads_aligned, delta.rrbs_non_cpg_bases,
       delta.rrbs_non_cpg_converted_bases, delta.rrbs_pct_noncpg_bases_convertd,
       delta.rrbs_cpg_bases_seen, delta.rrbs_cpg_bases_converted,
       delta.rrbs_pct_cpg_bases_converted, delta.rrbs_mean_cpg_coverage,
       delta.rrbs_median_cpg_coverage, delta.rrbs_reads_with_no_cpg,
       delta.rrbs_reads_ignored_short, delta.rrbs_reads_ignored_mismatches,
       delta.pond, delta.catch,
delta.min_lod,
delta.max_lod,
delta.last_run_end_date,
delta.lcset_type,
delta.lcset_protocol,
delta.lcset_seq_technology,
delta.lcset_topoff,

/*SOURCE*/
, DELTA.pdo_title,
DELTA.ap_pf_indel_rate,
DELTA.anp_pf_indel_rate,
DELTA.bsp_original_material_type,
DELTA.bsp_root_material_type,
-- artifact metrics
DELTA.cref_ref_alt_base,
DELTA.cref_total_qscore,
DELTA.cref_worst_cxt,
DELTA.cref_worst_cxt_qscore, 
DELTA.cref_worst_pre_cxt,
DELTA.cref_worst_pre_cxt_qscore, 
DELTA.cref_worst_post_cxt,
DELTA.cref_worst_post_cxt_qscore,
DELTA.oxog_ref_alt_base, 
DELTA.oxog_total_qscore,
DELTA.oxog_worst_cxt, 
DELTA.oxog_worst_cxt_qscore, 
DELTA.oxog_worst_pre_cxt,
DELTA.oxog_worst_pre_cxt_qscore, 
DELTA.oxog_worst_post_cxt,
DELTA.oxog_worst_post_cxt_qscore,
DELTA.n_hiseq_pool_test_lanes,
DELTA.n_aggregated_rg,
DELTA.processing_location,
to_char(null), -- sample_type
DELTA.index_type,
DELTA.hs_at_dropout,
DELTA.dup_pct_optical_duplication,
DELTA.dup_pct_nonoptical_duplication

)
WHERE DELTA.is_latest<>0  --=1

WHEN MATCHED THEN
UPDATE
SET tr.TIMESTAMP                	= SYSDATE,
    tr.on_risk                  	= delta.on_risk,
    tr.risk_types               	= delta.risk_types,
    tr.initiative               	= delta.initiative,
    tr.lcset                    	= delta.lcset,
    tr.lcset_type               	= delta.lcset_type,
    tr.lcset_protocol           	= delta.lcset_protocol,
    tr.lcset_seq_technology     	= delta.lcset_seq_technology,
    tr.lcset_topoff             	= delta.lcset_topoff,
    tr.lab_workflow             	= delta.lab_workflow,
    tr.analysis_type            	= delta.analysis_type,
    tr.product_order_key        	= decode(DELTA.is_latest, 0, 'DELETE', delta.product_order_key),
    tr.pdo_title                	= delta.pdo_title,
    tr.product                  	= delta.product,
    tr.product_part_number      	= delta.product_part_number,
    tr.product_order_sample     	= delta.product_order_sample,
    tr.research_project_name    	= delta.research_project_name,
    tr.research_project_number		= delta.research_project_number,
    tr.gssr_id                  	= delta.gssr_id,
    tr.sample_id                	= delta.sample_id,
	tr.bsp_original_material_type 	= delta.bsp_original_material_type,
	tr.bsp_root_material_type		= delta.bsp_root_material_type,	
	tr.sample_type					= delta.sample_type,	
    tr.organism_scientific_name 	= delta.organism_scientific_name,
    tr.molecular_barcode_name   	= delta.molecular_barcode_name,
    tr.wr_type                  	= delta.wr_type,
    tr.wr_domain                	= delta.wr_domain,
    tr.n_aggregated_lanes       	= delta.n_aggregated_lanes,
    tr.n_aggregated_rg       		= delta.n_aggregated_rg,
    tr.aggregated_lanes         	= delta.aggregated_lanes,
    tr.n_hiseq_pool_test_lanes      = delta.n_hiseq_pool_test_lanes	,
    tr.bc_total_pf_bases        	= DELTA.bc_total_pf_bases,
    tr.pond                     	= delta.pond,
    tr.catch                    	= delta.catch,
    tr.last_run_end_date        	= delta.last_run_end_date,

    tr.min_lod                  	= delta.min_lod,
    tr.max_lod                  	= delta.max_lod,
	tr.processing_location			= delta.processing_location,
	tr.index_type					= delta.index_type
--    tr.is_latest                = DELTA.is_latest

DELETE WHERE tr.product_order_key =  'DELETE' -- to clean up old aggs in case of re-projecting
;

--STATEMENT:artifact metrics if any
BEGIN
IF /*SOURCE*/='Regular' THEN 
MERGE INTO COGNOS.slxre2_pagg_library_artifact_2 laaf
USING (

WITH
agg AS (
    SELECT * 
    FROM metrics.aggregation/*DB_LINK*/ a 
    WHERE a.LIBRARY IS not NULL
    AND a.SAMPLE IN
       (SELECT DISTINCT  a.SAMPLE FROM metrics.aggregation/*DB_LINK*/ a
       WHERE a.LIBRARY IS NOT NULL
         AND nvl(a.modified_at, a.workflow_end_date) >= /*DELTA_START*/
         AND nvl(a.modified_at, a.workflow_end_date) < /*DELTA_END*/
       )
    AND a.SAMPLE NOT IN ('K-562', 'NA12878')
       
),
bait_bias AS (
SELECT 
    --PK
    id,
    --
        max(bb_ac_total_qscore)             bb_ac_total_qscore,
        max(bb_ac_worst_cxt)                bb_ac_worst_cxt,
        max(bb_ac_worst_cxt_qscore)         bb_ac_worst_cxt_qscore,
        max(bb_ac_worst_pre_cxt)            bb_ac_worst_pre_cxt,
        max(bb_ac_worst_pre_cxt_qscore)     bb_ac_worst_pre_cxt_qscore,
        max(bb_ac_worst_post_cxt)           bb_ac_worst_post_cxt,
        max(bb_ac_worst_post_cxt_qscore)    bb_ac_worst_post_cxt_qscore,

        max(bb_ag_total_qscore)             bb_ag_total_qscore,
        max(bb_ag_worst_cxt)                bb_ag_worst_cxt,
        max(bb_ag_worst_cxt_qscore)         bb_ag_worst_cxt_qscore,
        max(bb_ag_worst_pre_cxt)            bb_ag_worst_pre_cxt,
        max(bb_ag_worst_pre_cxt_qscore)     bb_ag_worst_pre_cxt_qscore,
        max(bb_ag_worst_post_cxt)           bb_ag_worst_post_cxt,
        max(bb_ag_worst_post_cxt_qscore)    bb_ag_worst_post_cxt_qscore,

        max(bb_at_total_qscore)             bb_at_total_qscore,
        max(bb_at_worst_cxt)                bb_at_worst_cxt,
        max(bb_at_worst_cxt_qscore)         bb_at_worst_cxt_qscore,
        max(bb_at_worst_pre_cxt)            bb_at_worst_pre_cxt,
        max(bb_at_worst_pre_cxt_qscore)     bb_at_worst_pre_cxt_qscore,
        max(bb_at_worst_post_cxt)           bb_at_worst_post_cxt,
        max(bb_at_worst_post_cxt_qscore)    bb_at_worst_post_cxt_qscore,

        max(bb_cref_total_qscore)           bb_cref_total_qscore,
        max(bb_cref_worst_cxt)              bb_cref_worst_cxt,
        max(bb_cref_worst_cxt_qscore)       bb_cref_worst_cxt_qscore,
        max(bb_cref_worst_pre_cxt)          bb_cref_worst_pre_cxt,
        max(bb_cref_worst_pre_cxt_qscore)   bb_cref_worst_pre_cxt_qscore,
        max(bb_cref_worst_post_cxt)         bb_cref_worst_post_cxt,
        max(bb_cref_worst_post_cxt_qscore)  bb_cref_worst_post_cxt_qscore,

        max(bb_cg_total_qscore)             bb_cg_total_qscore,
        max(bb_cg_worst_cxt)                bb_cg_worst_cxt,
        max(bb_cg_worst_cxt_qscore)         bb_cg_worst_cxt_qscore,
        max(bb_cg_worst_pre_cxt)            bb_cg_worst_pre_cxt,
        max(bb_cg_worst_pre_cxt_qscore)     bb_cg_worst_pre_cxt_qscore,
        max(bb_cg_worst_post_cxt)           bb_cg_worst_post_cxt,
        max(bb_cg_worst_post_cxt_qscore)    bb_cg_worst_post_cxt_qscore,

        max(bb_ct_total_qscore)             bb_ct_total_qscore,
        max(bb_ct_worst_cxt)                bb_ct_worst_cxt,
        max(bb_ct_worst_cxt_qscore)         bb_ct_worst_cxt_qscore,
        max(bb_ct_worst_pre_cxt)            bb_ct_worst_pre_cxt,
        max(bb_ct_worstprecxt_qscor)        bb_ct_worstprecxt_qscor,
        max(bb_ct_worst_post_cxt)           bb_ct_worst_post_cxt,
        max(bb_ct_worstpostcxt_scor)        bb_ct_worstpostcxt_scor,

        max(bb_ga_total_qscore)             bb_ga_total_qscore,
        max(bb_ga_worst_cxt)                bb_ga_worst_cxt,
        max(bb_ga_worst_cxt_qscore)         bb_ga_worst_cxt_qscore,
        max(bb_ga_worst_pre_cxt)            bb_ga_worst_pre_cxt,
        max(bb_ga_worst_pre_cxt_qscore)     bb_ga_worst_pre_cxt_qscore,
        max(bb_ga_worst_post_cxt)           bb_ga_worst_post_cxt,
        max(bb_ga_worst_post_cxt_qscore)    bb_ga_worst_post_cxt_qscore,

        max(bb_gc_total_qscore)             bb_gc_total_qscore,
        max(bb_gc_worst_cxt)                bb_gc_worst_cxt,
        max(bb_gc_worst_cxt_qscore)         bb_gc_worst_cxt_qscore,
        max(bb_gc_worst_pre_cxt)            bb_gc_worst_pre_cxt,
        max(bb_gc_worst_pre_cxt_qscore)     bb_gc_worst_pre_cxt_qscore,
        max(bb_gc_worst_post_cxt)           bb_gc_worst_post_cxt,
        max(bb_gc_worst_post_cxt_qscore)    bb_gc_worst_post_cxt_qscore,

        max(bb_gref_total_qscore)           bb_gref_total_qscore,
        max(bb_gref_worst_cxt)              bb_gref_worst_cxt,
        max(bb_gref_worst_cxt_qscore)       bb_gref_worst_cxt_qscore,
        max(bb_gref_worst_pre_cxt)          bb_gref_worst_pre_cxt,
        max(bb_gref_worst_pre_cxt_score)    bb_gref_worst_pre_cxt_score,
        max(bb_gref_worst_post_cxt)         bb_gref_worst_post_cxt,
        max(bb_gref_worstpostcxt_qscore)    bb_gref_worstpostcxt_qscore,

        max(bb_ta_total_qscore)             bb_ta_total_qscore,
        max(bb_ta_worst_cxt)                bb_ta_worst_cxt,
        max(bb_ta_worst_cxt_qscore)         bb_ta_worst_cxt_qscore,
        max(bb_ta_worst_pre_cxt)            bb_ta_worst_pre_cxt,
        max(bb_ta_worst_pre_cxt_qscore)     bb_ta_worst_pre_cxt_qscore,
        max(bb_ta_worst_post_cxt)           bb_ta_worst_post_cxt,
        max(bb_ta_worst_post_cxt_qscore)    bb_ta_worst_post_cxt_qscore,

        max(bb_tc_total_qscore)             bb_tc_total_qscore,
        max(bb_tc_worst_cxt)                bb_tc_worst_cxt,
        max(bb_tc_worst_cxt_qscore)         bb_tc_worst_cxt_qscore,
        max(bb_tc_worst_pre_cxt)            bb_tc_worst_pre_cxt,
        max(bb_tc_worst_pre_cxt_qscore)     bb_tc_worst_pre_cxt_qscore,
        max(bb_tc_worst_post_cxt)           bb_tc_worst_post_cxt,
        max(bb_tc_worst_post_cxt_qscore)    bb_tc_worst_post_cxt_qscore,

        max(bb_tg_total_qscore)             bb_tg_total_qscore,
        max(bb_tg_worst_cxt)                bb_tg_worst_cxt,
        max(bb_tg_worst_cxt_qscore)         bb_tg_worst_cxt_qscore,
        max(bb_tg_worst_pre_cxt)            bb_tg_worst_pre_cxt,
        max(bb_tg_worst_pre_cxt_qscore)     bb_tg_worst_pre_cxt_qscore,
        max(bb_tg_worst_post_cxt)           bb_tg_worst_post_cxt,
        max(bb_tg_worst_post_cxt_qscore)    bb_tg_worst_post_cxt_qscore


FROM (
    SELECT 
        --PK
        agg.id,
        bb.ref_base||bb.alt_base ref_alt_base,
        --
        decode(bb.ref_base||bb.alt_base, 'AC', bb.total_qscore         , NULL ) bb_ac_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'AC', bb.worst_cxt            , NULL ) bb_ac_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'AC', bb.worst_cxt_qscore     , NULL ) bb_ac_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'AC', bb.worst_pre_cxt        , NULL ) bb_ac_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'AC', bb.worst_pre_cxt_qscore , NULL ) bb_ac_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'AC', bb.worst_post_cxt       , NULL ) bb_ac_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'AC', bb.worst_post_cxt_qscore, NULL ) bb_ac_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'AG', bb.total_qscore         , NULL ) bb_ag_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'AG', bb.worst_cxt            , NULL ) bb_ag_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'AG', bb.worst_cxt_qscore     , NULL ) bb_ag_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'AG', bb.worst_pre_cxt        , NULL ) bb_ag_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'AG', bb.worst_pre_cxt_qscore , NULL ) bb_ag_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'AG', bb.worst_post_cxt       , NULL ) bb_ag_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'AG', bb.worst_post_cxt_qscore, NULL ) bb_ag_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'AT', bb.total_qscore         , NULL ) bb_at_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'AT', bb.worst_cxt            , NULL ) bb_at_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'AT', bb.worst_cxt_qscore     , NULL ) bb_at_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'AT', bb.worst_pre_cxt        , NULL ) bb_at_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'AT', bb.worst_pre_cxt_qscore , NULL ) bb_at_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'AT', bb.worst_post_cxt       , NULL ) bb_at_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'AT', bb.worst_post_cxt_qscore, NULL ) bb_at_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'CA', bb.total_qscore         , NULL ) bb_cref_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'CA', bb.worst_cxt            , NULL ) bb_cref_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'CA', bb.worst_cxt_qscore     , NULL ) bb_cref_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'CA', bb.worst_pre_cxt        , NULL ) bb_cref_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'CA', bb.worst_pre_cxt_qscore , NULL ) bb_cref_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'CA', bb.worst_post_cxt       , NULL ) bb_cref_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'CA', bb.worst_post_cxt_qscore, NULL ) bb_cref_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'CG', bb.total_qscore         , NULL ) bb_cg_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'CG', bb.worst_cxt            , NULL ) bb_cg_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'CG', bb.worst_cxt_qscore     , NULL ) bb_cg_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'CG', bb.worst_pre_cxt        , NULL ) bb_cg_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'CG', bb.worst_pre_cxt_qscore , NULL ) bb_cg_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'CG', bb.worst_post_cxt       , NULL ) bb_cg_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'CG', bb.worst_post_cxt_qscore, NULL ) bb_cg_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'CT', bb.total_qscore         , NULL ) bb_ct_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'CT', bb.worst_cxt            , NULL ) bb_ct_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'CT', bb.worst_cxt_qscore     , NULL ) bb_ct_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'CT', bb.worst_pre_cxt        , NULL ) bb_ct_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'CT', bb.worst_pre_cxt_qscore , NULL ) bb_ct_worstprecxt_qscor,
        decode(bb.ref_base||bb.alt_base, 'CT', bb.worst_post_cxt       , NULL ) bb_ct_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'CT', bb.worst_post_cxt_qscore, NULL ) bb_ct_worstpostcxt_scor,

        decode(bb.ref_base||bb.alt_base, 'GA', bb.total_qscore         , NULL ) bb_ga_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'GA', bb.worst_cxt            , NULL ) bb_ga_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'GA', bb.worst_cxt_qscore     , NULL ) bb_ga_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'GA', bb.worst_pre_cxt        , NULL ) bb_ga_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'GA', bb.worst_pre_cxt_qscore , NULL ) bb_ga_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'GA', bb.worst_post_cxt       , NULL ) bb_ga_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'GA', bb.worst_post_cxt_qscore, NULL ) bb_ga_worst_post_cxt_qscore,
    --    decode(bb.ref_base||bb.alt_base, 'AC', bb.artifact_name        , NULL ) ac_artifact_name

        decode(bb.ref_base||bb.alt_base, 'GC', bb.total_qscore         , NULL ) bb_gc_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'GC', bb.worst_cxt            , NULL ) bb_gc_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'GC', bb.worst_cxt_qscore     , NULL ) bb_gc_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'GC', bb.worst_pre_cxt        , NULL ) bb_gc_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'GC', bb.worst_pre_cxt_qscore , NULL ) bb_gc_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'GC', bb.worst_post_cxt       , NULL ) bb_gc_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'GC', bb.worst_post_cxt_qscore, NULL ) bb_gc_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'GT', bb.total_qscore         , NULL ) bb_gref_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'GT', bb.worst_cxt            , NULL ) bb_gref_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'GT', bb.worst_cxt_qscore     , NULL ) bb_gref_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'GT', bb.worst_pre_cxt        , NULL ) bb_gref_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'GT', bb.worst_pre_cxt_qscore , NULL ) bb_gref_worst_pre_cxt_score,
        decode(bb.ref_base||bb.alt_base, 'GT', bb.worst_post_cxt       , NULL ) bb_gref_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'GT', bb.worst_post_cxt_qscore, NULL ) bb_gref_worstpostcxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'TA', bb.total_qscore         , NULL ) bb_ta_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'TA', bb.worst_cxt            , NULL ) bb_ta_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'TA', bb.worst_cxt_qscore     , NULL ) bb_ta_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'TA', bb.worst_pre_cxt        , NULL ) bb_ta_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'TA', bb.worst_pre_cxt_qscore , NULL ) bb_ta_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'TA', bb.worst_post_cxt       , NULL ) bb_ta_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'TA', bb.worst_post_cxt_qscore, NULL ) bb_ta_worst_post_cxt_qscore,
    --    decode(bb.ref_base||bb.alt_base, 'AC', bb.artifact_name        , NULL ) ac_artifact_name

        decode(bb.ref_base||bb.alt_base, 'TC', bb.total_qscore         , NULL ) bb_tc_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'TC', bb.worst_cxt            , NULL ) bb_tc_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'TC', bb.worst_cxt_qscore     , NULL ) bb_tc_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'TC', bb.worst_pre_cxt        , NULL ) bb_tc_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'TC', bb.worst_pre_cxt_qscore , NULL ) bb_tc_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'TC', bb.worst_post_cxt       , NULL ) bb_tc_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'TC', bb.worst_post_cxt_qscore, NULL ) bb_tc_worst_post_cxt_qscore,

        decode(bb.ref_base||bb.alt_base, 'TG', bb.total_qscore         , NULL ) bb_tg_total_qscore,
        decode(bb.ref_base||bb.alt_base, 'TG', bb.worst_cxt            , NULL ) bb_tg_worst_cxt,
        decode(bb.ref_base||bb.alt_base, 'TG', bb.worst_cxt_qscore     , NULL ) bb_tg_worst_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'TG', bb.worst_pre_cxt        , NULL ) bb_tg_worst_pre_cxt,
        decode(bb.ref_base||bb.alt_base, 'TG', bb.worst_pre_cxt_qscore , NULL ) bb_tg_worst_pre_cxt_qscore,
        decode(bb.ref_base||bb.alt_base, 'TG', bb.worst_post_cxt       , NULL ) bb_tg_worst_post_cxt,
        decode(bb.ref_base||bb.alt_base, 'TG', bb.worst_post_cxt_qscore, NULL ) bb_tg_worst_post_cxt_qscore

    FROM agg 
    JOIN metrics.aggregation_bait_bias/*DB_LINK*/ bb ON bb.aggregation_id = agg.id
) 
GROUP BY id 
),
pread AS (
SELECT 
    --PK
    id,
    --
        max(pread_ac_total_qscore)          pread_ac_total_qscore,
        max(pread_ac_worst_cxt)             pread_ac_worst_cxt,
        max(pread_ac_worst_cxt_qscore)      pread_ac_worst_cxt_qscore,
        max(pread_ac_worst_pre_cxt)         pread_ac_worst_pre_cxt,
        max(pread_ac_worst_pre_cxt_qscore)  pread_ac_worst_pre_cxt_qscore,
        max(pread_ac_worst_post_cxt)        pread_ac_worst_post_cxt,
        max(pread_ac_worst_post_cxt_qscore) pread_ac_worst_post_cxt_qscore,

        max(pread_ag_total_qscore)          pread_ag_total_qscore,
        max(pread_ag_worst_cxt)             pread_ag_worst_cxt,
        max(pread_ag_worst_cxt_qscore)      pread_ag_worst_cxt_qscore,
        max(pread_ag_worst_pre_cxt)         pread_ag_worst_pre_cxt,
        max(pread_ag_worst_pre_cxt_qscore)  pread_ag_worst_pre_cxt_qscore,
        max(pread_ag_worst_post_cxt)        pread_ag_worst_post_cxt,
        max(pread_ag_worst_post_cxt_qscore) pread_ag_worst_post_cxt_qscore,

        max(pread_at_total_qscore)          pread_at_total_qscore,
        max(pread_at_worst_cxt)             pread_at_worst_cxt,
        max(pread_at_worst_cxt_qscore)      pread_at_worst_cxt_qscore,
        max(pread_at_worst_pre_cxt)         pread_at_worst_pre_cxt,
        max(pread_at_worst_pre_cxt_qscore)  pread_at_worst_pre_cxt_qscore,
        max(pread_at_worst_post_cxt)        pread_at_worst_post_cxt,
        max(pread_at_worst_post_cxt_qscore) pread_at_worst_post_cxt_qscore,

        max(pread_ca_total_qscore)          pread_ca_total_qscore,
        max(pread_ca_worst_cxt)             pread_ca_worst_cxt,
        max(pread_ca_worst_cxt_qscore)      pread_ca_worst_cxt_qscore,
        max(pread_ca_worst_pre_cxt)         pread_ca_worst_pre_cxt,
        max(pread_ca_worst_pre_cxt_qscore)  pread_ca_worst_pre_cxt_qscore,
        max(pread_ca_worst_post_cxt)        pread_ca_worst_post_cxt,
        max(pread_ca_worst_post_cxt_qscore) pread_ca_worst_post_cxt_qscore,

        max(pread_cg_total_qscore)          pread_cg_total_qscore,
        max(pread_cg_worst_cxt)             pread_cg_worst_cxt,
        max(pread_cg_worst_cxt_qscore)      pread_cg_worst_cxt_qscore,
        max(pread_cg_worst_pre_cxt)         pread_cg_worst_pre_cxt,
        max(pread_cg_worst_pre_cxt_qscore)  pread_cg_worst_pre_cxt_qscore,
        max(pread_cg_worst_post_cxt)        pread_cg_worst_post_cxt,
        max(pread_cg_worst_post_cxt_qscore) pread_cg_worst_post_cxt_qscore,

        max(pread_deamin_total_qscore)      pread_deamin_total_qscore,
        max(pread_deamin_worst_cxt)         pread_deamin_worst_cxt,
        max(pread_deamin_worst_cxt_qscore)  pread_deamin_worst_cxt_qscore,
        max(pread_deamin_worst_pre_cxt)     pread_deamin_worst_pre_cxt,
        max(pread_deamin_worstprecxt_qscor) pread_deamin_worstprecxt_qscor,
        max(pread_deamin_worst_post_cxt)    pread_deamin_worst_post_cxt,
        max(pread_deamin_worstpostcxt_scor) pread_deamin_worstpostcxt_scor,

        max(pread_ga_total_qscore)          pread_ga_total_qscore,
        max(pread_ga_worst_cxt)             pread_ga_worst_cxt,
        max(pread_ga_worst_cxt_qscore)      pread_ga_worst_cxt_qscore,
        max(pread_ga_worst_pre_cxt)         pread_ga_worst_pre_cxt,
        max(pread_ga_worst_pre_cxt_qscore)  pread_ga_worst_pre_cxt_qscore,
        max(pread_ga_worst_post_cxt)        pread_ga_worst_post_cxt,
        max(pread_ga_worst_post_cxt_qscore) pread_ga_worst_post_cxt_qscore,

        max(pread_gc_total_qscore)          pread_gc_total_qscore,
        max(pread_gc_worst_cxt)             pread_gc_worst_cxt,
        max(pread_gc_worst_cxt_qscore)      pread_gc_worst_cxt_qscore,
        max(pread_gc_worst_pre_cxt)         pread_gc_worst_pre_cxt,
        max(pread_gc_worst_pre_cxt_qscore)  pread_gc_worst_pre_cxt_qscore,
        max(pread_gc_worst_post_cxt)        pread_gc_worst_post_cxt,
        max(pread_gc_worst_post_cxt_qscore) pread_gc_worst_post_cxt_qscore,

        max(pread_oxog_total_qscore)        pread_oxog_total_qscore,
        max(pread_oxog_worst_cxt)           pread_oxog_worst_cxt,
        max(pread_oxog_worst_cxt_qscore)    pread_oxog_worst_cxt_qscore,
        max(pread_oxog_worst_pre_cxt)       pread_oxog_worst_pre_cxt,
        max(pread_oxog_worst_pre_cxt_score) pread_oxog_worst_pre_cxt_score,
        max(pread_oxog_worst_post_cxt)      pread_oxog_worst_post_cxt,
        max(pread_oxog_worstpostcxt_qscore) pread_oxog_worstpostcxt_qscore,

        max(pread_ta_total_qscore)          pread_ta_total_qscore,
        max(pread_ta_worst_cxt)             pread_ta_worst_cxt,
        max(pread_ta_worst_cxt_qscore)      pread_ta_worst_cxt_qscore,
        max(pread_ta_worst_pre_cxt)         pread_ta_worst_pre_cxt,
        max(pread_ta_worst_pre_cxt_qscore)  pread_ta_worst_pre_cxt_qscore,
        max(pread_ta_worst_post_cxt)        pread_ta_worst_post_cxt,
        max(pread_ta_worst_post_cxt_qscore) pread_ta_worst_post_cxt_qscore,

        max(pread_tc_total_qscore)          pread_tc_total_qscore,
        max(pread_tc_worst_cxt)             pread_tc_worst_cxt,
        max(pread_tc_worst_cxt_qscore)      pread_tc_worst_cxt_qscore,
        max(pread_tc_worst_pre_cxt)         pread_tc_worst_pre_cxt,
        max(pread_tc_worst_pre_cxt_qscore)  pread_tc_worst_pre_cxt_qscore,
        max(pread_tc_worst_post_cxt)        pread_tc_worst_post_cxt,
        max(pread_tc_worst_post_cxt_qscore) pread_tc_worst_post_cxt_qscore,

        max(pread_tg_total_qscore)          pread_tg_total_qscore,
        max(pread_tg_worst_cxt)             pread_tg_worst_cxt,
        max(pread_tg_worst_cxt_qscore)      pread_tg_worst_cxt_qscore,
        max(pread_tg_worst_pre_cxt)         pread_tg_worst_pre_cxt,
        max(pread_tg_worst_pre_cxt_qscore)  pread_tg_worst_pre_cxt_qscore,
        max(pread_tg_worst_post_cxt)        pread_tg_worst_post_cxt,
        max(pread_tg_worst_post_cxt_qscore) pread_tg_worst_post_cxt_qscore


FROM (
    SELECT 
        --PK
        agg.id,
        pread.ref_base||pread.alt_base ref_alt_base,
        --
        decode(pread.ref_base||pread.alt_base, 'AC', pread.total_qscore         , NULL ) pread_ac_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'AC', pread.worst_cxt            , NULL ) pread_ac_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'AC', pread.worst_cxt_qscore     , NULL ) pread_ac_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'AC', pread.worst_pre_cxt        , NULL ) pread_ac_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'AC', pread.worst_pre_cxt_qscore , NULL ) pread_ac_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'AC', pread.worst_post_cxt       , NULL ) pread_ac_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'AC', pread.worst_post_cxt_qscore, NULL ) pread_ac_worst_post_cxt_qscore,
    --    decode(pread.ref_base||pread.alt_base, 'AC', pread.artifact_name        , NULL ) ac_pread_artifact_name

        decode(pread.ref_base||pread.alt_base, 'AG', pread.total_qscore         , NULL ) pread_ag_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'AG', pread.worst_cxt            , NULL ) pread_ag_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'AG', pread.worst_cxt_qscore     , NULL ) pread_ag_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'AG', pread.worst_pre_cxt        , NULL ) pread_ag_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'AG', pread.worst_pre_cxt_qscore , NULL ) pread_ag_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'AG', pread.worst_post_cxt       , NULL ) pread_ag_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'AG', pread.worst_post_cxt_qscore, NULL ) pread_ag_worst_post_cxt_qscore,

        decode(pread.ref_base||pread.alt_base, 'AT', pread.total_qscore         , NULL ) pread_at_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'AT', pread.worst_cxt            , NULL ) pread_at_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'AT', pread.worst_cxt_qscore     , NULL ) pread_at_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'AT', pread.worst_pre_cxt        , NULL ) pread_at_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'AT', pread.worst_pre_cxt_qscore , NULL ) pread_at_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'AT', pread.worst_post_cxt       , NULL ) pread_at_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'AT', pread.worst_post_cxt_qscore, NULL ) pread_at_worst_post_cxt_qscore,

        decode(pread.ref_base||pread.alt_base, 'CA', pread.total_qscore         , NULL ) pread_ca_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'CA', pread.worst_cxt            , NULL ) pread_ca_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'CA', pread.worst_cxt_qscore     , NULL ) pread_ca_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'CA', pread.worst_pre_cxt        , NULL ) pread_ca_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'CA', pread.worst_pre_cxt_qscore , NULL ) pread_ca_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'CA', pread.worst_post_cxt       , NULL ) pread_ca_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'CA', pread.worst_post_cxt_qscore, NULL ) pread_ca_worst_post_cxt_qscore,
    --    decode(pread.ref_base||pread.alt_base, 'AC', pread.artifact_name        , NULL ) ac_artifact_name

        decode(pread.ref_base||pread.alt_base, 'CG', pread.total_qscore         , NULL ) pread_cg_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'CG', pread.worst_cxt            , NULL ) pread_cg_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'CG', pread.worst_cxt_qscore     , NULL ) pread_cg_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'CG', pread.worst_pre_cxt        , NULL ) pread_cg_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'CG', pread.worst_pre_cxt_qscore , NULL ) pread_cg_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'CG', pread.worst_post_cxt       , NULL ) pread_cg_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'CG', pread.worst_post_cxt_qscore, NULL ) pread_cg_worst_post_cxt_qscore,

        decode(pread.ref_base||pread.alt_base, 'CT', pread.total_qscore         , NULL ) pread_deamin_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'CT', pread.worst_cxt            , NULL ) pread_deamin_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'CT', pread.worst_cxt_qscore     , NULL ) pread_deamin_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'CT', pread.worst_pre_cxt        , NULL ) pread_deamin_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'CT', pread.worst_pre_cxt_qscore , NULL ) pread_deamin_worstprecxt_qscor,
        decode(pread.ref_base||pread.alt_base, 'CT', pread.worst_post_cxt       , NULL ) pread_deamin_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'CT', pread.worst_post_cxt_qscore, NULL ) pread_deamin_worstpostcxt_scor,

        decode(pread.ref_base||pread.alt_base, 'GA', pread.total_qscore         , NULL ) pread_ga_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'GA', pread.worst_cxt            , NULL ) pread_ga_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'GA', pread.worst_cxt_qscore     , NULL ) pread_ga_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'GA', pread.worst_pre_cxt        , NULL ) pread_ga_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'GA', pread.worst_pre_cxt_qscore , NULL ) pread_ga_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'GA', pread.worst_post_cxt       , NULL ) pread_ga_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'GA', pread.worst_post_cxt_qscore, NULL ) pread_ga_worst_post_cxt_qscore,
    --    decode(pread.ref_base||pread.alt_base, 'AC', pread.artifact_name        , NULL ) ac_artifact_name

        decode(pread.ref_base||pread.alt_base, 'GC', pread.total_qscore         , NULL ) pread_gc_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'GC', pread.worst_cxt            , NULL ) pread_gc_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'GC', pread.worst_cxt_qscore     , NULL ) pread_gc_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'GC', pread.worst_pre_cxt        , NULL ) pread_gc_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'GC', pread.worst_pre_cxt_qscore , NULL ) pread_gc_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'GC', pread.worst_post_cxt       , NULL ) pread_gc_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'GC', pread.worst_post_cxt_qscore, NULL ) pread_gc_worst_post_cxt_qscore,

        decode(pread.ref_base||pread.alt_base, 'GT', pread.total_qscore         , NULL ) pread_oxog_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'GT', pread.worst_cxt            , NULL ) pread_oxog_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'GT', pread.worst_cxt_qscore     , NULL ) pread_oxog_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'GT', pread.worst_pre_cxt        , NULL ) pread_oxog_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'GT', pread.worst_pre_cxt_qscore , NULL ) pread_oxog_worst_pre_cxt_score,
        decode(pread.ref_base||pread.alt_base, 'GT', pread.worst_post_cxt       , NULL ) pread_oxog_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'GT', pread.worst_post_cxt_qscore, NULL ) pread_oxog_worstpostcxt_qscore,

        decode(pread.ref_base||pread.alt_base, 'TA', pread.total_qscore         , NULL ) pread_ta_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'TA', pread.worst_cxt            , NULL ) pread_ta_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'TA', pread.worst_cxt_qscore     , NULL ) pread_ta_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'TA', pread.worst_pre_cxt        , NULL ) pread_ta_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'TA', pread.worst_pre_cxt_qscore , NULL ) pread_ta_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'TA', pread.worst_post_cxt       , NULL ) pread_ta_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'TA', pread.worst_post_cxt_qscore, NULL ) pread_ta_worst_post_cxt_qscore,
    --    decode(pread.ref_base||pread.alt_base, 'AC', pread.artifact_name        , NULL ) ac_artifact_name

        decode(pread.ref_base||pread.alt_base, 'TC', pread.total_qscore         , NULL ) pread_tc_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'TC', pread.worst_cxt            , NULL ) pread_tc_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'TC', pread.worst_cxt_qscore     , NULL ) pread_tc_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'TC', pread.worst_pre_cxt        , NULL ) pread_tc_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'TC', pread.worst_pre_cxt_qscore , NULL ) pread_tc_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'TC', pread.worst_post_cxt       , NULL ) pread_tc_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'TC', pread.worst_post_cxt_qscore, NULL ) pread_tc_worst_post_cxt_qscore,

        decode(pread.ref_base||pread.alt_base, 'TG', pread.total_qscore         , NULL ) pread_tg_total_qscore,
        decode(pread.ref_base||pread.alt_base, 'TG', pread.worst_cxt            , NULL ) pread_tg_worst_cxt,
        decode(pread.ref_base||pread.alt_base, 'TG', pread.worst_cxt_qscore     , NULL ) pread_tg_worst_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'TG', pread.worst_pre_cxt        , NULL ) pread_tg_worst_pre_cxt,
        decode(pread.ref_base||pread.alt_base, 'TG', pread.worst_pre_cxt_qscore , NULL ) pread_tg_worst_pre_cxt_qscore,
        decode(pread.ref_base||pread.alt_base, 'TG', pread.worst_post_cxt       , NULL ) pread_tg_worst_post_cxt,
        decode(pread.ref_base||pread.alt_base, 'TG', pread.worst_post_cxt_qscore, NULL ) pread_tg_worst_post_cxt_qscore

    FROM agg 
    JOIN metrics.aggregation_pre_adapter pread/*DB_LINK*/ ON pread.aggregation_id = agg.id
) 
GROUP BY id 
)

SELECT 
    --PK
    agg.project,
    agg.SAMPLE, 
    agg.LIBRARY,
    agg.data_type,
    agg.version,
    --
    
    agg.id                  analysis_id, 
    agg.workflow_end_date   analysis_end,

--Bait Bias
bb_ac_total_qscore,
bb_ac_worst_cxt,
bb_ac_worst_cxt_qscore,
bb_ac_worst_pre_cxt,
bb_ac_worst_pre_cxt_qscore,
bb_ac_worst_post_cxt,
bb_ac_worst_post_cxt_qscore,

bb_ag_total_qscore,
bb_ag_worst_cxt,
bb_ag_worst_cxt_qscore,
bb_ag_worst_pre_cxt,
bb_ag_worst_pre_cxt_qscore,
bb_ag_worst_post_cxt,
bb_ag_worst_post_cxt_qscore,

bb_at_total_qscore,
bb_at_worst_cxt,
bb_at_worst_cxt_qscore,
bb_at_worst_pre_cxt,
bb_at_worst_pre_cxt_qscore,
bb_at_worst_post_cxt,
bb_at_worst_post_cxt_qscore,

bb_cref_total_qscore,
bb_cref_worst_cxt,
bb_cref_worst_cxt_qscore,
bb_cref_worst_pre_cxt,
bb_cref_worst_pre_cxt_qscore,
bb_cref_worst_post_cxt,
bb_cref_worst_post_cxt_qscore,

bb_cg_total_qscore,
bb_cg_worst_cxt,
bb_cg_worst_cxt_qscore,
bb_cg_worst_pre_cxt,
bb_cg_worst_pre_cxt_qscore,
bb_cg_worst_post_cxt,
bb_cg_worst_post_cxt_qscore,

bb_ct_total_qscore,
bb_ct_worst_cxt,
bb_ct_worst_cxt_qscore,
bb_ct_worst_pre_cxt,
bb_ct_worstprecxt_qscor,
bb_ct_worst_post_cxt,
bb_ct_worstpostcxt_scor,

bb_ga_total_qscore,
bb_ga_worst_cxt,
bb_ga_worst_cxt_qscore,
bb_ga_worst_pre_cxt,
bb_ga_worst_pre_cxt_qscore,
bb_ga_worst_post_cxt,
bb_ga_worst_post_cxt_qscore,

bb_gc_total_qscore,
bb_gc_worst_cxt,
bb_gc_worst_cxt_qscore,
bb_gc_worst_pre_cxt,
bb_gc_worst_pre_cxt_qscore,
bb_gc_worst_post_cxt,
bb_gc_worst_post_cxt_qscore,

bb_gref_total_qscore,
bb_gref_worst_cxt,
bb_gref_worst_cxt_qscore,
bb_gref_worst_pre_cxt,
bb_gref_worst_pre_cxt_score,
bb_gref_worst_post_cxt,
bb_gref_worstpostcxt_qscore,

bb_ta_total_qscore,
bb_ta_worst_cxt,
bb_ta_worst_cxt_qscore,
bb_ta_worst_pre_cxt,
bb_ta_worst_pre_cxt_qscore,
bb_ta_worst_post_cxt,
bb_ta_worst_post_cxt_qscore,

bb_tc_total_qscore,
bb_tc_worst_cxt,
bb_tc_worst_cxt_qscore,
bb_tc_worst_pre_cxt,
bb_tc_worst_pre_cxt_qscore,
bb_tc_worst_post_cxt,
bb_tc_worst_post_cxt_qscore,

bb_tg_total_qscore,
bb_tg_worst_cxt,
bb_tg_worst_cxt_qscore,
bb_tg_worst_pre_cxt,
bb_tg_worst_pre_cxt_qscore,
bb_tg_worst_post_cxt,
bb_tg_worst_post_cxt_qscore,

pread_ac_total_qscore,
pread_ac_worst_cxt,
pread_ac_worst_cxt_qscore,
pread_ac_worst_pre_cxt,
pread_ac_worst_pre_cxt_qscore,
pread_ac_worst_post_cxt,
pread_ac_worst_post_cxt_qscore,

pread_ag_total_qscore,
pread_ag_worst_cxt,
pread_ag_worst_cxt_qscore,
pread_ag_worst_pre_cxt,
pread_ag_worst_pre_cxt_qscore,
pread_ag_worst_post_cxt,
pread_ag_worst_post_cxt_qscore,

pread_at_total_qscore,
pread_at_worst_cxt,
pread_at_worst_cxt_qscore,
pread_at_worst_pre_cxt,
pread_at_worst_pre_cxt_qscore,
pread_at_worst_post_cxt,
pread_at_worst_post_cxt_qscore,

pread_ca_total_qscore,
pread_ca_worst_cxt,
pread_ca_worst_cxt_qscore,
pread_ca_worst_pre_cxt,
pread_ca_worst_pre_cxt_qscore,
pread_ca_worst_post_cxt,
pread_ca_worst_post_cxt_qscore,

pread_cg_total_qscore,
pread_cg_worst_cxt,
pread_cg_worst_cxt_qscore,
pread_cg_worst_pre_cxt,
pread_cg_worst_pre_cxt_qscore,
pread_cg_worst_post_cxt,
pread_cg_worst_post_cxt_qscore,

pread_deamin_total_qscore,
pread_deamin_worst_cxt,
pread_deamin_worst_cxt_qscore,
pread_deamin_worst_pre_cxt,
pread_deamin_worstprecxt_qscor,
pread_deamin_worst_post_cxt,
pread_deamin_worstpostcxt_scor,

pread_ga_total_qscore,
pread_ga_worst_cxt,
pread_ga_worst_cxt_qscore,
pread_ga_worst_pre_cxt,
pread_ga_worst_pre_cxt_qscore,
pread_ga_worst_post_cxt,
pread_ga_worst_post_cxt_qscore,

pread_gc_total_qscore,
pread_gc_worst_cxt,
pread_gc_worst_cxt_qscore,
pread_gc_worst_pre_cxt,
pread_gc_worst_pre_cxt_qscore,
pread_gc_worst_post_cxt,
pread_gc_worst_post_cxt_qscore,

pread_oxog_total_qscore,
pread_oxog_worst_cxt,
pread_oxog_worst_cxt_qscore,
pread_oxog_worst_pre_cxt,
pread_oxog_worst_pre_cxt_score,
pread_oxog_worst_post_cxt,
pread_oxog_worstpostcxt_qscore,

pread_ta_total_qscore,
pread_ta_worst_cxt,
pread_ta_worst_cxt_qscore,
pread_ta_worst_pre_cxt,
pread_ta_worst_pre_cxt_qscore,
pread_ta_worst_post_cxt,
pread_ta_worst_post_cxt_qscore,

pread_tc_total_qscore,
pread_tc_worst_cxt,
pread_tc_worst_cxt_qscore,
pread_tc_worst_pre_cxt,
pread_tc_worst_pre_cxt_qscore,
pread_tc_worst_post_cxt,
pread_tc_worst_post_cxt_qscore,

pread_tg_total_qscore,
pread_tg_worst_cxt,
pread_tg_worst_cxt_qscore,
pread_tg_worst_pre_cxt,
pread_tg_worst_pre_cxt_qscore,
pread_tg_worst_post_cxt,
pread_tg_worst_post_cxt_qscore,

--'Regular' AS Source,
--SYSDATE AS TIMESTAMP 
 agg.is_latest    
FROM agg 
LEFT JOIN pread         ON pread.id = agg.id 
LEFT JOIN bait_bias bb  ON    bb.id = agg.id 
) DELTA 
ON (laaf.analysis_id = DELTA.analysis_id )  -- AND laaf.source = /*SOURCE*/

WHEN NOT MATCHED THEN INSERT VALUES (

    DELTA.project,
    DELTA.SAMPLE, 
    DELTA.LIBRARY,
    DELTA.data_type,
    DELTA.version,
    DELTA.analysis_id, 
    DELTA.analysis_end,

--Bait Bias
DELTA.bb_ac_total_qscore,
DELTA.bb_ac_worst_cxt,
DELTA.bb_ac_worst_cxt_qscore,
DELTA.bb_ac_worst_pre_cxt,
DELTA.bb_ac_worst_pre_cxt_qscore,
DELTA.bb_ac_worst_post_cxt,
DELTA.bb_ac_worst_post_cxt_qscore,

DELTA.bb_ag_total_qscore,
DELTA.bb_ag_worst_cxt,
DELTA.bb_ag_worst_cxt_qscore,
DELTA.bb_ag_worst_pre_cxt,
DELTA.bb_ag_worst_pre_cxt_qscore,
DELTA.bb_ag_worst_post_cxt,
DELTA.bb_ag_worst_post_cxt_qscore,

DELTA.bb_at_total_qscore,
DELTA.bb_at_worst_cxt,
DELTA.bb_at_worst_cxt_qscore,
DELTA.bb_at_worst_pre_cxt,
DELTA.bb_at_worst_pre_cxt_qscore,
DELTA.bb_at_worst_post_cxt,
DELTA.bb_at_worst_post_cxt_qscore,

DELTA.bb_cref_total_qscore,
DELTA.bb_cref_worst_cxt,
DELTA.bb_cref_worst_cxt_qscore,
DELTA.bb_cref_worst_pre_cxt,
DELTA.bb_cref_worst_pre_cxt_qscore,
DELTA.bb_cref_worst_post_cxt,
DELTA.bb_cref_worst_post_cxt_qscore,

DELTA.bb_cg_total_qscore,
DELTA.bb_cg_worst_cxt,
DELTA.bb_cg_worst_cxt_qscore,
DELTA.bb_cg_worst_pre_cxt,
DELTA.bb_cg_worst_pre_cxt_qscore,
DELTA.bb_cg_worst_post_cxt,
DELTA.bb_cg_worst_post_cxt_qscore,

DELTA.bb_ct_total_qscore,
DELTA.bb_ct_worst_cxt,
DELTA.bb_ct_worst_cxt_qscore,
DELTA.bb_ct_worst_pre_cxt,
DELTA.bb_ct_worstprecxt_qscor,
DELTA.bb_ct_worst_post_cxt,
DELTA.bb_ct_worstpostcxt_scor,

DELTA.bb_ga_total_qscore,
DELTA.bb_ga_worst_cxt,
DELTA.bb_ga_worst_cxt_qscore,
DELTA.bb_ga_worst_pre_cxt,
DELTA.bb_ga_worst_pre_cxt_qscore,
DELTA.bb_ga_worst_post_cxt,
DELTA.bb_ga_worst_post_cxt_qscore,

DELTA.bb_gc_total_qscore,
DELTA.bb_gc_worst_cxt,
DELTA.bb_gc_worst_cxt_qscore,
DELTA.bb_gc_worst_pre_cxt,
DELTA.bb_gc_worst_pre_cxt_qscore,
DELTA.bb_gc_worst_post_cxt,
DELTA.bb_gc_worst_post_cxt_qscore,

DELTA.bb_gref_total_qscore,
DELTA.bb_gref_worst_cxt,
DELTA.bb_gref_worst_cxt_qscore,
DELTA.bb_gref_worst_pre_cxt,
DELTA.bb_gref_worst_pre_cxt_score,
DELTA.bb_gref_worst_post_cxt,
DELTA.bb_gref_worstpostcxt_qscore,

DELTA.bb_ta_total_qscore,
DELTA.bb_ta_worst_cxt,
DELTA.bb_ta_worst_cxt_qscore,
DELTA.bb_ta_worst_pre_cxt,
DELTA.bb_ta_worst_pre_cxt_qscore,
DELTA.bb_ta_worst_post_cxt,
DELTA.bb_ta_worst_post_cxt_qscore,

DELTA.bb_tc_total_qscore,
DELTA.bb_tc_worst_cxt,
DELTA.bb_tc_worst_cxt_qscore,
DELTA.bb_tc_worst_pre_cxt,
DELTA.bb_tc_worst_pre_cxt_qscore,
DELTA.bb_tc_worst_post_cxt,
DELTA.bb_tc_worst_post_cxt_qscore,

DELTA.bb_tg_total_qscore,
DELTA.bb_tg_worst_cxt,
DELTA.bb_tg_worst_cxt_qscore,
DELTA.bb_tg_worst_pre_cxt,
DELTA.bb_tg_worst_pre_cxt_qscore,
DELTA.bb_tg_worst_post_cxt,
DELTA.bb_tg_worst_post_cxt_qscore,

DELTA.pread_ac_total_qscore,
DELTA.pread_ac_worst_cxt,
DELTA.pread_ac_worst_cxt_qscore,
DELTA.pread_ac_worst_pre_cxt,
DELTA.pread_ac_worst_pre_cxt_qscore,
DELTA.pread_ac_worst_post_cxt,
DELTA.pread_ac_worst_post_cxt_qscore,

DELTA.pread_ag_total_qscore,
DELTA.pread_ag_worst_cxt,
DELTA.pread_ag_worst_cxt_qscore,
DELTA.pread_ag_worst_pre_cxt,
DELTA.pread_ag_worst_pre_cxt_qscore,
DELTA.pread_ag_worst_post_cxt,
DELTA.pread_ag_worst_post_cxt_qscore,

DELTA.pread_at_total_qscore,
DELTA.pread_at_worst_cxt,
DELTA.pread_at_worst_cxt_qscore,
DELTA.pread_at_worst_pre_cxt,
DELTA.pread_at_worst_pre_cxt_qscore,
DELTA.pread_at_worst_post_cxt,
DELTA.pread_at_worst_post_cxt_qscore,

DELTA.pread_ca_total_qscore,
DELTA.pread_ca_worst_cxt,
DELTA.pread_ca_worst_cxt_qscore,
DELTA.pread_ca_worst_pre_cxt,
DELTA.pread_ca_worst_pre_cxt_qscore,
DELTA.pread_ca_worst_post_cxt,
DELTA.pread_ca_worst_post_cxt_qscore,

DELTA.pread_cg_total_qscore,
DELTA.pread_cg_worst_cxt,
DELTA.pread_cg_worst_cxt_qscore,
DELTA.pread_cg_worst_pre_cxt,
DELTA.pread_cg_worst_pre_cxt_qscore,
DELTA.pread_cg_worst_post_cxt,
DELTA.pread_cg_worst_post_cxt_qscore,

DELTA.pread_deamin_total_qscore,
DELTA.pread_deamin_worst_cxt,
DELTA.pread_deamin_worst_cxt_qscore,
DELTA.pread_deamin_worst_pre_cxt,
DELTA.pread_deamin_worstprecxt_qscor,
DELTA.pread_deamin_worst_post_cxt,
DELTA.pread_deamin_worstpostcxt_scor,

DELTA.pread_ga_total_qscore,
DELTA.pread_ga_worst_cxt,
DELTA.pread_ga_worst_cxt_qscore,
DELTA.pread_ga_worst_pre_cxt,
DELTA.pread_ga_worst_pre_cxt_qscore,
DELTA.pread_ga_worst_post_cxt,
DELTA.pread_ga_worst_post_cxt_qscore,

DELTA.pread_gc_total_qscore,
DELTA.pread_gc_worst_cxt,
DELTA.pread_gc_worst_cxt_qscore,
DELTA.pread_gc_worst_pre_cxt,
DELTA.pread_gc_worst_pre_cxt_qscore,
DELTA.pread_gc_worst_post_cxt,
DELTA.pread_gc_worst_post_cxt_qscore,

DELTA.pread_oxog_total_qscore,
DELTA.pread_oxog_worst_cxt,
DELTA.pread_oxog_worst_cxt_qscore,
DELTA.pread_oxog_worst_pre_cxt,
DELTA.pread_oxog_worst_pre_cxt_score,
DELTA.pread_oxog_worst_post_cxt,
DELTA.pread_oxog_worstpostcxt_qscore,

DELTA.pread_ta_total_qscore,
DELTA.pread_ta_worst_cxt,
DELTA.pread_ta_worst_cxt_qscore,
DELTA.pread_ta_worst_pre_cxt,
DELTA.pread_ta_worst_pre_cxt_qscore,
DELTA.pread_ta_worst_post_cxt,
DELTA.pread_ta_worst_post_cxt_qscore,

DELTA.pread_tc_total_qscore,
DELTA.pread_tc_worst_cxt,
DELTA.pread_tc_worst_cxt_qscore,
DELTA.pread_tc_worst_pre_cxt,
DELTA.pread_tc_worst_pre_cxt_qscore,
DELTA.pread_tc_worst_post_cxt,
DELTA.pread_tc_worst_post_cxt_qscore,

DELTA.pread_tg_total_qscore,
DELTA.pread_tg_worst_cxt,
DELTA.pread_tg_worst_cxt_qscore,
DELTA.pread_tg_worst_pre_cxt,
DELTA.pread_tg_worst_pre_cxt_qscore,
DELTA.pread_tg_worst_post_cxt,
DELTA.pread_tg_worst_post_cxt_qscore,
/*SOURCE*/,
SYSDATE --AS TIMESTAMP 
)
WHERE DELTA.is_latest<>0  -- =1

WHEN MATCHED THEN UPDATE
SET
laaf.bb_ac_total_qscore             = DELTA.bb_ac_total_qscore,
laaf.bb_ac_worst_cxt                = DELTA.bb_ac_worst_cxt,
laaf.bb_ac_worst_cxt_qscore         = DELTA.bb_ac_worst_cxt_qscore,
laaf.bb_ac_worst_pre_cxt            = DELTA.bb_ac_worst_pre_cxt,
laaf.bb_ac_worst_pre_cxt_qscore     = DELTA.bb_ac_worst_pre_cxt_qscore,
laaf.bb_ac_worst_post_cxt           = DELTA.bb_ac_worst_post_cxt,
laaf.bb_ac_worst_post_cxt_qscore    = DELTA.bb_ac_worst_post_cxt_qscore,

laaf.bb_ag_total_qscore             = DELTA.bb_ag_total_qscore,
laaf.bb_ag_worst_cxt                = DELTA.bb_ag_worst_cxt,
laaf.bb_ag_worst_cxt_qscore         = DELTA.bb_ag_worst_cxt_qscore,
laaf.bb_ag_worst_pre_cxt            = DELTA.bb_ag_worst_pre_cxt,
laaf.bb_ag_worst_pre_cxt_qscore     = DELTA.bb_ag_worst_pre_cxt_qscore,
laaf.bb_ag_worst_post_cxt           = DELTA.bb_ag_worst_post_cxt,
laaf.bb_ag_worst_post_cxt_qscore    = DELTA.bb_ag_worst_post_cxt_qscore,

laaf.bb_at_total_qscore             = DELTA.bb_at_total_qscore,
laaf.bb_at_worst_cxt                = DELTA.bb_at_worst_cxt,
laaf.bb_at_worst_cxt_qscore         = DELTA.bb_at_worst_cxt_qscore,
laaf.bb_at_worst_pre_cxt            = DELTA.bb_at_worst_pre_cxt,
laaf.bb_at_worst_pre_cxt_qscore     = DELTA.bb_at_worst_pre_cxt_qscore,
laaf.bb_at_worst_post_cxt           = DELTA.bb_at_worst_post_cxt,
laaf.bb_at_worst_post_cxt_qscore    = DELTA.bb_at_worst_post_cxt_qscore,

laaf.bb_cref_total_qscore           = DELTA.bb_cref_total_qscore,
laaf.bb_cref_worst_cxt              = DELTA.bb_cref_worst_cxt,
laaf.bb_cref_worst_cxt_qscore       = DELTA.bb_cref_worst_cxt_qscore,
laaf.bb_cref_worst_pre_cxt          = DELTA.bb_cref_worst_pre_cxt,
laaf.bb_cref_worst_pre_cxt_qscore   = DELTA.bb_cref_worst_pre_cxt_qscore,
laaf.bb_cref_worst_post_cxt         = DELTA.bb_cref_worst_post_cxt,
laaf.bb_cref_worst_post_cxt_qscore  = DELTA.bb_cref_worst_post_cxt_qscore,

laaf.bb_cg_total_qscore             = DELTA.bb_cg_total_qscore,
laaf.bb_cg_worst_cxt                = DELTA.bb_cg_worst_cxt,
laaf.bb_cg_worst_cxt_qscore         = DELTA.bb_cg_worst_cxt_qscore,
laaf.bb_cg_worst_pre_cxt            = DELTA.bb_cg_worst_pre_cxt,
laaf.bb_cg_worst_pre_cxt_qscore     = DELTA.bb_cg_worst_pre_cxt_qscore,
laaf.bb_cg_worst_post_cxt           = DELTA.bb_cg_worst_post_cxt,
laaf.bb_cg_worst_post_cxt_qscore    = DELTA.bb_cg_worst_post_cxt_qscore,

laaf.bb_ct_total_qscore             = DELTA.bb_ct_total_qscore,
laaf.bb_ct_worst_cxt                = DELTA.bb_ct_worst_cxt,
laaf.bb_ct_worst_cxt_qscore         = DELTA.bb_ct_worst_cxt_qscore,
laaf.bb_ct_worst_pre_cxt            = DELTA.bb_ct_worst_pre_cxt,
laaf.bb_ct_worstprecxt_qscor        = DELTA.bb_ct_worstprecxt_qscor,
laaf.bb_ct_worst_post_cxt           = DELTA.bb_ct_worst_post_cxt,
laaf.bb_ct_worstpostcxt_scor        = DELTA.bb_ct_worstpostcxt_scor,

laaf.bb_ga_total_qscore             = DELTA.bb_ga_total_qscore,
laaf.bb_ga_worst_cxt                = DELTA.bb_ga_worst_cxt,
laaf.bb_ga_worst_cxt_qscore         = DELTA.bb_ga_worst_cxt_qscore,
laaf.bb_ga_worst_pre_cxt            = DELTA.bb_ga_worst_pre_cxt,
laaf.bb_ga_worst_pre_cxt_qscore     = DELTA.bb_ga_worst_pre_cxt_qscore,
laaf.bb_ga_worst_post_cxt           = DELTA.bb_ga_worst_post_cxt,
laaf.bb_ga_worst_post_cxt_qscore    = DELTA.bb_ga_worst_post_cxt_qscore,

laaf.bb_gc_total_qscore             = DELTA.bb_gc_total_qscore,
laaf.bb_gc_worst_cxt                = DELTA.bb_gc_worst_cxt,
laaf.bb_gc_worst_cxt_qscore         = DELTA.bb_gc_worst_cxt_qscore,
laaf.bb_gc_worst_pre_cxt            = DELTA.bb_gc_worst_pre_cxt,
laaf.bb_gc_worst_pre_cxt_qscore     = DELTA.bb_gc_worst_pre_cxt_qscore,
laaf.bb_gc_worst_post_cxt           = DELTA.bb_gc_worst_post_cxt,
laaf.bb_gc_worst_post_cxt_qscore    = DELTA.bb_gc_worst_post_cxt_qscore,

laaf.bb_gref_total_qscore           = DELTA.bb_gref_total_qscore,
laaf.bb_gref_worst_cxt              = DELTA.bb_gref_worst_cxt,
laaf.bb_gref_worst_cxt_qscore       = DELTA.bb_gref_worst_cxt_qscore,
laaf.bb_gref_worst_pre_cxt          = DELTA.bb_gref_worst_pre_cxt,
laaf.bb_gref_worst_pre_cxt_score    = DELTA.bb_gref_worst_pre_cxt_score,
laaf.bb_gref_worst_post_cxt         = DELTA.bb_gref_worst_post_cxt,
laaf.bb_gref_worstpostcxt_qscore    = DELTA.bb_gref_worstpostcxt_qscore,

laaf.bb_ta_total_qscore             = DELTA.bb_ta_total_qscore,
laaf.bb_ta_worst_cxt                = DELTA.bb_ta_worst_cxt,
laaf.bb_ta_worst_cxt_qscore         = DELTA.bb_ta_worst_cxt_qscore,
laaf.bb_ta_worst_pre_cxt            = DELTA.bb_ta_worst_pre_cxt,
laaf.bb_ta_worst_pre_cxt_qscore     = DELTA.bb_ta_worst_pre_cxt_qscore,
laaf.bb_ta_worst_post_cxt           = DELTA.bb_ta_worst_post_cxt,
laaf.bb_ta_worst_post_cxt_qscore    = DELTA.bb_ta_worst_post_cxt_qscore,

laaf.bb_tc_total_qscore             = DELTA.bb_tc_total_qscore,
laaf.bb_tc_worst_cxt                = DELTA.bb_tc_worst_cxt,
laaf.bb_tc_worst_cxt_qscore         = DELTA.bb_tc_worst_cxt_qscore,
laaf.bb_tc_worst_pre_cxt            = DELTA.bb_tc_worst_pre_cxt,
laaf.bb_tc_worst_pre_cxt_qscore     = DELTA.bb_tc_worst_pre_cxt_qscore,
laaf.bb_tc_worst_post_cxt           = DELTA.bb_tc_worst_post_cxt,
laaf.bb_tc_worst_post_cxt_qscore    = DELTA.bb_tc_worst_post_cxt_qscore,

laaf.bb_tg_total_qscore             = DELTA.bb_tg_total_qscore,
laaf.bb_tg_worst_cxt                = DELTA.bb_tg_worst_cxt,
laaf.bb_tg_worst_cxt_qscore         = DELTA.bb_tg_worst_cxt_qscore,
laaf.bb_tg_worst_pre_cxt            = DELTA.bb_tg_worst_pre_cxt,
laaf.bb_tg_worst_pre_cxt_qscore     = DELTA.bb_tg_worst_pre_cxt_qscore,
laaf.bb_tg_worst_post_cxt           = DELTA.bb_tg_worst_post_cxt,
laaf.bb_tg_worst_post_cxt_qscore    = DELTA.bb_tg_worst_post_cxt_qscore,

laaf.pread_ac_total_qscore          = DELTA.pread_ac_total_qscore,
laaf.pread_ac_worst_cxt             = DELTA.pread_ac_worst_cxt,
laaf.pread_ac_worst_cxt_qscore      = DELTA.pread_ac_worst_cxt_qscore,
laaf.pread_ac_worst_pre_cxt         = DELTA.pread_ac_worst_pre_cxt,
laaf.pread_ac_worst_pre_cxt_qscore  = DELTA.pread_ac_worst_pre_cxt_qscore,
laaf.pread_ac_worst_post_cxt        = DELTA.pread_ac_worst_post_cxt,
laaf.pread_ac_worst_post_cxt_qscore = DELTA.pread_ac_worst_post_cxt_qscore,

laaf.pread_ag_total_qscore          = DELTA.pread_ag_total_qscore,
laaf.pread_ag_worst_cxt             = DELTA.pread_ag_worst_cxt,
laaf.pread_ag_worst_cxt_qscore      = DELTA.pread_ag_worst_cxt_qscore,
laaf.pread_ag_worst_pre_cxt         = DELTA.pread_ag_worst_pre_cxt,
laaf.pread_ag_worst_pre_cxt_qscore  = DELTA.pread_ag_worst_pre_cxt_qscore,
laaf.pread_ag_worst_post_cxt        = DELTA.pread_ag_worst_post_cxt,
laaf.pread_ag_worst_post_cxt_qscore = DELTA.pread_ag_worst_post_cxt_qscore,

laaf.pread_at_total_qscore          = DELTA.pread_at_total_qscore,
laaf.pread_at_worst_cxt             = DELTA.pread_at_worst_cxt,
laaf.pread_at_worst_cxt_qscore      = DELTA.pread_at_worst_cxt_qscore,
laaf.pread_at_worst_pre_cxt         = DELTA.pread_at_worst_pre_cxt,
laaf.pread_at_worst_pre_cxt_qscore  = DELTA.pread_at_worst_pre_cxt_qscore,
laaf.pread_at_worst_post_cxt        = DELTA.pread_at_worst_post_cxt,
laaf.pread_at_worst_post_cxt_qscore = DELTA.pread_at_worst_post_cxt_qscore,

laaf.pread_ca_total_qscore          = DELTA.pread_ca_total_qscore,
laaf.pread_ca_worst_cxt             = DELTA.pread_ca_worst_cxt,
laaf.pread_ca_worst_cxt_qscore      = DELTA.pread_ca_worst_cxt_qscore,
laaf.pread_ca_worst_pre_cxt         = DELTA.pread_ca_worst_pre_cxt,
laaf.pread_ca_worst_pre_cxt_qscore  = DELTA.pread_ca_worst_pre_cxt_qscore,
laaf.pread_ca_worst_post_cxt        = DELTA.pread_ca_worst_post_cxt,
laaf.pread_ca_worst_post_cxt_qscore = DELTA.pread_ca_worst_post_cxt_qscore,

laaf.pread_cg_total_qscore          = DELTA.pread_cg_total_qscore,
laaf.pread_cg_worst_cxt             = DELTA.pread_cg_worst_cxt,
laaf.pread_cg_worst_cxt_qscore      = DELTA.pread_cg_worst_cxt_qscore,
laaf.pread_cg_worst_pre_cxt         = DELTA.pread_cg_worst_pre_cxt,
laaf.pread_cg_worst_pre_cxt_qscore  = DELTA.pread_cg_worst_pre_cxt_qscore,
laaf.pread_cg_worst_post_cxt        = DELTA.pread_cg_worst_post_cxt,
laaf.pread_cg_worst_post_cxt_qscore = DELTA.pread_cg_worst_post_cxt_qscore,

laaf.pread_deamin_total_qscore      = DELTA.pread_deamin_total_qscore,
laaf.pread_deamin_worst_cxt         = DELTA.pread_deamin_worst_cxt,
laaf.pread_deamin_worst_cxt_qscore  = DELTA.pread_deamin_worst_cxt_qscore,
laaf.pread_deamin_worst_pre_cxt     = DELTA.pread_deamin_worst_pre_cxt,
laaf.pread_deamin_worstprecxt_qscor = DELTA.pread_deamin_worstprecxt_qscor,
laaf.pread_deamin_worst_post_cxt    = DELTA.pread_deamin_worst_post_cxt,
laaf.pread_deamin_worstpostcxt_scor = DELTA.pread_deamin_worstpostcxt_scor,

laaf.pread_ga_total_qscore          = DELTA.pread_ga_total_qscore,
laaf.pread_ga_worst_cxt             = DELTA.pread_ga_worst_cxt,
laaf.pread_ga_worst_cxt_qscore      = DELTA.pread_ga_worst_cxt_qscore,
laaf.pread_ga_worst_pre_cxt         = DELTA.pread_ga_worst_pre_cxt,
laaf.pread_ga_worst_pre_cxt_qscore  = DELTA.pread_ga_worst_pre_cxt_qscore,
laaf.pread_ga_worst_post_cxt        = DELTA.pread_ga_worst_post_cxt,
laaf.pread_ga_worst_post_cxt_qscore = DELTA.pread_ga_worst_post_cxt_qscore,

laaf.pread_gc_total_qscore          = DELTA.pread_gc_total_qscore,
laaf.pread_gc_worst_cxt             = DELTA.pread_gc_worst_cxt,
laaf.pread_gc_worst_cxt_qscore      = DELTA.pread_gc_worst_cxt_qscore,
laaf.pread_gc_worst_pre_cxt         = DELTA.pread_gc_worst_pre_cxt,
laaf.pread_gc_worst_pre_cxt_qscore  = DELTA.pread_gc_worst_pre_cxt_qscore,
laaf.pread_gc_worst_post_cxt        = DELTA.pread_gc_worst_post_cxt,
laaf.pread_gc_worst_post_cxt_qscore = DELTA.pread_gc_worst_post_cxt_qscore,

laaf.pread_oxog_total_qscore        = DELTA.pread_oxog_total_qscore,
laaf.pread_oxog_worst_cxt           = DELTA.pread_oxog_worst_cxt,
laaf.pread_oxog_worst_cxt_qscore    = DELTA.pread_oxog_worst_cxt_qscore,
laaf.pread_oxog_worst_pre_cxt       = DELTA.pread_oxog_worst_pre_cxt,
laaf.pread_oxog_worst_pre_cxt_score = DELTA.pread_oxog_worst_pre_cxt_score,
laaf.pread_oxog_worst_post_cxt      = DELTA.pread_oxog_worst_post_cxt,
laaf.pread_oxog_worstpostcxt_qscore = DELTA.pread_oxog_worstpostcxt_qscore,

laaf.pread_ta_total_qscore          = DELTA.pread_ta_total_qscore,
laaf.pread_ta_worst_cxt             = DELTA.pread_ta_worst_cxt,
laaf.pread_ta_worst_cxt_qscore      = DELTA.pread_ta_worst_cxt_qscore,
laaf.pread_ta_worst_pre_cxt         = DELTA.pread_ta_worst_pre_cxt,
laaf.pread_ta_worst_pre_cxt_qscore  = DELTA.pread_ta_worst_pre_cxt_qscore,
laaf.pread_ta_worst_post_cxt        = DELTA.pread_ta_worst_post_cxt,
laaf.pread_ta_worst_post_cxt_qscore = DELTA.pread_ta_worst_post_cxt_qscore,

laaf.pread_tc_total_qscore          = DELTA.pread_tc_total_qscore,
laaf.pread_tc_worst_cxt             = DELTA.pread_tc_worst_cxt,
laaf.pread_tc_worst_cxt_qscore      = DELTA.pread_tc_worst_cxt_qscore,
laaf.pread_tc_worst_pre_cxt         = DELTA.pread_tc_worst_pre_cxt,
laaf.pread_tc_worst_pre_cxt_qscore  = DELTA.pread_tc_worst_pre_cxt_qscore,
laaf.pread_tc_worst_post_cxt        = DELTA.pread_tc_worst_post_cxt,
laaf.pread_tc_worst_post_cxt_qscore = DELTA.pread_tc_worst_post_cxt_qscore,

laaf.pread_tg_total_qscore          = DELTA.pread_tg_total_qscore,
laaf.pread_tg_worst_cxt             = decode(DELTA.is_latest, 0, 'DEL', DELTA.pread_tg_worst_cxt),
laaf.pread_tg_worst_cxt_qscore      = DELTA.pread_tg_worst_cxt_qscore,
laaf.pread_tg_worst_pre_cxt         = DELTA.pread_tg_worst_pre_cxt,
laaf.pread_tg_worst_pre_cxt_qscore  = DELTA.pread_tg_worst_pre_cxt_qscore,
laaf.pread_tg_worst_post_cxt        = DELTA.pread_tg_worst_post_cxt,
laaf.pread_tg_worst_post_cxt_qscore = DELTA.pread_tg_worst_post_cxt_qscore,
laaf.TIMESTAMP                      = SYSDATE --AS TIMESTAMP 

DELETE WHERE laaf.pread_tg_worst_cxt = 'DEL'
;
END IF;
END;
-- artifact metrics if any: END