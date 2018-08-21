--STATEMENT
MERGE INTO COGNOS.pooling_calculator pc  
USING (
WITH
rgm AS (
    SELECT DISTINCT rgm.flowcell_barcode, rgm.setup_read_structure, rgm.actual_read_structure, rgm.run_name
, t.rgmd_timestamp
    FROM COGNOS.slxre_readgroup_metadata rgm, 
        COGNOS.slxre_rghqs_targets t
    WHERE t.flowcell_barcode = rgm.flowcell_barcode
        AND rgmd_timestamp >= /*DELTA_START*/
        AND rgmd_timestamp < /*DELTA_END*/      
		and rgm.source = /*SOURCE*/	
    ),

index_data AS (
    SELECT
        ba.flowcell_barcode, ba.lane,
        ba.molecular_barcode_name,
        ba.metrics_type,
        --
        ba.run_name,
        ba.library_name,
        rgm.setup_read_structure,
        rgm.actual_read_structure,
        decode(rgm.setup_read_structure, '8B', bbm.total_reads, '8B8B', bbm.total_reads, bm.total_reads) matching_reads,
        decode(rgm.setup_read_structure, '8B', bbm.pf_reads, '8B8B', bbm.pf_reads, bm.pf_reads) matching_reads_pf
        , bbm.total_reads bbm_total_reads
    FROM metrics.basecalling_analysis/*DBLINK*/ ba
    JOIN metrics.basecalling_barcode_metrics/*DBLINK*/ bbm ON bbm.basecalling_analysis_id = ba.id
    JOIN metrics.basecalling_metrics/*DBLINK*/ bm          ON  bm.basecalling_analysis_id = ba.id
    JOIN rgm                                     ON        rgm.flowcell_barcode = ba.flowcell_barcode
    WHERE
        ba.metrics_type IN ('Indexed', 'Unindexed', 'Unmatched')

),

lane_data AS (
    SELECT
        flowcell_barcode, lane,

        sum(   decode(metrics_type, 'Unmatched', 0, matching_reads)) matching_reads,
        sum(   decode(metrics_type, 'Unmatched', 0, matching_reads_pf)) matching_reads_pf,
        sum(   decode(metrics_type, 'Unmatched', matching_reads, 0)) lane_no_match_reads,
        sum(   decode(metrics_type, 'Unmatched', matching_reads_pf, 0)) lane_no_match_reads_pf,

        count(DISTINCT decode(id.metrics_type, 'Unmatched', NULL, id.molecular_barcode_name)) lane_num_barcodes,
        sum(decode(id.metrics_type, 'Unmatched', NULL,   decode(bbm_total_reads , NULL, 0, 0, 0, 1) ) ) lane_num_barcodes_matched

    FROM index_data id
    GROUP BY flowcell_barcode, lane
),
cd AS (
    SELECT
        --PK
        ba.flowcell_barcode,
        ba.lane,
        --
        lm.cluster_density
    FROM metrics.basecalling_analysis/*DBLINK*/ ba
    JOIN metrics.basecalling_lane_metrics/*DBLINK*/ lm ON lm.basecalling_analysis_id = ba.id
    WHERE ba.metrics_type = 'Lane'
),
slpp AS (
    SELECT rgmd.sequenced_library,  count(DISTINCT rgapp.plex_pond) solexa_library_plex_ponds
    FROM COGNOS.slxre2_rg_ancestry rgapp
    JOIN rgm ON rgm.flowcell_barcode = rgapp.flowcell_barcode
    JOIN COGNOS.slxre_readgroup_metadata rgmd ON
        rgmd.flowcell_barcode = rgapp.flowcell_barcode AND
        rgmd.lane = rgapp.lane AND
        rgmd.molecular_indexing_scheme = rgapp.molecular_indexing_scheme
    GROUP BY rgmd.sequenced_library
),
mb_seq AS (
    SELECT
    a.NAME mb_name, 
    CASE 
        WHEN a.p5_seq IS NULL THEN p7_seq
        WHEN p7_seq IS NULL THEN p5_seq
        ELSE p5_seq||p7_seq
    END mb_sequence

    FROM (
        SELECT mis.NAME, 
            max(decode(mip.position_hint, 'ILLUMINA_P5', mi.SEQUENCE, NULL )) p5_seq,
            max(decode(mip.position_hint, 'ILLUMINA_P7', mi.SEQUENCE, NULL )) p7_seq
    --        max(decode(mip.position_hint, 'ILLUMINA_IS1', mi.SEQUENCE, NULL )) is1_seq

        FROM seq20.molecular_indexing_scheme mis ,
            seq20.molecular_index_position mip ,
            seq20.molecular_index mi 
        WHERE 
            (mis.NAME LIKE 'tagged%' OR 
            mis.NAME LIKE '%P5%'  OR 
            mis.NAME LIKE '%P7%' 
            )
            AND mis.NAME NOT LIKE '%IS1%'
            AND mip.scheme_id = mis.id
            AND mip.index_id = mi.id
        GROUP BY mis.NAME 
    ) a 
),

metadata AS (
SELECT  DISTINCT -- to prevent duplicate rows due to pdo sample being in multiple positions in PDO
    --PK
    rgmd.flowcell_barcode,
    rgmd.lane,
    rgmd.molecular_indexing_scheme,
    --
    pdos.on_risk,    
    pdos.risk_types,
--    max( pdos.on_risk)    on_risk,    -- to cover when sample on multiple positions in PDO has multiple On risk flags or reasons. This had not happen since 2014.
--    max( pdos.risk_types) risk_types,
    rgmd.sequenced_library solexa_library,
    rgmd.sequenced_library_creation solexa_library_creation_date,
	l.TYPE sequenced_library_type,    
    rgmd.initiative,
    rgmd.analysis_type,
    rgmd.lcset,
    rgmd.lsid,
    rgmd.loading_concentration,
    rgmd.is_greedy,
	rgmd.is_pool_test,
    rgmd.product,
    rgmd.product_family,
    rgmd.product_order_key,
    rgmd.product_order_sample,
    jira.TYPE lcset_type,
    jira.topoffs lcset_topoff,
    jira.priority lcset_priority,
	rgmd.source,
    rgmd.sample_id ,
    rgmd.reference_sequence,
    mb_seq.mb_sequence molecular_barcode_sequence,
    rgmd.project seq_project,
    rgmd.research_project_id,
    rgmd.collaborator_sample_id

FROM rgm
JOIN COGNOS.slxre_readgroup_metadata rgmd 				ON rgmd.flowcell_barcode = rgm.flowcell_barcode
LEFT JOIN COGNOS.slxre_library l 						ON l.NAME = rgmd.sequenced_library
LEFT JOIN mercurydwro.product_order pdo 		ON pdo.jira_ticket_key = rgmd.product_order_key
LEFT JOIN mercurydwro.product_order_sample pdos ON pdos.product_order_id = pdo.product_order_id AND pdos.sample_name = rgmd.product_order_sample
LEFT JOIN reporting.loj_issue_lcset@loj_link.seqbldr jira   ON jira.KEY = rgmd.lcset
JOIN mb_seq                                     ON mb_seq.mb_name = rgmd.molecular_indexing_scheme
--LEFT JOIN slxre2_pond_plexity pplx                          ON              pplx.pond = rgmd.LIBRARY 
/*GROUP BY rgmd.flowcell_barcode, rgmd.lane, rgmd.molecular_indexing_scheme,
    rgmd.sequenced_library ,rgmd.sequenced_library_creation ,l.TYPE,
    rgmd.initiative, rgmd.analysis_type,
    rgmd.lcset, rgmd.lsid, rgmd.loading_concentration, rgmd.is_greedy, rgmd.is_pool_test,
    rgmd.product, rgmd.product_family, rgmd.product_order_key, rgmd.product_order_sample, jira.TYPE , jira.topoffs, jira.priority,
    rgmd.source
*/
),

pc_core AS (
    SELECT
        -- PK
        ru.run_name,
        id.lane,
        metadata.molecular_indexing_scheme molecular_barcode_name,
        --
        id.flowcell_barcode,
        metadata.molecular_barcode_sequence,
        metadata.solexa_library,
        metadata.solexa_library_creation_date,
        ru.run_end_date,
        ru.instrument_model run_instrument_model,
        ru.instrument run_instrument,
        metadata.initiative,
        '' fc_lane_comments,
        metadata.analysis_type,
        id.setup_read_structure,
        id.actual_read_structure,
        NULL min_setup_cycles,
        NULL max_setup_cycles,

        id.matching_reads,
        id.matching_reads_pf,
        id.library_name picard_library,

        ld.lane_no_match_reads,
        ld.lane_no_match_reads_pf,
        ld.lane_num_barcodes,
        ld.lane_num_barcodes_matched,

        ld.matching_reads       lane_matching_reads,
        ld.matching_reads_pf    lane_matching_reads_pf,

        rga.catch   enriched_catch_name,
        slpp.solexa_library_plex_ponds,
        rga.pond    enriched_pond_name,
        rga.plex_pond plex_pond,
        rga.spri_concentrated spri_library,
        metadata.lcset,
        cd.cluster_density,
        metadata.loading_concentration,
        metadata.lsid,
        metadata.on_risk,
        metadata.risk_types,
        rga.nextera nextera_library,
        rga.nexome_catch,

        metadata.product,
        metadata.product_family,
        metadata.product_order_key,
        metadata.product_order_sample,
        metadata.lcset_type,
        metadata.lcset_topoff,
        metadata.source,
--		ru.hiseq_pool_test
        decode(id.flowcell_barcode, 'HYTTTCCXX', 1, metadata.is_pool_test) hiseq_pool_test,
		decode(metadata.sequenced_library_type, 'Pooled Normalized Library', metadata.solexa_library, rga.pooled_normalized)  pooled_normalized,
		decode(metadata.sequenced_library_type, 'Normalized Library',        metadata.solexa_library, rga.normalized)         normalized ,
		metadata.lcset_priority,
        rga.pcr_plus_norm_pond norm_pond,
        COALESCE(rga.pond, rga.pcr_free_pond, rga.pcr_plus_pond, rga.pcr_plus_norm_pond, rga.nextera) pond,
        COALESCE(rga.catch, rga.nexome_catch) catch ,
        pplx.first_pool,
        pplx.plexity pond_plexity,
		rga.cdna_library,
        metadata.sample_id plated_sample_id ,
		metadata.reference_sequence,
		rga.norm_pond_2,
        metadata.seq_project,
        metadata.research_project_id,
        metadata.collaborator_sample_id

    FROM index_data id
    JOIN lane_data ld ON ld.flowcell_barcode = id.flowcell_barcode AND ld.lane = id.lane
    JOIN metadata ON
        metadata.flowcell_barcode=id.flowcell_barcode
        AND metadata.lane=id.lane
        --?? rework it so that Unmatched analysis joins to RGMD readgroup with a valid barcode
        AND CASE
            WHEN
                metadata.molecular_indexing_scheme=decode(id.molecular_barcode_name , 'N/A', 'NULL',id.molecular_barcode_name)
                OR metadata.is_greedy=1
            THEN 1
            ELSE 0
           END =1
   JOIN COGNOS.slxre2_organic_run ru ON ru.run_name = id.run_name
   LEFT JOIN COGNOS.slxre2_rg_ancestry rga ON
        rga.flowcell_barcode = metadata.flowcell_barcode
        AND rga.lane = metadata.lane
        AND rga.molecular_indexing_scheme = metadata.molecular_indexing_scheme
   LEFT JOIN slpp ON slpp.sequenced_library = metadata.solexa_library
   LEFT JOIN cd ON
        cd.flowcell_barcode = id.flowcell_barcode
        AND cd.lane = id.lane
   LEFT JOIN COGNOS.slxre2_pond_plexity pplx ON pplx.pond = COALESCE(rga.pond, rga.pcr_free_pond, rga.pcr_plus_pond, rga.pcr_plus_norm_pond, id.library_name)

    WHERE
        id.metrics_type IN ('Indexed', 'Unindexed')

)

SELECT
            -- PK: pc.flowcell_barcode, pc.lane, pc.molecular_barcode_name
            0 run_id,
            pc_core.lane,
            pc_core.molecular_barcode_name,
            pc_core.flowcell_barcode,
            --
            pc_core.molecular_barcode_sequence,
            pc_core.solexa_library,
            pc_core.solexa_library_creation_date,
            pc_core.run_end_date,
            pc_core.run_instrument_model,
            pc_core.run_instrument,
            pc_core.initiative,
            pc_core.fc_lane_comments,
            pc_core.analysis_type,
            pc_core.setup_read_structure,
            pc_core.actual_read_structure,
            pc_core.min_setup_cycles,
            pc_core.max_setup_cycles,
            pc_core.matching_reads,
            pc_core.matching_reads_pf,
            pc_core.picard_library,
            pc_core.lane_no_match_reads,
            pc_core.lane_no_match_reads_pf,
            pc_core.lane_num_barcodes,
            pc_core.lane_num_barcodes_matched,
            pc_core.lane_matching_reads,
            pc_core.lane_matching_reads_pf,
            pc_core.enriched_catch_name,
            pc_core.solexa_library_plex_ponds,
            pc_core.enriched_pond_name,
            pc_core.plex_pond,
            pc_core.spri_library,
            pc_core.lcset,
            pc_core.lsid,
            pc_core.on_risk,
            pc_core.risk_types,
            pc_core.cluster_density,
            pc_core.loading_concentration,
            catch.well catch_well,
            catch.quant_value catch_pico_quant,
            pond.well pond_well,
            pond.quant_value pond_pico_quant,
            pc_core.nextera_library,
            pc_core.product,
            pc_core.product_family,
            pc_core.product_order_key,
            pc_core.product_order_sample,
            pc_core.lcset_type,
            pc_core.lcset_topoff,
            pc_core.source,
			pc_core.hiseq_pool_test,
            pc_core.pooled_normalized,
            pc_core.normalized ,
			pc_core.lcset_priority,
--            pond_qpcr.concentration pond_qpcr_quant,
            pond_qpcr.quant_value pond_qpcr_quant,
			pc_core.norm_pond,
			pc_core.pond,
			pc_core.catch,
            pc_core.first_pool,
            pc_core.pond_plexity,
            cdna.well cdna_well,
            cdna.quant_value cdna_pico_quant,
			pc_core.reference_sequence,
			pc_core.norm_pond_2,
            pc_core.seq_project,
            pc_core.research_project_id,
            pc_core.collaborator_sample_id

        FROM
            pc_core
            LEFT JOIN COGNOS.sample_qc_metrics catch ON 
                catch.content_name = pc_core.catch 
                AND (catch.lsid = pc_core.lsid OR catch.plated_sample_id = pc_core.plated_sample_id)
                AND catch.quant_type = 'Catch Pico'
            LEFT JOIN COGNOS.sample_qc_metrics pond ON 
                pond.content_name = pc_core.pond 
                AND (pond.lsid = pc_core.lsid OR pond.plated_sample_id = pc_core.plated_sample_id )
                AND pond.quant_type = 'Pond Pico'
            LEFT JOIN COGNOS.sample_qc_metrics cdna ON
                cdna.content_name = pc_core.cdna_library
                AND cdna.lsid = pc_core.lsid 
                AND cdna.quant_type = 'cDNA Enriched Pico'
--            LEFT JOIN slxre_library_qpcr_value pond_qpcr ON pond_qpcr.library_name = pc_core.picard_library
            LEFT JOIN COGNOS.slxre2_library_qpcr pond_qpcr ON pond_qpcr.library_name = nvl(pc_core.norm_pond, pc_core.pond)

/*        FROM
            pc_core,
            sample_qc_metrics catch,
            sample_qc_metrics pond,
			sample_qc_metrics cdna,
            slxre_library_qpcr_value pond_qpcr

        WHERE
        -- this will not get the QC metrics for topoff LCSETs because the QC exists for the original LCSET but the newer read groups are under the newer LCSET
		-- NVL needed to capture pico for Nexome workflow
            nvl(pc_core.enriched_catch_name,pc_core.nexome_catch) = catch.content_name (+)
            AND pc_core.lsid  = catch.lsid(+)
            AND pc_core.lcset = catch.lcset(+)
            AND nvl(catch.quant_type , 'Catch Pico') = 'Catch Pico'

            AND  nvl(pc_core.enriched_pond_name,pc_core.nextera_library) = pond.content_name(+)
            AND pc_core.lsid  = pond.lsid(+)
            AND pc_core.lcset = pond.lcset(+)
            AND nvl(pond.quant_type , 'Pond Pico') = 'Pond Pico'

            AND pc_core.cdna_library = cdna.content_name(+)
            AND pc_core.lsid  = cdna.lsid(+)
            AND pc_core.lcset = cdna.lcset(+)
            AND nvl(cdna.quant_type , 'Enriched cDNA Pico') = 'Enriched cDNA Pico'
			
			AND pc_core.picard_library = pond_qpcr.library_name(+)
*/			
) DELTA
ON (
    DELTA.flowcell_barcode = pc.flowcell_barcode
    AND DELTA.lane = pc.lane
    AND DELTA.molecular_barcode_name = pc.molecular_barcode_name
    )

WHEN NOT MATCHED THEN
INSERT VALUES (
    DELTA.run_id,
    DELTA.lane,
    DELTA.molecular_barcode_name,
    DELTA.flowcell_barcode,
    DELTA.molecular_barcode_sequence,
    DELTA.solexa_library,
    DELTA.solexa_library_creation_date,
    DELTA.run_end_date,
    DELTA.run_instrument_model,
    DELTA.run_instrument,
    DELTA.initiative,
    DELTA.fc_lane_comments,
    DELTA.analysis_type,
    DELTA.setup_read_structure,
    DELTA.actual_read_structure,
    DELTA.min_setup_cycles,
    DELTA.max_setup_cycles,
    DELTA.matching_reads,
    DELTA.matching_reads_pf,
    DELTA.picard_library,
    DELTA.lane_no_match_reads,
    DELTA.lane_no_match_reads_pf,
    DELTA.lane_num_barcodes,
    DELTA.lane_num_barcodes_matched,
    DELTA.lane_matching_reads,
    DELTA.lane_matching_reads_pf,
    DELTA.enriched_catch_name,
    DELTA.solexa_library_plex_ponds,
    DELTA.enriched_pond_name,
    DELTA.plex_pond,
    DELTA.spri_library,
    DELTA.lcset,
    DELTA.cluster_density,
    DELTA.loading_concentration,
    DELTA.catch_well,
    DELTA.catch_pico_quant,
    DELTA.pond_well,
    DELTA.pond_pico_quant,
    DELTA.on_risk,
    DELTA.risk_types,
    DELTA.nextera_library,
    DELTA.product,
    DELTA.product_family,
    DELTA.product_order_key,
    DELTA.product_order_sample,
    DELTA.lcset_type,
    DELTA.lcset_topoff,
    DELTA.source,
	DELTA.hiseq_pool_test,
    DELTA.pooled_normalized,
	DELTA.normalized ,
	DELTA.lcset_priority,
	DELTA.pond_qpcr_quant,
	DELTA.norm_pond,
	DELTA.pond,
	DELTA.catch,
    DELTA.first_pool,
    DELTA.pond_plexity,
    DELTA.cdna_well,
    DELTA.cdna_pico_quant,
	DELTA.reference_sequence,
	DELTA.norm_pond_2,
    DELTA.seq_project,
    DELTA.research_project_id,
    DELTA.collaborator_sample_id

)

WHEN MATCHED THEN
UPDATE SET
    pc.molecular_barcode_sequence   = DELTA.molecular_barcode_sequence,
    pc.solexa_library               = DELTA.solexa_library,
    pc.solexa_library_creation_date = DELTA.solexa_library_creation_date,
    pc.run_end_date                 = DELTA.run_end_date,
    pc.run_instrument_model         = DELTA.run_instrument_model,
    pc.run_instrument               = DELTA.run_instrument,
    pc.initiative                   = DELTA.initiative,
    pc.fc_lane_comments             = DELTA.fc_lane_comments,
    pc.analysis_type                = DELTA.analysis_type,
    pc.setup_read_structure         = DELTA.setup_read_structure,
    pc.actual_read_structure        = DELTA.actual_read_structure,
	pc.hiseq_pool_test				= DELTA.hiseq_pool_test,
    pc.min_setup_cycles             = DELTA.min_setup_cycles,
    pc.max_setup_cycles             = DELTA.max_setup_cycles,
    pc.matching_reads               = DELTA.matching_reads,
    pc.matching_reads_pf            = DELTA.matching_reads_pf,
    pc.picard_library               = DELTA.picard_library,
    pc.lane_no_match_reads          = DELTA.lane_no_match_reads,
    pc.lane_no_match_reads_pf       = DELTA.lane_no_match_reads_pf,
    pc.lane_num_barcodes            = DELTA.lane_num_barcodes,
    pc.lane_num_barcodes_matched    = DELTA.lane_num_barcodes_matched,
    pc.lane_matching_reads          = DELTA.lane_matching_reads,
    pc.lane_matching_reads_pf       = DELTA.lane_matching_reads_pf,
    pc.enriched_catch_name          = DELTA.enriched_catch_name,
    pc.solexa_library_plex_ponds    = DELTA.solexa_library_plex_ponds,
    pc.enriched_pond_name           = DELTA.enriched_pond_name,
    pc.plex_pond                    = DELTA.plex_pond,
    pc.spri_library                 = DELTA.spri_library,
    pc.lcset                        = DELTA.lcset,
    pc.cluster_density              = DELTA.cluster_density,
    pc.loading_concentration        = DELTA.loading_concentration,
    pc.catch_well                   = DELTA.catch_well,
    pc.catch_pico_quant             = DELTA.catch_pico_quant,
    pc.pond_well                    = DELTA.pond_well,
    pc.pond_pico_quant              = DELTA.pond_pico_quant,
    pc.on_risk                      = DELTA.on_risk,
    pc.risk_types                   = DELTA.risk_types,
    pc.nextera_library              = DELTA.nextera_library,
    pc.product                      = DELTA.product,
    pc.product_family               = DELTA.product_family,
    pc.product_order_key            = DELTA.product_order_key,
    pc.product_order_sample         = DELTA.product_order_sample,
    pc.lcset_type                   = DELTA.lcset_type,
    pc.lcset_topoff                 = DELTA.lcset_topoff,
	pc.lcset_priority				= DELTA.lcset_priority,
    pc.pooled_normalized            = DELTA.pooled_normalized,
	pc.normalized                   = DELTA.normalized ,
	pc.pond_qpcr_quant				= DELTA.pond_qpcr_quant,
	pc.norm_pond					= DELTA.norm_pond,
	pc.norm_pond_2  				= DELTA.norm_pond_2,
	pc.pond							= DELTA.pond,
	pc.catch						= DELTA.catch,
	pc.first_pool					= DELTA.first_pool,
	pc.pond_plexity					= DELTA.pond_plexity,
    pc.cdna_well                    = DELTA.cdna_well,
    pc.cdna_pico_quant              = DELTA.cdna_pico_quant,
    pc.reference_sequence           = DELTA.reference_sequence,
    pc.seq_project                  = DELTA.seq_project,
    pc.research_project_id          = DELTA.research_project_id,
    pc.collaborator_sample_id       = DELTA.collaborator_sample_id

	
;

