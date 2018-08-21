--STATEMENT
DELETE FROM COGNOS.target
WHERE sessionid=userenv('SESSIONID');

--STATEMENT
INSERT INTO COGNOS.target(SESSIONID, STRING_FIELD1, STRING_FIELD2)
-- samples presented in pdo_star in non-terminal status
  SELECT userenv('SESSIONID'), a.pdo_name,a.pdo_sample_id
  FROM COGNOS.pdo_star5 a
  WHERE
--    a.pdo_created_date<'1-Nov-2013'
    a.sample_is_billed='F'
    AND a.sample_delivery_status<>'ABANDONED'

    UNION
-- PDOs transitioned to Completed status 
	SELECT userenv('SESSIONID'), a.pdo_name, a.pdo_sample_id
	FROM COGNOS.pdo_star5 a ,
		(SELECT DISTINCT a.pdo_name
		FROM COGNOS.pdo_star5_aux a,
      COGNOS.mercurydw_pdo_samples mp
		WHERE a.pdo_name = mp.pdo_name(+)
			AND a.pdo_sample = mp.pdo_sample_id(+)
			AND a.sample_position = mp.sample_position(+)
			AND a.pdo_status = 'Submitted'
			AND nvl(mp.pdo_status,'Completed') = 'Completed'  -- some Abandoned samples were removed from Mercury as in PDO-10627, SM- CNZYB, SM-CNZY8
		) pdo_t
	WHERE a.pdo_name = pdo_t.pdo_name      

		UNION
  -- samples not presented yet in pdo_star
  SELECT DISTINCT userenv('SESSIONID'), pdos.pdo_name, pdos.pdo_sample_id
  FROM COGNOS.mercurydw_pdo_samples pdos
    JOIN COGNOS.pdo_star_interesting_products sip ON sip.product_part_number=pdos.product_part_number
    LEFT JOIN COGNOS.pdo_star5 ps ON ps.pdo_name=pdos.pdo_name AND ps.pdo_sample_id=pdos.pdo_sample_id
  WHERE
--    pdos.pdo_created_date>='1-nov-2013'
    pdos.pdo_name IS NOT NULL
    AND (ps.pdo_name IS NULL
         OR pdos.pdo_created_date>=/*DELTA_START*/ -- Limited full refresh
    )
  UNION
  SELECT userenv('SESSIONID'), pdo_name, pdo_sample_id FROM COGNOS.pdo_star_special_refreshes

  UNION 
    SELECT userenv('SESSIONID'), a.pdo_name,a.pdo_sample_id
    FROM COGNOS.pdo_star5 a
    LEFT JOIN COGNOS.billing_ledger b ON
        b.pdo_number = a.pdo_name             AND 
        b.sample_name = a.pdo_sample_id          AND 
        b.sample_position = a.sample_position AND 
        b.price_item_type <>'ADD_ON_PRICE_ITEM' AND 
        b.billing_session_id<>7599 -- ugly hack: RPT-3625
    WHERE
        a.sample_is_billed='T'
        AND a.sample_delivery_status<>'ABANDONED'
        AND a.pdo_name NOT IN ('PDO-1952', 'PDO-6087')  -- old PDOs with GSSR and a Solexa library as PDO sample
    GROUP BY 
        a.pdo_name, a.pdo_sample_id
    HAVING MAX(b.billed_date) IS NULL    
;

--STATEMENT
DELETE FROM COGNOS.pdo_star_special_refreshes
WHERE (pdo_name, pdo_sample_id) IN (SELECT STRING_FIELD1, STRING_FIELD2 FROM COGNOS.target WHERE sessionid=userenv('SESSIONID'))
;

--STATEMENT
DELETE FROM COGNOS.pdo_star5
WHERE (pdo_name,pdo_sample_ID) IN (SELECT STRING_FIELD1, STRING_FIELD2 FROM COGNOS.target WHERE sessionid=userenv('SESSIONID'))
;

--STATEMENT
INSERT INTO COGNOS.pdo_star5 tr
  WITH
      pdos AS (
        SELECT
          pdos0.*,
          -1+row_number() over(PARTITION BY pdo_name, pdo_sample_id ORDER BY sample_position) relative_sample_position

        FROM
          (SELECT
             --PK
             mp.pdo_name,
             mp.pdo_sample_id,
             mp.sample_position,
             plated_sample.plated_sample_id bsp_sample_id,
             --

             mp.pdo_created_date,
             mp.pdo_title,
             mp.pdo_owner,
             mp.pdo_quote_id,
             mp.research_project,
             mp.research_project_key,
             mp.product_name,
             mp.product_family_name,

             mp.sample_delivery_status,
             mp.sample_is_on_risk,
             mp.sample_is_billed,
             mp.sample_quote_id,
             mp.product_part_number,

             plated_sample.plated_sample_lsid        bsp_sample_lsid,
             plated_sample.plating_task_completed_on bsp_export_date,
             plated_sample.requestor                 bsp_requestor,
             NULL                                    bsp_work_request_name,
             sa.collaborator_sample_id               bsp_collaborator_sample_id,
             sa.collaborator_pt_id                   bsp_collaborator_pt_id,
             sa.o_material_type_name                 bsp_original_material_type,
             sa.root_material_type                   bsp_root_material_type,

             -1+dense_rank() over (PARTITION BY mp.pdo_name, mp.pdo_sample_id ORDER BY sample_position) position_rank,
             count(DISTINCT sample_position) over (PARTITION BY mp.pdo_name, mp.pdo_sample_id) position_count,
             -1+dense_rank() over (PARTITION BY mp.pdo_name, mp.pdo_sample_id ORDER BY plated_sample.plated_sample_id) aliquot_rank

           FROM COGNOS.target
             JOIN COGNOS.mercurydw_pdo_samples mp ON mp.pdo_name=target.STRING_FIELD1 AND mp.pdo_sample_id=target.STRING_FIELD2
             LEFT JOIN COGNOS.bsp_plated_samples plated_sample ON plated_sample.pdo=mp.pdo_name AND plated_sample.pdo_sample_id=mp.pdo_sample_id AND plated_sample.work_request_item_type  in ('SEQ_PLATING_REQUEST', 'Mercury Seq Import')
             LEFT JOIN analytics.bsp_sample sa ON sa.sample_barcode = mp.pdo_sample_id

           WHERE
             target.sessionid=userenv('SESSIONID')
             AND mp.pdo_status NOT IN ('Abandoned','Draft','Pending')
          ) pdos0

        -- demultiplexing
        WHERE mod(aliquot_rank, position_count) = position_rank
    ),

      pdo_sample_lsid         AS (SELECT DISTINCT pdo_name,pdo_sample_id,bsp_sample_lsid,bsp_collaborator_sample_id FROM pdos),
      pdo_sample_positions    AS (SELECT pdo_name, pdo_sample_id, count(sample_position) positions FROM pdos GROUP BY pdo_name, pdo_sample_id),

      rghqs AS (
        SELECT
          rghqs0.*,
          rownum rownumber,
          row_number() over(PARTITION BY pdo,flowcell_barcode ORDER BY run_date) run_rank

        FROM
          (SELECT /*+ INDEX(h) */
             DISTINCT
             -- PK
             rgmd.product_order_key pdo,
             rgmd.lsid,
             --
             rgmd.rgmd_id,
             rgmd.gssr_barcodes gssr_id,
             rgmd.sample_id rgmd_aliquot,
             rgmd.collaborator_sample_id,
             rgmd.organism_scientific_name,
             rgmd.project project_name,
             rgmd.run_fraction,
             rgmd.data_type,
             rgmd.source,
             rgmd.product_order_sample,

             rgmd.run_date,
             ru.run_end_date,
             decode(ru.read_length, NULL, 1, 0) is_index_only,
             rgmd.flowcell_barcode,
             rgmd.lane,
             rgmd.molecular_indexing_scheme,
             rgmd.LIBRARY,
             fpv2_lod,
             bc_pf_reads,
             bc_total_reads,
             bc_total_pf_bases,
             decode(bl.flowcell_barcode, NULL, NULL, 'BlacklistedByPicard') issues,
             rgmd.root_sample,
             rgmd.sample_type,
             rgmd.collaborator_participant_id,
             CASE
             WHEN SUBSTR(ru.flowcell_barcode,1, 1)='A'   THEN rgmd.run_fraction*1
             WHEN SUBSTR(ru.flowcell_barcode, -3, 1)='D' THEN rgmd.run_fraction*2
             ELSE                                                 rgmd.run_fraction*8
             END lane_fraction,
             count(DISTINCT CASE WHEN nvl(rgmd.setup_read_structure, rgmd.actual_read_structure) NOT  IN ('8B', '8B8B') THEN rgmd.rgmd_id else NULL END )
             over (PARTITION BY rgmd.product_order_key , rgmd.product_order_sample) seq_rg,
             count(DISTINCT CASE WHEN bl.flowcell_barcode IS NOT NULL AND nvl(rgmd.setup_read_structure, rgmd.actual_read_structure) NOT  IN ('8B', '8B8B') THEN rgmd.rgmd_id ELSE null END)
             over (PARTITION BY rgmd.product_order_key , rgmd.product_order_sample) blacklisted_rg

           FROM pdo_sample_lsid
             JOIN COGNOS.slxre_readgroup_metadata rgmd ON
                                                         rgmd.product_order_key=pdo_sample_lsid.pdo_name AND
                                                         CASE
                                                         WHEN (rgmd.product_order_sample IS NOT NULL AND rgmd.product_order_sample=pdo_sample_lsid.pdo_sample_id )
                                                              OR rgmd.lsid =pdo_sample_lsid.bsp_sample_lsid
                                                              OR rgmd.gssr_barcodes =pdo_sample_lsid.pdo_sample_id
                                                              OR rgmd.collaborator_sample_id =pdo_sample_lsid.bsp_collaborator_sample_id
                                                           THEN 1
                                                         ELSE 0
                                                         END =1

             JOIN COGNOS.slxre2_rghqs h ON h.rgmd_id=rgmd.rgmd_id
             JOIN COGNOS.slxre2_organic_run ru ON ru.run_name=rgmd.run_name

             -- lookup blacklisted read groups from BC agg
             -- PK: flowcell_barcode, lane, molecular_barcode_name, blacklist_reason
             LEFT JOIN seq20.sequence_data_blacklist bl ON
                                                          bl.flowcell_barcode = rgmd.flowcell_barcode AND
                                                          bl.lane = rgmd.lane AND
                                                          CASE WHEN bl.LIBRARY = rgmd.LIBRARY OR rgmd.is_greedy = 1 THEN 1 ELSE 0 END = 1 AND
                                                          bl.whitelisted_on IS NULL
          ) rghqs0
    ),

      agg_reg AS (
        SELECT /*+ MATERIALIZE */
          -- PK
          arg.flowcell_barcode,
          arg.lane,
          arg.LIBRARY_name,
          --
          arg.molecular_barcode_name,
          a.id,
          a.workflow_end_date,

          a.project,
          a.SAMPLE,
          a.data_type
        FROM metrics.aggregation a
          JOIN metrics.aggregation_read_group arg ON arg.aggregation_id=a.id
          JOIN (SELECT bsp_collaborator_sample_id sample FROM pdos UNION SELECT collaborator_sample_id FROM rghqs) sampagg ON sampagg.sample=a.sample
        WHERE is_latest<>0  --=1
    ),

      agg_crsp AS (
        SELECT /*+ MATERIALIZE */
          -- PK
          arg.flowcell_barcode,
          arg.lane,
          arg.LIBRARY_name,
          --
          arg.molecular_barcode_name,
          a.id,
          a.workflow_end_date,

          a.project,
          a.SAMPLE,
          a.data_type
        FROM metrics.aggregation@CRSPREPORTING.crspprod a
          JOIN metrics.aggregation_read_group@CRSPREPORTING.crspprod arg ON arg.aggregation_id=a.id
        WHERE is_latest<>0  --=1
    ),

      agg as (SELECT /*+ MATERIALIZE */ * FROM (SELECT * FROM agg_reg UNION ALL SELECT * FROM agg_crsp))

  SELECT
    --PK
    pdos.pdo_name,
    pdos.pdo_sample_id,
    pdos.sample_position,

    row_number() over(PARTITION BY pdos.pdo_name, pdos.pdo_sample_id, pdos.sample_position ORDER BY
      decode(ssp.aggregation_project, null, 1, 0),
      CASE
      WHEN project IS NULL            THEN 100
      WHEN substr(project, 1,3)='RP-' THEN 0
      WHEN substr(project, 1,1)='V'   THEN 10
      WHEN substr(project, 1,1)='D'   THEN 20
      ELSE 5
      END asc
      ) project_rank,
    --
    --    pdos.bsp_sample_id,

    COGNOS.concat_string_csv(DISTINCT nvl(pdos.bsp_sample_id, decode( pdos.research_project, 'GTEx', rgmd_aliquot, null)))  bsp_sample_id_csv,
    max(rghqs.project_name  ) project_name,
    count(*) n_collapsed_records,
    COGNOS.concat_string_csv(DISTINCT '('||coalesce(pdos.bsp_sample_id,rghqs.gssr_id,rghqs.collaborator_sample_id,pdos.bsp_collaborator_sample_id)||' -> ['||rghqs.flowcell_barcode||','||rghqs.lane||','||rghqs.molecular_indexing_scheme||'])') aliquot_readgroup,
    max(pdos.pdo_created_date)  pdo_created_date,
    max(pdos.pdo_title       )  pdo_title,
    max(pdos.pdo_owner       )  pdo_owner,
    max(pdos.pdo_quote_id    )  pdo_quote_id,
    max(pdos.research_project)  research_project,
    max(pdos.research_project_key) research_project_key,
    max(pdos.product_name    )  product_name,

    max(pdos.sample_delivery_status ) sample_delivery_status,
    max(pdos.sample_is_on_risk      ) sample_is_on_risk,
    max(pdos.sample_is_billed       ) sample_is_billed,
    max(pdos.sample_quote_id        ) sample_quote_id,

    NULL                              bsp_work_request_csv,
    min(pdos.bsp_export_date) bsp_first_export_date,
    COGNOS.concat_string_csv(DISTINCT pdos.bsp_export_date) bsp_export_date_csv,

    max(pdos.bsp_requestor          ) bsp_requestor,
    max(NVL(pdos.bsp_collaborator_sample_id,rghqs.collaborator_sample_id)) bsp_collaborator_sample_id,
    max(pdos.product_part_number    ) product_part_number,

    max(rghqs.organism_scientific_name) organism_scientific_name,
    max(rghqs.data_type     ) data_type,

    MIN(decode(is_index_only, 0, rghqs.run_date         )) runs_start,
    MAX(decode(is_index_only, 0, rghqs.run_end_date     )) runs_end,
    MAX(decode(is_index_only, 0, rghqs.fpv2_lod         )) fpv2_lod_max,
    MIN(decode(is_index_only, 0, rghqs.fpv2_lod         )) fpv2_lod_min,
    SUM(decode(is_index_only, 0, rghqs.bc_pf_reads      )) bc_pf_reads,
    SUM(decode(is_index_only, 0, rghqs.bc_total_reads   )) bc_total_reads,
    SUM(decode(is_index_only, 0, rghqs.bc_total_pf_bases)) bc_total_pf_bases,
    SUM(decode(is_index_only, 0, rghqs.run_fraction     )) run_fraction_sum,

    min(decode(rghqs.run_rank, 1, rghqs.flowcell_barcode, null)) first_run_barcode,

    max(agg.project) aggregation_project,
    max(agg.workflow_end_date) last_aggregation_date,
    COGNOS.concat_string_csv(DISTINCT rghqs.issues) issues,
    max(pdos.bsp_original_material_type) bsp_original_material_type,
    max(pdos.bsp_root_material_type)     bsp_root_material_type,
    max(rghqs.source) source,
    DECODE(max(rghqs.source),
           'Regular', max(NVL(pdos.bsp_collaborator_sample_id,rghqs.collaborator_sample_id)),
           MAX(rghqs.root_sample)
    ) external_id,
    MAX(rghqs.sample_type) sample_type,
    MAX(nvl(pdos.bsp_collaborator_pt_id,rghqs.collaborator_participant_id)) collaborator_participant_id,
    SUM(decode(is_index_only, 0, rghqs.lane_fraction    ))  lane_fraction,
    max(seq_rg)                                             n_seq_rg,
    max(blacklisted_rg)                                     n_blacklisted_rg,
    max(pdos.product_family_name    )                       product_family

  FROM pdos
    JOIN pdo_sample_positions sp ON sp.pdo_name=pdos.pdo_name AND sp.pdo_sample_id=pdos.pdo_sample_id
    LEFT JOIN rghqs ON
                      rghqs.pdo=pdos.pdo_name AND
                      CASE
                      WHEN rghqs.product_order_sample  IS NOT NULL  AND pdos.pdo_sample_id = rghqs.product_order_sample  THEN 1
                      WHEN rghqs.product_order_sample  IS NULL  AND
                           (pdos.bsp_sample_lsid            =rghqs.lsid OR
                            pdos.bsp_collaborator_sample_id =rghqs.collaborator_sample_id OR
                            pdos.pdo_sample_id              =rghqs.gssr_id)
                        THEN 1
                      ELSE 0
                      END = 1

                      -- demultiplexing
                      AND --mod(rghqs.rownumber, sp.positions) = pdos.relative_sample_position
                      CASE --added on Oct 11, 2017 RPT-4408
                        WHEN pdos.bsp_sample_id = rghqs.rgmd_aliquot THEN 1
                        WHEN pdos.bsp_sample_id IS NULL AND mod(rghqs.rownumber, sp.positions) = pdos.relative_sample_position THEN 1
                      ELSE 0
                      END =1
					  

    LEFT JOIN agg ON
                    agg.flowcell_barcode        =rghqs.flowcell_barcode AND
                    agg.lane                    =rghqs.lane AND
                    agg.library_name            =rghqs.library
    LEFT JOIN COGNOS.pdo_star_special_projects ssp on ssp.pdo_name=pdos.pdo_name AND ssp.pdo_sample_id=pdos.pdo_sample_id and ssp.aggregation_project=agg.project

  GROUP BY pdos.pdo_name, pdos.pdo_sample_id, pdos.sample_position,/*pdos.bsp_sample_id,*/agg.project,ssp.aggregation_project
;

--STATEMENT
DELETE FROM COGNOS.pdo_star5_aux a
WHERE (pdo_name,pdo_sample) IN (SELECT STRING_FIELD1, STRING_FIELD2 FROM COGNOS.target WHERE sessionid=userenv('SESSIONID'))
;

--STATEMENT
INSERT INTO COGNOS.pdo_star5_aux
  WITH
      target_pdos AS (
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
          a.pdo_sample_root,
          a.collaborator_participant_id, a.collection, a.uuid,
          a.bsp_sample_id_csv,
          a.bsp_export_date, a.bsp_export_date_csv,
          a.first_run_barcode, a.runs_start,
          a.runs_end, a.fpv2_lod_min, a.fpv2_lod_max,
          a.last_aggregation_date, a.bc_pf_reads, a.bc_total_reads,
          a.bc_total_pf_bases, a.run_fraction_sum, a.use_raw,
          a.bsp_original_material_type,
          a.bsp_root_material_type bsp_material_type,
          a.sample_type, a.pdo_status

          , a.lane_fraction
          , a.n_seq_rg, a.n_blacklisted_rg,
          pdoj.original_completion_date   original_est_completion_date,
          pdoj.revised_completion_date    revised_est_completion_date,
          pdoj.publication_deadline       publication_deadline,
          pdoj.funding_deadline           funding_deadline,
          pdoj.assignee,
          a.product_family

        FROM COGNOS.target
          JOIN COGNOS.star_face_aux a  ON
                                         a.pdo_name   = target.string_field1 AND
                                         a.pdo_sample = target.string_field2
          JOIN jiradwh.loj_issue_pdo@loj_link.seqbldr pdoj ON pdoj.KEY = target.string_field1
        WHERE target.sessionid = userenv('SESSIONID')
              AND a.pdo_name <> 'PDO-7427'  -- IgFit Validation PDO, initially aggregated in CRSP and then in research, First Contam i First Agg Date break the UK

    ),

      dcfm AS (
        SELECT target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
          cfm.dcfm,
          cfm.rule_name
        FROM target_pdos
          JOIN COGNOS.slxre_sample_cvrg_first_met cfm ON
                                                        cfm.product_part_number = target_pdos.product_part_number AND
                                                        cfm.pdo_name = target_pdos.pdo_name                       AND
                                                        cfm.aggregation_project = target_pdos.project_name        AND
                                                        cfm.external_sample_id = target_pdos.external_id
    ),
      desig AS (
        SELECT
          target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
          min(d.designation_time) flowcell_designation_time
        FROM target_pdos
          JOIN COGNOS.slxre2_designation_metadata d ON
            d.pdo = target_pdos.pdo_name AND
            d.pdo_sample_id = target_pdos.pdo_sample
        WHERE
          NVL(d.instrument_model, 'Null')<>'MISEQ'
          AND NVL(d.is_cancelled, 0)<>1
          AND ( d.run_start_date IS NOT NULL   OR   d.designation_time>(TRUNC(to_date(sysdate)))-5 )

        GROUP BY     target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position
    ),

      first_contam AS (
        SELECT pdo_name, pdo_sample, sample_position,
          workflow_end_date, pct_contamination
        FROM (
          SELECT
            target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
            a.project, a.SAMPLE ,
            a.workflow_end_date,
            c.pct_contamination*100 pct_contamination,
            DENSE_RANK () OVER ( PARTITION BY a.project, a.sample  ORDER BY a.workflow_end_date ASC ) drank

          FROM target_pdos
            JOIN metrics.aggregation a ON
                                         a.project = target_pdos.project_name                 AND
                                         a.SAMPLE  = target_pdos.external_id                  AND
                                         a.LIBRARY IS NULL                                    AND
                                         target_pdos.pdo_created_date < a.workflow_start_date AND
                                         target_pdos.runs_start < a.workflow_start_date
            JOIN metrics.aggregation_contam c ON c.aggregation_id = a.id

          UNION

          SELECT
            target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
            a.project, a.SAMPLE ,
            a.workflow_end_date,
            c.pct_contamination*100 pct_contamination,
            DENSE_RANK () OVER ( PARTITION BY a.project, a.sample  ORDER BY a.workflow_end_date ASC ) drank

          FROM target_pdos
            JOIN metrics.aggregation@crspreporting.crspprod a ON
                a.project = target_pdos.project_name                 AND
                a.SAMPLE = target_pdos.external_id                   AND
                target_pdos.pdo_created_date < a.workflow_start_date AND
                target_pdos.runs_start < a.workflow_start_date       AND
                a.LIBRARY IS NULL
            JOIN metrics.aggregation_contam@crspreporting.crspprod c ON c.aggregation_id = a.id
        )
        WHERE drank=1
    ),

      sample_agg AS (
        SELECT
          --PK
          target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
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
          sa.processing_location,
          sa.n_hiseq_pool_test_lanes,

          sa.al_total_reads,
          sa.al_pf_reads,
          sa.al_pf_noise_reads,
          sa.al_pf_reads_aligned,
          sa.al_pf_hq_aligned_reads,
          sa.al_pf_hq_aligned_bases,
          sa.al_pf_hq_aligned_q20_bases,
          sa.al_pf_aligned_bases,
          sa.al_pf_mismatch_rate,
          sa.al_pct_chimeras,
          sa.al_pct_adapter,

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

          sa.wgs_het_snp_sensitivity,
          sa.wgs_het_snp_q,
          sa.wgs_pct_1x,
          sa.sample_lod,

          qc.latest_plated_date, qc.latest_sage_qpcr_date, --qc.version sa_version,  -- named as "QC Version (QC)" but ref is replaced with "Picard Version"
          qc.latest_bsp_concentration, qc.latest_bsp_volume, qc.latest_sage_qpcr
        FROM target_pdos
          JOIN COGNOS.slxre2_pagg_sample sa  ON sa.project = target_pdos.project_name -- this is the agg project from S5
            AND sa.SAMPLE = target_pdos.external_id
          LEFT JOIN COGNOS.slxre2_sample_agg_qc qc ON
            qc.project = sa.project AND
            qc.SAMPLE = sa.SAMPLE   AND
            qc.data_type = sa.data_type
    ),

      billing  AS (  -- there are no sum(illing_amount)>0 since jan-2015, even before then
        SELECT
          -- PK
          target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position
          --
          , MAX(b.billed_date) max_billed_date, MAX(b.work_complete_date) max_work_complete_date

        FROM target_pdos
          JOIN COGNOS.billing_ledger b ON
            b.pdo_number = target_pdos.pdo_name             AND
            b.sample_name = target_pdos.pdo_sample          AND
            b.sample_position = target_pdos.sample_position AND
            b.price_item_type <>'ADD_ON_PRICE_ITEM'
        WHERE b.billing_session_id<>7599 -- ugly hack: RPT-3625
        GROUP BY 
          target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position
        HAVING SUM(b.billed_amount)>0
    ),
      min_agg_by_aggproj AS (
      SELECT distinct
        --PK
        target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
        min(ag.workflow_end_date) KEEP (DENSE_RANK FIRST ORDER BY ag.workflow_end_date) OVER (PARTITION BY target_pdos.pdo_name, target_pdos.project_name,target_pdos.external_id )  first_agg_end

      FROM
        target_pdos
        JOIN metrics.aggregation ag ON
            ag.project = target_pdos.project_name   AND
            ag.SAMPLE = target_pdos.external_id     AND
            ag.LIBRARY IS NULL

      WHERE
        target_pdos.runs_end < ag.workflow_start_date
      UNION

      SELECT distinct
        --PK
        target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
        min(ag.workflow_end_date) KEEP (DENSE_RANK FIRST ORDER BY ag.workflow_end_date) OVER (PARTITION BY target_pdos.pdo_name, target_pdos.project_name,target_pdos.external_id )  first_agg_end

      FROM
        target_pdos
        JOIN metrics.aggregation@crspreporting.crspprod ag ON
            ag.project = target_pdos.project_name   AND
            ag.SAMPLE = target_pdos.external_id     AND
            ag.LIBRARY IS NULL

      WHERE
        target_pdos.runs_end < ag.workflow_start_date
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


        FROM target_pdos  a
          JOIN COGNOS.slxre2_pagg_library l ON
                                              l.project = a.aggregation_project AND
                                              l.SAMPLE  = a.external_id
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
          COGNOS.concat_string_csv(DISTINCT lcset)               lcset_csv,
          'LCSET-'||min(to_number(substr(lcset,7)))       lcset_min,
          'LCSET-'||max(to_number(substr(lcset,7)))       lcset_max,
          min(orphan_rate)                                relative_orphan_rate_min,
          max(orphan_rate)                                relative_orphan_rate_max,
          avg(orphan_rate)                                relative_orphan_rate_avg
        FROM (
          SELECT
            --PK
            rgmd.flowcell_barcode, rgmd.lane, rgmd.molecular_indexing_scheme,
            --
            target_pdos.pdo_name,
            target_pdos.pdo_sample,
            --
            rgmd.lcset,
            CASE
            WHEN (lhqs.pf_bases_indexed<>lhqs.pf_bases) AND (NVL(lhqs.actual_index_length, 0) > 0 AND lhqs.pf_bases >0)
              THEN (1-lhqs.pf_bases_indexed/(lhqs.pf_bases- NVL(lhqs.ic_bases, 0)))
            ELSE 0.0
            END   orphan_rate
          FROM target_pdos
            JOIN COGNOS.slxre_readgroup_metadata rgmd ON
                rgmd.product_order_key = target_pdos.pdo_name       AND
                rgmd.product_order_sample = target_pdos.pdo_sample  AND
                rgmd.setup_read_structure NOT IN ('8B', '8B8B')
            JOIN COGNOS.slxre2_lane_hqs lhqs ON
                lhqs.flowcell_barcode = rgmd.flowcell_barcode AND
                lhqs.lane = rgmd.lane
        )
        GROUP BY pdo_name, pdo_sample
    ),
      sample_plexity AS (
        SELECT
          --PK
          target_pdos.pdo_name,
          target_pdos.pdo_sample,
          target_pdos.sample_position,
          --
          target_pdos.bsp_sample_id_csv plated_sample_id,
          first_pool,
          plexity,
          sample_lib_member
        FROM target_pdos
          JOIN (SELECT
                  --PK
                  ll.library_name,
                  ll.plated_sample_id,
                  --
                  count(DISTINCT ll.plated_sample_id) over (PARTITION BY ll.library_name) plexity,
                  count(DISTINCT ll.library_name) over(PARTITION BY ll.plated_sample_id) sample_lib_member,
                  min(ll.library_name) keep (dense_rank FIRST ORDER BY ll.library_creation ASC ) over (PARTITION BY ll.plated_sample_id) first_pool

                FROM COGNOS.slxre_library_lcset ll
                WHERE ll.library_type in ( 'Pooled Normalized Library', 'Pooled')
               ) lp ON lp.plated_sample_id = target_pdos.bsp_sample_id_csv  --target_pdos.swr_sample_id

        WHERE lp.library_name = first_pool
    )


  SELECT
    --PK check if it is indeed the PK
    target_pdos.pdo_name, target_pdos.pdo_sample, target_pdos.sample_position,
    --
    target_pdos.research_project, target_pdos.product_name, target_pdos.data_type,
    target_pdos.product_part_number, target_pdos.research_project_key,
    target_pdos.pdo_created_date, target_pdos.pdo_title, target_pdos.pdo_owner, target_pdos.pdo_quote_id,
    target_pdos.organism_name, target_pdos.sample_delivery_status, target_pdos.sample_is_on_risk,
    target_pdos.sample_is_billed, target_pdos.sample_quote_id, target_pdos.squid_project_name,
    target_pdos.dcfm_proj,
    target_pdos.project_name, target_pdos.aggregation_project,  -- project_name is the customized aggregated project for some PDOs
    target_pdos.external_id,target_pdos.translated_external_id,
    target_pdos.bsp_collaborator_sample_id,
    target_pdos.pdo_sample_root,
    target_pdos.collaborator_participant_id, target_pdos.collection, target_pdos.uuid,
    target_pdos.bsp_sample_id_csv,
    target_pdos.bsp_export_date, target_pdos.bsp_export_date_csv,
    target_pdos.first_run_barcode, target_pdos.runs_start,
    target_pdos.runs_end, target_pdos.fpv2_lod_min, target_pdos.fpv2_lod_max,
    target_pdos.last_aggregation_date, target_pdos.bc_pf_reads, target_pdos.bc_total_reads,
    target_pdos.bc_total_pf_bases, target_pdos.run_fraction_sum, target_pdos.use_raw,
    target_pdos.bsp_original_material_type, target_pdos.bsp_material_type,
    target_pdos.sample_type, target_pdos.pdo_status,
    target_pdos.lane_fraction,
    target_pdos.n_seq_rg,
    target_pdos.n_blacklisted_rg ,

    s.gssr_id ,
    s.strain,
    s.rin,
    s.tissue_site, s.tissue_site_detail,

    cfm.dcfm,
    cfm.rule_name,

    desig.flowcell_designation_time,

    first_contam.pct_contamination first_pct_contamination,

    billing.max_work_complete_date,--
    billing.max_billed_date,--

    TO_DATE(NULL) dcfm_first_agg_end,
    min_agg_by_aggproj.first_agg_end ,

    bam.ncbi_status ,
    --
    sa.analysis_id,
    sa.analysis_type,
    sa.analysis_start, sa.analysis_end,
    sa.version,

    sa.gssr_id sagg_gssr_ids,
    sa.lcset_type,sa.lcset_protocol, sa.lcset_seq_technology, sa.lcset_topoff,
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
    la.lib_min_mean_ins_sz,

    sample_plexity.plexity,
    sample_plexity.first_pool,
    sample_plexity.sample_lib_member,

    s.group_name sample_group,
    sa.processing_location agg_processing_location,
    sa.n_hiseq_pool_test_lanes,
    sa.al_total_reads,
    sa.al_pf_reads,
    sa.al_pf_noise_reads,
    sa.al_pf_reads_aligned,
    sa.al_pf_hq_aligned_reads,
    sa.al_pf_hq_aligned_bases,
    sa.al_pf_hq_aligned_q20_bases,
    sa.al_pf_aligned_bases,
    sa.al_pf_mismatch_rate,
    sa.al_pct_chimeras,
    sa.al_pct_adapter,

    sa.wgs_het_snp_sensitivity,
    sa.wgs_het_snp_q,
    sa.wgs_pct_1x,
    sa.sample_lod,

    target_pdos.original_est_completion_date,
    target_pdos.revised_est_completion_date,
    target_pdos.publication_deadline,
    target_pdos.funding_deadline,
    target_pdos.assignee,
    target_pdos.product_family

  FROM
    target_pdos
    LEFT JOIN dcfm cfm ON
        cfm.pdo_name = target_pdos.pdo_name     AND
        cfm.pdo_sample = target_pdos.pdo_sample AND
        cfm.sample_position = target_pdos.sample_position
    LEFT JOIN desig ON
        desig.pdo_name = target_pdos.pdo_name       AND
        desig.pdo_sample = target_pdos.pdo_sample   AND
        desig.sample_position = target_pdos.sample_position
    LEFT JOIN first_contam ON
        first_contam.pdo_name = target_pdos.pdo_name     AND
        first_contam.pdo_sample = target_pdos.pdo_sample AND
        first_contam.sample_position = target_pdos.sample_position
    LEFT JOIN sample_agg sa ON
        sa.pdo_name = target_pdos.pdo_name               AND
        sa.pdo_sample = target_pdos.pdo_sample           AND
        sa.sample_position = target_pdos.sample_position AND
        (case when target_pdos.data_type = sa.data_type or nvl(sa.data_type , 'N/A') = 'N/A' then 1 else 0 end =1 )
    LEFT JOIN lib_agg la ON
        la.pdo_name = target_pdos.pdo_name      AND
        la.pdo_sample = target_pdos.pdo_sample  AND
        la.sample_position = target_pdos.sample_position
    LEFT JOIN billing ON
        billing.pdo_name = target_pdos.pdo_name     AND
        billing.pdo_sample = target_pdos.pdo_sample AND
        billing.sample_position = target_pdos.sample_position
    LEFT JOIN min_agg_by_aggproj ON
        min_agg_by_aggproj.pdo_name = target_pdos.pdo_name      AND
        min_agg_by_aggproj.pdo_sample = target_pdos.pdo_sample  AND
        min_agg_by_aggproj.sample_position = target_pdos.sample_position
    LEFT JOIN lcsets ON
        lcsets.pdo_name = target_pdos.pdo_name AND
        lcsets.pdo_sample = target_pdos.pdo_sample
    LEFT JOIN COGNOS.ng_bam_files_mat bam ON
        bam.project = target_pdos.project_name AND
        bam.SAMPLE = target_pdos.external_id
    LEFT JOIN analytics.bsp_sample s ON s.sample_barcode = target_pdos.pdo_sample     --s.sample_id = substr(target_pdos.pdo_sample, 4)
    LEFT JOIN sample_plexity ON
        sample_plexity.pdo_name = target_pdos.pdo_name          AND
        sample_plexity.pdo_sample = target_pdos.pdo_sample      AND
        sample_plexity.sample_position = target_pdos.sample_position
;

--STATEMENT
DELETE FROM COGNOS.target
WHERE sessionid=userenv('SESSIONID');
