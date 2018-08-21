--STATEMENT
MERGE INTO slxre2_rg_ancestry rganc
USING (
    WITH
    metadata AS (
    SELECT
        -- PK
        rgmd.flowcell_barcode,
        rgmd.lane,
        rgmd.molecular_indexing_scheme,
        --
        rgmd.sample_id plated_sample,
        rgmd.lsid,
        rgmd.sequenced_library,
        l.TYPE sequenced_library_type,
        rgmd.lcset,
        jira.TYPE lcset_type
    FROM  slxre_readgroup_metadata rgmd
    LEFT JOIN slxre_library l                ON l.NAME = rgmd.sequenced_library
    LEFT JOIN jiradwh.loj_issue_lcset@loj_link.seqbldr jira ON jira.KEY = rgmd.lcset
    JOIN slxre_rghqs_targets t ON t.flowcell_barcode = rgmd.flowcell_barcode
    WHERE t.rgmd_timestamp >= /*DELTA_START*/  --trunc(sysdate)+12/24 -- p_start
        AND t.rgmd_timestamp < /*DELTA_END*/  -- trunc(sysdate)+16/24  --p_end
    ),
    squid_anc as(
    SELECT
        rgmd.flowcell_barcode,
        rgmd.lane,
        rgmd.molecular_indexing_scheme,
        --
        max(rgmd.plated_sample)                                                   plated_sample,
        max(decode(subj_sl.type,'Enriched Catch'                , subj_sl.NAME )) catch,
        max(decode(subj_sl.type,'Enriched Pond'                 , subj_sl.NAME )) pond,
        max(decode(subj_sl.type,'Plex Pond'                     , subj_sl.NAME )) plex_pond,
        max(decode(subj_sl.type,'SPRI Concentrated'             , subj_sl.NAME )) spri_concentrated,
        max(decode(subj_sl.type,'Size Selected Enriched Library', subj_sl.NAME )) sage,
        CASE WHEN max(sequenced_library_type) = 'Pooled Normalized Library' THEN max(rgmd.sequenced_library)
            ELSE max(decode(subj_sl.type,'Pooled Normalized Library'     , subj_sl.NAME ))
        END                                                                       pooled_normalized,
        max(decode(subj_sl.type,'Nextera Enriched Library'      , subj_sl.NAME )) nextera ,

        max(rgmd.lcset)                                                           lcset,
        max(rgmd.lcset_type)                                                      lcset_type,
        --NEW COLUMNS
        max(decode(subj_sl.type,'Nexome Catch'                      , subj_sl.NAME )) nexome_catch ,
        max(decode(subj_sl.type,'Nextera Pooled Normalized Library' , subj_sl.NAME )) nexome_pool_norm ,
        max(decode(subj_sl.type,'Nextera SPRI Concentrated Pool'    , subj_sl.NAME )) nexome_spri,
        max(decode(subj_sl.type,'cDNA Enriched Library'            , subj_sl.NAME )) cdna_library,
        max(decode(subj_sl.type,'PCR-Free Pond'                    , subj_sl.NAME )) pcr_free_pond,
        max(decode(subj_sl.type,'PCR-Plus Pond'                    , subj_sl.NAME )) pcr_plus_pond,

        CASE WHEN max(sequenced_library_type) = 'Normalized Library' THEN max(rgmd.sequenced_library)
            ELSE max(decode(subj_sl.type,'Normalized Library'               , subj_sl.NAME ))
        END normalized,
        max(decode(subj_sl.type,'PCR-Plus Norm Pond'               , subj_sl.NAME )) pcr_plus_norm_pond,
        NULL norm_pond_2
		

    FROM metadata rgmd
    JOIN slxre_library trig                              ON             trig.NAME = rgmd.sequenced_library
    JOIN lims_ancestry_event lae                    ON        lae.trigger_id = trig.library_id AND lae.subject_found=1
    JOIN slxre_library subj_sl                      ON    subj_sl.library_id = lae.subject_id
    JOIN seq20.seq_content subj                     ON   subj.seq_content_id = lae.subject_id
    JOIN seq20.seq_content_descr_set scds           ON   scds.seq_content_id = subj.seq_content_id
    JOIN seq20.next_generation_library_descr ngld   ON ngld.library_descr_id = scds.seq_content_descr_id
    LEFT JOIN seq20.molecular_indexing_scheme mis   ON                mis.id = ngld.molecular_indexing_scheme_id
    JOIN seq20.lc_sample s                          ON        s.lc_sample_id = ngld.sample_id AND s.lsid = rgmd.lsid
    WHERE
        CASE WHEN mis.NAME = rgmd.molecular_indexing_scheme        OR ngld.molecular_indexing_scheme_id IS NULL THEN 1 ELSE 0 END =1
    GROUP BY rgmd.flowcell_barcode,
        rgmd.lane,
        rgmd.molecular_indexing_scheme
    ),
    mercury_anc AS (
        SELECT  
            --PK
            flowcell_barcode, 
            lane, 
            molecular_indexing_scheme,
            --
            
            max(plated_sample) plated_sample,
            concat_string_csv(DISTINCT decode(ancestor_library_type, 'Catch', ancestor_library)) catch,
            concat_string_csv(DISTINCT decode(ancestor_library_type, 'Pond', ancestor_library))  pond,
            NULL plex_pond,
            NULL spri_concentrated,
            NULL sage,
            concat_string_csv(DISTINCT decode(ancestor_library_type, 'Pooled', ancestor_library, 'Calibrated Pooled', ancestor_library))         pooled_normalized,
            NULL nextera,
            max(lcset)      lcset, 
            max(lcset_type) lcset_type,
            NULL nexome_catch, NULL nexome_pool_norm, NULL nexome_spri, NULL cdna_library, NULL pcr_free_pond,
            NULL pcr_plus_pond,
    --        concat_string_csv(DISTINCT decode(ancestor_library_type, 'Normalized', ancestor_library))     normalized,
            concat_string_csv(DISTINCT CASE WHEN ancestor_library_type = 'Normalized' AND n_samples > 1 THEN ancestor_library ELSE NULL END ) normalized,
            concat_string_csv(DISTINCT decode(ancestor_library_type, 'Norm Pond', ancestor_library))      pcr_plus_norm_pond,
            
            concat_string_csv(DISTINCT CASE WHEN ancestor_library_type = 'Normalized' AND n_samples = 1 THEN ancestor_library ELSE NULL END ) norm_pond_2
        FROM (        
            SELECT 
            rgmd.flowcell_barcode, 
            rgmd.lane, 
            rgmd.molecular_indexing_scheme,
            --
            
            rgmd.plated_sample,
            rgmd.lcset,
            rgmd.lcset_type,
            la.ancestor_library_type,
            la.ancestor_library,
            count(DISTINCT lls.lcset_sample) over (PARTITION BY lls.library_label ) n_samples
            FROM metadata rgmd 
            JOIN mercury_library_ancestry la ON la.child_library = rgmd.sequenced_library
            JOIN mercurydw.library_lcset_sample lls ON lls.library_label = la.ancestor_library AND lls.lcset_sample = rgmd.plated_sample
            WHERE 
                CASE WHEN la.ancestor_library_type = 'Pooled' AND la.ancestor_library LIKE 'AB%' THEN 0 ELSE 1 END =1  --temporary until Pooled AB vessels are removed from Library_ancestry
        )
        GROUP BY flowcell_barcode, lane, molecular_indexing_scheme
    )
    -- PK: flowcell_barcode, lane, molecular_indexing_scheme
    SELECT * FROM squid_anc
    UNION ALL
    SELECT * FROM mercury_anc 
    
   ) DELTA
ON (
    rganc.flowcell_barcode = delta.flowcell_barcode AND
    rganc.lane = delta.lane AND
    rganc.molecular_indexing_Scheme = delta.molecular_indexing_Scheme
)

WHEN NOT MATCHED THEN
    INSERT
    VALUES (
    delta.flowcell_barcode,
    delta.lane,
    delta.molecular_indexing_Scheme,
    --
    delta.plated_sample,
    delta.catch,
    delta.pond,
    delta.plex_pond,
    delta.spri_concentrated,
    delta.sage,
    delta.pooled_normalized,
    delta.nextera,
    DELTA.lcset,
    DELTA.lcset_type,
    DELTA.nexome_catch,
    DELTA.nexome_pool_norm,
    DELTA.nexome_spri,
    DELTA.cdna_library,
    DELTA.pcr_free_pond,
    SYSDATE,
    DELTA.pcr_plus_pond,
    DELTA.normalized,
	DELTA.pcr_plus_norm_pond,
	DELTA.norm_pond_2

    )
WHEN MATCHED THEN
    UPDATE
    SET
        rganc.TIMESTAMP         = SYSDATE,
        rganc.plated_sample     = DELTA.plated_sample,
        rganc.catch             = DELTA.catch,
        rganc.pond              = DELTA.pond,
        rganc.plex_pond         = DELTA.plex_pond,
        rganc.spri_concentrated = DELTA.spri_concentrated,
        rganc.sage              = DELTA.sage,
        rganc.pooled_normalized = DELTA.pooled_normalized,
        rganc.nextera           = DELTA.nextera,

        rganc.lcset             = DELTA.lcset,
        rganc.lcset_type        = DELTA.lcset_type,
        rganc.nexome_catch      = DELTA.nexome_catch,
        rganc.nexome_pool_norm  = DELTA.nexome_pool_norm,
        rganc.nexome_spri       = DELTA.nexome_spri,
        rganc.cdna_library      = DELTA.cdna_library,
        rganc.pcr_free_pond     = DELTA.pcr_free_pond,
        rganc.pcr_plus_pond     = DELTA.pcr_plus_pond,
        rganc.normalized     	= DELTA.normalized,
		rganc.pcr_plus_norm_pond=DELTA.pcr_plus_norm_pond,
		rganc.norm_pond_2=DELTA.norm_pond_2
;