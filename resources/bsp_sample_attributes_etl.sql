--STATEMENT
DELETE FROM target WHERE sessionid=userenv('SESSIONID');

--STATEMENT
INSERT INTO target(SESSIONID, STRING_FIELD1, NUMBER_FIELD1)
SELECT userenv('SESSIONID'), sample_id, 1
FROM bsp.bsp_sample s
WHERE /*DELTA_START*/<=s.last_updated_on AND s.last_updated_on</*DELTA_END*/
UNION
SELECT userenv('SESSIONID'), r.sample_id, 2
FROM gap.object_alias_hist hist
JOIN gap.object_alias al ON al.alias_id=hist.alias_id
JOIN bsp.bsp_individual i ON i.lsid=al.lsid
JOIN bsp.bsp_sample root_s ON root_s.individual_id=i.individual_id
JOIN bsp.bsp_root_sample r ON r.root_sample_id=root_s.sample_id
WHERE
    /*DELTA_START*/<=hist.updated_on AND hist.updated_on</*DELTA_END*/
    AND hist.alias_type_id IN (4,6)
UNION
SELECT userenv('SESSIONID'), r.sample_id, 3
FROM gap.object_alias_hist hist
JOIN gap.object_alias al ON al.alias_id=hist.alias_id
--JOIN bsp.bsp_individual i ON i.lsid=al.lsid
JOIN bsp.bsp_sample root_s ON root_s.lsid=al.lsid
JOIN bsp.bsp_root_sample r ON r.root_sample_id=root_s.sample_id
WHERE
    /*DELTA_START*/<=hist.updated_on AND hist.updated_on</*DELTA_END*/
    AND hist.alias_type_id IN (9,26)
;

--STATEMENT
MERGE INTO sample_attribute_bsp satt
USING (
SELECT
    s.sample_id,
    --
    concat_string_csv(DISTINCT r.root_sample_id) AS root_sample_id ,
    s.lsid                              AS SAMPLE_lsid,
    concat_string_csv(DISTINCT root_s.lsid)      AS root_lsid,
    concat_string_csv(DISTINCT collab_s.alias)   AS collaborator_sample_id,

    max(root_s.receipt_date)            AS receipt_date,

    concat_string_csv(DISTINCT
        DECODE (root_s.individual_id, NULL, root_s.individual_id, 'PT-'||root_s.individual_id)
    ) AS participant_id,

    concat_string_csv(DISTINCT collab_pt.alias)  AS collaborator_ptid,
    concat_string_csv(DISTINCT collab_pt2.alias) AS collaborator_ptid_2,
    concat_string_csv(DISTINCT uu_id.alias)      AS uuid,

    max(a.volume)                       AS volume_ul,
    max(mt.material_type_name)          AS material_type,
    max(omt.material_type_name)         AS original_material_type,
    max(sc.collection_name)             AS collection,
    max(g.group_name)                   AS group_name,

    max(st.status)                      AS sample_status,
    max(s.created_on)                   AS created_on,
    max(s.last_updated_on)              AS last_updated_on,
    max(s.organism_classification_id)   AS organism_classification_id,
    max(s.sample_kit_order)             AS sample_kit_order
FROM target
JOIN bsp.bsp_sample s                 ON s.sample_id              =target.STRING_FIELD1
JOIN bsp.bsp_root_sample r            ON r.sample_id              =s.sample_id
JOIN bsp.bsp_sample root_s            ON root_s.sample_id         =r.root_sample_id
JOIN bsp.bsp_sample_attributes a      ON a.sample_attributes_id   =s.sample_attributes_id
JOIN bsp.bsp_material_type mt         ON mt.material_type_id      =s.material_type_id
JOIN bsp.bsp_material_type omt        ON omt.material_type_id     =s.original_material_type_id
JOIN bsp.bsp_sample_collection sc     ON sc.project_id            = s.project_id
JOIN bsp.bsp_group g                  ON g.group_id               = sc.group_id
JOIN bsp.bsp_sample_status st         ON st.sample_status_id      = s.status_id

LEFT JOIN bsp.bsp_individual i        ON i.individual_id          =root_s.individual_id
LEFT JOIN gap.object_alias collab_pt  ON collab_pt.lsid           =i.lsid      AND  collab_pt.alias_type_id=4
LEFT JOIN gap.object_alias collab_pt2 ON collab_pt2.lsid          =i.lsid      AND collab_pt2.alias_type_id=6
LEFT JOIN gap.object_alias collab_s   ON collab_s.lsid            =root_s.lsid AND   collab_s.alias_type_id=9
LEFT JOIN gap.object_alias uu_id      ON uu_id.lsid               =root_s.lsid AND   uu_id.alias_type_id=26

WHERE target.sessionid=userenv('SESSIONID')

GROUP BY s.sample_id, s.lsid
) DELTA ON (DELTA.sample_id = satt.sample_id)

WHEN NOT MATCHED THEN INSERT VALUES (
    DELTA.sample_id,
    DELTA.root_sample_id,
    DELTA.SAMPLE_lsid,
    DELTA.root_lsid,
    DELTA.collaborator_sample_id,
    DELTA.receipt_date,
    DELTA.participant_id,
    DELTA.collaborator_ptid,
    DELTA.collaborator_ptid_2,
    DELTA.uuid,
    DELTA.volume_ul,
    DELTA.material_type,
    DELTA.original_material_type,
    DELTA.collection,
    DELTA.group_name,
    DELTA.sample_status,
    DELTA.created_on,
    DELTA.last_updated_on,
    DELTA.organism_classification_id,
    DELTA.sample_kit_order,
    sysdate
)

WHEN MATCHED THEN UPDATE SET
    satt.root_sample_id            = DELTA.root_sample_id,
    satt.SAMPLE_lsid               = DELTA.SAMPLE_lsid,
    satt.root_lsid                 = DELTA.root_lsid,
    satt.collaborator_sample_id    = DELTA.collaborator_sample_id,
    satt.receipt_date              = DELTA.receipt_date,
    satt.participant_id            = DELTA.participant_id,
    satt.collaborator_ptid         = DELTA.collaborator_ptid,
    satt.collaborator_ptid_2       = DELTA.collaborator_ptid_2,
    satt.uuid                      = DELTA.uuid,
    satt.volume_ul                 = DELTA.volume_ul,
    satt.material_type             = DELTA.material_type,
    satt.original_material_type    = DELTA.original_material_type,
    satt.collection                = DELTA.collection,
    satt.group_name                = DELTA.group_name,
    satt.sample_status             = DELTA.sample_status,
    satt.created_on                = DELTA.created_on,
    satt.last_updated_on           = DELTA.last_updated_on,
    satt.organism_classification_id= DELTA.organism_classification_id,
    satt.sample_kit_order          = DELTA.sample_kit_order,
    satt.TIMESTAMP                 = SYSDATE
;

--STATEMENT
SELECT COUNT(*) || ' samples processed (incl.' || SUM(DECODE(NUMBER_FIELD1, 1, 0, 1)) || ' alias-updated samples)'
FROM target
WHERE target.sessionid=userenv('SESSIONID') ;

--STATEMENT
DELETE FROM target WHERE sessionid=userenv('SESSIONID');
