CREATE TEMP TABLE drug_statins
AS
select subject_id, 
CASE
    when drug_taken = 1 then 1
    else 0
END as STATINS
from 
scratch.cohort COHORT
left join (
   select distinct person_id, 1 as drug_taken
   from drug_era DRUG
   join scratch.cohort COHORT on COHORT.subject_id=DRUG.person_id
   where DRUG.drug_concept_id in (1539403,1592085,1551860,1549686,1545958,1592180,1510813,40165636)
   and DRUG.drug_era_start_date >= COHORT.cohort_start_date
) drug_inclusion
on COHORT.subject_id = drug_inclusion.person_id;

ANALYZE drug_statins;


CREATE TEMP TABLE drug_low_molec_weight_heparin
AS
select subject_id, 
CASE
    when drug_taken = 1 then 1
    else 0
END as LOW_MOLEC_WEIGHT_HEPARIN
from 
scratch.cohort COHORT
left join (
   select distinct person_id, 1 as drug_taken
   from drug_era DRUG
   join scratch.cohort COHORT on COHORT.subject_id=DRUG.person_id
   where DRUG.drug_concept_id in (35884372,35884361)
   and DRUG.drug_era_start_date >= COHORT.cohort_start_date
) drug_inclusion
on COHORT.subject_id = drug_inclusion.person_id
;
ANALYZE drug_low_molec_weight_heparin;


CREATE TEMP TABLE drug_beta_blocker
AS
select subject_id, 
CASE
    when drug_taken = 1 then 1
    else 0
END as BETA_BLOCKER
from 
scratch.cohort COHORT
left join (
   select distinct person_id, 1 as drug_taken
   from drug_era DRUG
   join scratch.cohort COHORT on COHORT.subject_id=DRUG.person_id
   where DRUG.drug_concept_id in (1353766,36856962,950370,1346823,1386957,1313200,19024904,1327978,1345858,19018640,19072028,1370109,902427,1319998,1314002,1322081,1338005,19049145,1307046,1314577,19063575,19018488,1314577)
   and DRUG.drug_era_start_date >= COHORT.cohort_start_date
) drug_inclusion
on COHORT.subject_id = drug_inclusion.person_id
;
ANALYZE drug_beta_blocker;


CREATE TEMP TABLE drug_warfarin
AS
select subject_id, 
CASE
    when drug_taken = 1 then 1
    else 0
END as WARFARIN
from 
scratch.cohort COHORT
left join (
   select distinct person_id, 1 as drug_taken
   from drug_era DRUG
   join scratch.cohort COHORT on COHORT.subject_id=DRUG.person_id
   where DRUG.drug_concept_id=1310149
   and DRUG.drug_era_start_date >= COHORT.cohort_start_date
) drug_inclusion
on COHORT.subject_id = drug_inclusion.person_id
;
ANALYZE drug_warfarin;


CREATE TEMP TABLE drug_aspirin
AS
select subject_id, 
CASE
    when drug_taken = 1 then 1
    else 0
END as ASPIRIN
from 
scratch.cohort COHORT
left join (
   select distinct person_id, 1 as drug_taken
   from drug_era DRUG
   join scratch.cohort COHORT on COHORT.subject_id=DRUG.person_id
   where DRUG.drug_concept_id=1112807
   and DRUG.drug_era_start_date >= COHORT.cohort_start_date
) drug_inclusion
on COHORT.subject_id = drug_inclusion.person_id
;
ANALYZE drug_aspirin;


CREATE TEMP TABLE proced_cardioversion
AS
select subject_id, 
CASE
    when proced_done = 1 then 1
    else 0
END as CARDIOVERSION
from 
scratch.cohort COHORT
left join (
   select distinct person_id, 1 as proced_done
   from procedure_occurrence PROCED
   join scratch.cohort COHORT on COHORT.subject_id=PROCED.person_id
   where PROCED.procedure_concept_id in (2008357,2008358,2008359,2008360,2008361,2313791,2313792)
   and PROCED.procedure_date >= COHORT.cohort_start_date
) proced_inclusion
on COHORT.subject_id = proced_inclusion.person_id
;
ANALYZE proced_cardioversion;

CREATE TEMP TABLE features
AS
select 
    COHORT.subject_id as PERSON_ID, 
    --COHORT.cohort_start_date, 
    FLOOR((EXTRACT(YEAR FROM COHORT.cohort_end_date) - PERSON.year_of_birth)) as AGE, 
    DRUG_1.STATINS,
    DRUG_2.LOW_MOLEC_WEIGHT_HEPARIN,
    DRUG_3.BETA_BLOCKER,
    DRUG_4.WARFARIN,
    DRUG_5.ASPIRIN,
    PROCED_1.cardioversion,
    CASE
        WHEN DEATH.death_date is NULL THEN 0
        ELSE 1
    END as DEAD,
    CASE
        WHEN PERSON.gender_concept_id=8507 THEN 0
        ELSE 1
    END as GENDER

    
from scratch.cohort COHORT
inner join "OMOP_CDM_5.2.2".person PERSON on COHORT.subject_id = PERSON.person_id 
left join "OMOP_CDM_5.2.2".death DEATH on COHORT.subject_id = DEATH.person_id
inner join drug_statins DRUG_1 on DRUG_1.subject_id=COHORT.subject_id
inner join drug_low_molec_weight_heparin DRUG_2 on DRUG_2.subject_id=COHORT.subject_id
inner join drug_beta_blocker DRUG_3 on DRUG_3.subject_id=COHORT.subject_id
inner join drug_warfarin DRUG_4 on DRUG_4.subject_id=COHORT.subject_id
inner join drug_aspirin DRUG_5 on DRUG_5.subject_id=COHORT.subject_id
inner join proced_cardioversion PROCED_1 on PROCED_1.subject_id=COHORT.subject_id
where COHORT.cohort_definition_id=3
ORDER BY COHORT.subject_id
;
ANALYZE features;

COPY features TO '/tmp/features.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE features;
DROP TABLE features;
TRUNCATE TABLE drug_statins;
DROP TABLE drug_statins;
TRUNCATE TABLE drug_low_molec_weight_heparin;
DROP TABLE drug_low_molec_weight_heparin;
TRUNCATE TABLE drug_beta_blocker;
DROP TABLE drug_beta_blocker;
TRUNCATE TABLE drug_warfarin;
DROP TABLE drug_warfarin;
TRUNCATE TABLE drug_aspirin;
DROP TABLE drug_aspirin;
TRUNCATE TABLE proced_cardioversion;
DROP TABLE proced_cardioversion;
