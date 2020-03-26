-- Find client ID
select * from dim_client_master;

-- Find source ID
select c.id as Client_ID, c.descriptive_name as Client_Name,
       cds.id as Source_ID, dsm.name as DataSource_Name, dssm.name as DataSourceSystem_Name, dsom.name as DataSourceOrigin_Name
from portal_test2.dim_client_data_source cds
join portal_test2.dim_data_source_master dsm
    on cds.data_source_id = dsm.id
join portal_test2.dim_client_master c
    on cds.client_id = c.id
join portal_test2.dim_data_source_system_master dssm
    on cds.data_source_system_id = dssm.id
join portal_test2.dim_data_source_origin_master dsom
    on cds.data_source_origin_id = dsom.id
where cds.active = 'Y'
order by c.id, cds.id;

-- As of 2/12/2020

-- 1001	Arrowhead Regional Medical Center	13	EMR Data	Meditech	Meditech
-- 1001	Arrowhead Regional Medical Center	14	Claims Data	Claims Adjudication System	IEHP
-- 1001	Arrowhead Regional Medical Center	15	Care Coordination Data	Team of Care	Team of care
-- 1001	Arrowhead Regional Medical Center	16	Care Coordination Data	MD Revolution	MD Revolution
-- 1001	Arrowhead Regional Medical Center	55	Claims Data	Claims Adjudication System	Molina
-- 1001	Arrowhead Regional Medical Center	56	Behavioral Health data	Unknown	Department of Behavioral Health
-- 1002	Riverside University Health System	43	EMR Data	EPIC	EPIC
-- 1002	Riverside University Health System	44	Claims Data	Claims Adjudication System	IEHP
-- 1003	Visiting Nurse Service of New York (VNSNY)	57	Care Coordination Data	Altruista	Altruista
-- 1005	Albany Area Primary Health Care	58	EMR Data	eClinicalworks	eClinicalWorks
-- 1006	Fresenius Medical Care	17	EMR Data	AthenaHealth	Athena Health
-- 1006	Fresenius Medical Care	40	EMR Data	eCube	eCube
-- 1006	Fresenius Medical Care	41	EMR Data	Acumen MD	Acumen MD
-- 1006	Fresenius Medical Care	42	EMR Data	NextGen	NextGen
-- 1007	La Clínica del Pueblo	59	EMR Data	eClinicalworks	eClinicalWorks
-- 1008	Mile Square Health Center	60	EMR Data	Cerner	Cerner
-- 1009	Penobscot Community Health Care	61	EMR Data	Centricity	Centricity
-- 1010	HealthLinc, Inc.	38	Claims Data	Unknown	MHS Centene
-- 1010	HealthLinc, Inc.	62	EMR Data	Greenway	Greenway Medical
-- 1011	Southwest Viral Med	64	EMR Data	AthenaHealth	Athena 1
-- 1011	Southwest Viral Med	65	EMR Data	AthenaHealth	Athena 2
-- 1013	RWJ Barnabas Health	25	EHR	Cerner	RWJBH
-- 1013	RWJ Barnabas Health	27	Practice Management 	AllScripts Practice Management System	RWJBH
-- 1013	RWJ Barnabas Health	28	EHR	AllScripts Touchworks	RWJBH
-- 1013	RWJ Barnabas Health	30	Client Generated	Data Dictionary	RWJBH
-- 1013	RWJ Barnabas Health	31	Claims Data	Horizon BCBSNJ_Claims	RWJBH
-- 1013	RWJ Barnabas Health	32	Claims Data	Centricity	RWJBH
-- 1013	RWJ Barnabas Health	33	Client Generated	Uncategorized	RWJBH
-- 1013	RWJ Barnabas Health	34	Client Generated	Member Roster	RWJBH
-- 1013	RWJ Barnabas Health	35	Client Generated	Physician Roster	RWJBH
-- 1013	RWJ Barnabas Health	36	EHR	CMS	RWJBH
-- 1013	RWJ Barnabas Health	37	Client Generated	Cerner Data Dictionary	RWJBH
-- 1018	St John Medical Center	63	EMR Data	AthenaHealth	Athena Health
