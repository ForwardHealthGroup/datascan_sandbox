#!/usr/bin/env python3

# standardize_filenames.py


# This file accesses files which are not in the /datascan/ path,
# but, are in /datascan/client_stdz_scripts .
# Here is the method to so if you are curious:

# First import sys
#  import sys
# Second append the folder path
# sys.path.insert(0, '/the/folder/path/name-folder/')
# Third Make a blank file called __ init __.py in your subdirectory (this tells Python it is a module)
#     name-file.py
#     name-folder
#         __ init __.py
#         name-module.py
# Fourth import the module inside the folder
# from name-folder import name-module

import sys
sys.path.insert(0, '/data/development/automation/datascan_sandbox/client_stdz_scripts')
# init created.
import pandas as pd
from client_stdz_scripts import pchc_stdz

"""
# Script for standardizing file names. Uses client id and source id
from datascan configuration to decide how to standardize.
# Uses files which were not previously matched using dcds_stdz_file_name and fact_data_load
and attempts to standardize these files.

# client id mapping and source id mapping are listed at the bottom of this script.
"""

def client_standardize_files(db_cursor, config, fact_data_load_df):
    """
    Accepts client id, returns standardized files.
    """
    print(fact_data_load_df)
    print(config['client_id'])
    # 1001	Arrowhead Regional Medical Center
    if config['client_id'] == '1001':
        # Run ARMC script
        print('')

    # 1002	Riverside University Health System
    if config['client_id'] == '1002':
        print('')

    # 1003	Visiting Nurse Service of New York
    if config['client_id'] == '1003':
        # Simple... removes extension from end of file_name.
        stdz_name_list = []
        for row in fact_data_load_df.itertuples():
            if pd.isna(row.stdz_file_id):
                stdz_name_list.append( row.file_name.split('.')[0] )
            else:
                query = 'SELECT stdz_file_name FROM dim_client_data_source_stdz_file_name \
                        WHERE id = {} AND client_id = {} AND source_id = {};\
                        '.format(row.stdz_file_id, config['client_id'], config['source_id'])
                db_cursor.execute(query)
                exist_stdz = db_cursor.fetchall()
                stdz_name_list.append( exist_stdz[0][0] )
        stdz_files = pd.DataFrame(stdz_name_list, columns = ['stdz_file_name'])
        client_stdz_df = pd.concat([fact_data_load_df, stdz_files], axis = 1)


        # stdz_files = pd.DataFrame([file.split('.')[0] for file in fact_data_load_df['file_name'].tolist()] \
        #                             , columns = ['stdz_file_name'])
        # print(stdz_files)
        # client_stdz_df = pd.concat([fact_data_load_df, stdz_files], axis = 1)
        #
        # for row in client_stdz_df.itertuples():
        #     # print(row)
        #     # If stdz_file id is null, insert stdz_file_name into dcds_stdz_file_name
        #     if pd.isna(row.stdz_file_id):
        #         query = 'INSERT IGNORE INTO dim_client_data_source_stdz_file_name (stdz_file_name, client_id, source_id) \
        #                 VALUES ("{}", {}, {})\
        #                 '.format(row.stdz_file_name, config["client_id"], config["source_id"])
        #         db_cursor.execute(query)
        #         db_conn.commit()
        #
        #         query = 'SELECT id, stdz_file_name FROM dim_client_data_source_stdz_file_name \
        #                 WHERE stdz_file_name = "{}" AND client_id = {} AND source_id = {} ;\
        #                 '.format(row.stdz_file_name, config["client_id"], config["source_id"])
        #         db_cursor.execute(query)
        #         ids_stdzfiles = db_cursor.fetchall()
        #         client_stdz_df.at[row.Index, 'stdz_file_id'] = ids_stdzfiles[0][0]

    # 1004	ARC Community Services Inc.
    if config['client_id'] == '1004':
        print('')

    # 1005	Albany Area Primary Health Care
    if config['client_id'] == '1005':
        print('')

    # 1006	Fresenius Medical Care
    if config['client_id'] == '1006':
        print('')

    # 1007	La Clínica del Pueblo
    if config['client_id'] == '1007':
        print('')

    # 1008	Mile Square Health Center
    if config['client_id'] == '1008':
        print('')

    # 1009	Penobscot Community Health Care
    if config['client_id'] == '1009':
        print('')
        # Note: PCHC may not work... pchc_standardize_filenames uses python2 stuff.

    # 1010	HealthLinc, Inc.
    if config['client_id'] == '1010':
        print('')

    # 1011	Southwest Viral Med
    if config['client_id'] == '1011':
        print('')

    # 1012	DaVita Inc.
    if config['client_id'] == '1012':
        print('')

    # 1013	RWJ Barnabas Health
    if config['client_id'] == '1013':
        print('')

    # 1014	Molina Healthcare
    if config['client_id'] == '1014':
        print('')

    # 1015	Inland Empire Health Plan
    if config['client_id'] == '1015':
        print('')

    # 1016	Caduceus Health
    if config['client_id'] == '1016':
        print('')

    # 1017	RoundTable Strategic Solutions
    if config['client_id'] == '1017':
        print('')

    # 1018	St John Medical Center
    if config['client_id'] == '1018':
        print('')

    # 1019	Blue Cross Blue Shield of South Carolina
    if config['client_id'] == '1019':
        print('')

    # 0	    Test
    if config['client_id'] == '0':
        print('')
        
    return client_stdz_df


# dim_client_master id <=> client_id
# id    active descriptive_name                         alias
# 1001	Y	   Arrowhead Regional Medical Center        ARMC
# 1002	Y	   Riverside University Health System	    RUHS
# 1003	Y	   Visiting Nurse Service of New York       VNSNY
# 1004	Y	   ARC Community Services Inc.	            ARC
# 1005	Y	   Albany Area Primary Health Care	        AAPHC
# 1006	Y	   Fresenius Medical Care	                Fresenius
# 1007	Y	   La Clínica del Pueblo	                LCDP
# 1008	Y	   Mile Square Health Center	            MSHC
# 1009	Y	   Penobscot Community Health Care	        PCHC
# 1010	Y	   HealthLinc, Inc.					        Healthlinc
# 1011	Y	   Southwest Viral Med                      SWVM
# 1012	Y	   DaVita Inc.	                            Davita
# 1013	Y	   RWJ Barnabas Health					    RWJB
# 1014	Y	   Molina Healthcare					    Molina
# 1015	Y	   Inland Empire Health Plan				IEHP
# 1016	Y	   Caduceus Health	                        None
# 1017	Y	   RoundTable Strategic Solutions	        None
# 1018	Y	   St John Medical Center					SJMC
# 1019	Y	   Blue Cross Blue Shield of South Carolina	BCBSSC
# 0	    N	   Test	                                    Test
#
# dim_client_data_source id <=> source_id
# id    client_name client_id active master                 system                       origin
# 13	ARMC	    1001      Y	     EMR Data	            Meditech	                 Meditech
# 14	ARMC	    1001      Y	     Claims Data	        Claims Adjud. System	     IEHP
# 15	ARMC	    1001      Y	     Care Co-ord. Data	    Team of Care	             Team of care
# 16	ARMC	    1001      Y	     Care Co-ord. Data	    MD Revolution	             MD Revolution
# 55	ARMC 	    1001      Y	     Claims Data	        Claims Adjud. System	     Molina
# 56	ARMC	    1001      Y	     Behavioral Health data	Unknown	                     Department of Behavioral Health
# 43	RUHS	    1002      Y	     EMR Data	            EPIC	                     EPIC
# 44	RUHS	    1002      Y	     Claims Data	        Claims Adjud. System	     IEHP
# 45	RUHS	    1002      N	     EMR Data	            BCA	                         BCA
# 46	RUHS	    1002      N	     EMR Data	            Invision	                 Invision
# 47	RUHS	    1002      N	     EMR Data	            McKesson	                 McKesson
# 48	RUHS	    1002      N	     EMR Data	            NextGen	                     NextGen
# 49	RUHS	    1002      N	     EMR Data	            Novius	                     Novius
# 50	RUHS	    1002      N	     Pharmacy data	        Pharmacy	                 Pharmacy
# 51	RUHS	    1002      N	     Lab data	            Quest	                     Quest
# 52	RUHS	    1002      N	     EMR Data	            Soarian	                     Soarian
# 53	RUHS	    1002      N	     EMR Data	            Vantage	                     Vantage
# 54	RUHS	    1002      N	     Unknown	            Medication_rec	             Medication_rec
# 57	VNSNY	    1003      Y	     Care Co-ord. Data	    Altruista	                 Altruista
# 58	AAPHC	    1005      Y	     EMR Data	            eClinicalworks	             eClinicalWorks
# 17	Fresenius	1006      Y	     EMR Data	            AthenaHealth	             Athena Health
# 40	Fresenius	1006      Y	     EMR Data	            eCube	                     eCube
# 41	Fresenius	1006      Y	     EMR Data	            Acumen MD	                 Acumen MD
# 42	Fresenius	1006      Y	     EMR Data	            a	                     NextGen
# 59	LCDP	    1007      Y	     EMR Data	            eClinicalworks	             eClinicalWorks
# 60	MSHC	    1008      Y	     EMR Data	            Cerner	                     Cerner
# 61	PCHC	    1009      Y	     EMR Data	            Centricity	                 Centricity
# 38	Healthlinc	1010      Y	     Claims Data	        Unknown	                     MHS Centene
# 62	Healthlinc	1010      Y	     EMR Data	            Greenway	                 Greenway Medical
# 64	SWVM	    1011      Y	     EMR Data	            AthenaHealth	             Athena 1
# 65	SWVM	    1011      Y	     EMR Data	            AthenaHealth	             Athena 2
# 25	RWJB	    1013      Y	     EHR	                Cerner	                     RWJBH
# 27	RWJB	    1013      Y	     Practice Management 	AllScripts Practice Mgt.sys. RWJBH
# 28	RWJB	    1013      Y	     EHR	                AllScripts Touchworks	     RWJBH
# 30	RWJB	    1013      Y	     Client Generated	    Data Dictionary	             RWJBH
# 31	RWJB	    1013      Y	     Claims Data	        Horizon BCBSNJ_Claims	     RWJBH
# 32	RWJB	    1013      Y	     Claims Data	        CMS	                         RWJBH
# 33	RWJB	    1013      Y	     Client Generated	    Uncategorized	             RWJBH
# 34	RWJB	    1013      Y   	 Client Generated	    Member Roster	             RWJBH
# 35	RWJB	    1013      Y	     Client Generated	    Physician Roster	         RWJBH
# 36	RWJB	    1013      Y	     EHR	                Centricity	                 RWJBH
# 37	RWJB	    1013      Y	     Client Generated	    Cerner Data Dictionary	     RWJBH
# 63	SJMC	    1018      Y	     EMR Data	            AthenaHealth	             Athena Health
# 66	BCBSSC	    1019      Y 	 Claims Data	        Claims Adjud. System	     BCBSSC
# 67	BCBSSC	    1019      Y	     Client Generated	    Unknown	                     BCBSSC
# 0	    Test	    0         N	     Test	                Test	                     Test

# 1001	Arrowhead Regional Medical Center
# 1002	Riverside University Health System
# 1003	Visiting Nurse Service of New York (VNSNY)
# 1004	ARC Community Services Inc.
# 1005	Albany Area Primary Health Care
# 1006	Fresenius Medical Care
# 1007	La Clínica del Pueblo
# 1008	Mile Square Health Center
# 1009	Penobscot Community Health Care
# 1010	HealthLinc, Inc.
# 1011	Southwest Viral Med
# 1012	DaVita Inc.
# 1013	RWJ Barnabas Health
# 1014	Molina Healthcare
# 1015	Inland Empire Health Plan
# 1016	Caduceus Health
# 1017	RoundTable Strategic Solutions
# 1018	St John Medical Center
# 1019	Blue Cross Blue Shield of South Carolina
# 0	    Test
