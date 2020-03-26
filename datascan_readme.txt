Datascan Process:
-- Goal of Datascan: Take inventory and measure quality of new data:
--  -- Inventory:
--  --  -- Do we have the expected files?
--  --  -- Do we have the expected number of columns for each file?
--  --  -- How do row counts compare to previous files?
--  -- Quality:
--  --  -- Null counts, min/max, other statistics

A. Overview:
0. Initialize data and update config
1. Log File Info
2a. Compare files with previous results
2b. Update database with Inventory results
3. If Inventory data checks out, run Statistics for files
Final. Display results in portal
--  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --
B. Datascan Vocabulary and Key Tables:

Vocabulary:
Datascan: Process of taking inventory and measuring quality of new data.
Client: FHG has several clients, typically hospitals and clinics.
  ex: VNSNY, Fresenius
Source: Each client has >= 1 source for their data. There may or may not be
  overlap for patient data between sources.
  ex: VNSNY(1) - Altruista
      Fresenius(4) - Acumen MD, Athena Health, NextGen, eCube
Refresh: The client/source will dictate how often we 'refresh' their
  data. The 'refresh' is composed of
    1. receiving client's new data per a variable period of time:
      -- ex: VNSNY is weekly, PCHC is monthly, ARMC is daily.
    2. ETL process to display data in our platform (not part of datascan)
Datascan-Refresh: a datascan for a particular refresh.
File Name: The client's data is typically consistent, but the file name
  may vary per refresh.
  ex: jan: filename_jan_2020 -> feb: filename_feb_2020
Stdz File Name: An attempt to 'standardize' file names across refresh_id
  ex: jan: filename -> feb: filename
Field Name: The name of a column within a file.
File Set: A set of expected files each refresh for client, source pair.
  File Set should be agreed upon between the client and FHG.
Issue: Various messages to display for 'issues' with the datascan.
  ex: row counts significantly decreased, column counts do not match, etc.
Download of files: A newly-received folder of files within a
  /$client/current/$source pair.

portal_test2: A schema within mysql.test containing all tables relevant
  to the datascan (and other information)

Key Tables (within portal_test2):
dim_client_master: Table containing all client ids/info
dim_client_data_source: Table containing all client source ids/info
fact_datascan: Table containing row counts and column counts
fact_datascan_refresh: Table containing inventory success or non-success of a
  datascan for a particular refresh of data.
fact_datascan_statistics: Table containing statistics for columnar data
fact_data_load: Table containing information pertaining to loading of data
-- Note: There are several tables in the form of
  dim_client(_data_source_)$name that contribute to the fact tables.
-- The Datascan portal_test2 schema structure can be viewed @
    '/data/development/datascan_sandbox/portal_test2_datascan_layout.pdf'
    -- Note: may or may not be datascan_sandbox ^.
-- Acronyms: d.c.; d.s.; d.c.d.s. These will be used over and over below.
  -- The acronym dc is 'dim_client'
  -- The acronym ds is 'data_source'
  -- The acronym dcds is 'dim_client_data_source'
    -- All dcds_$table have an associated client/source pair for each entry.
--  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --
C. Breakdown of overview

Repeat of overview:
0. Initialize data and get config
1. Log File Info
2a. Compare files with previous results
2b. Update database with Inventory results
3. If Inventory data checks out, run Statistics for files
Final. Display results in portal

- Beginning note: This process is partially dependent upon the client being
  proactive and consistent regarding the files they send us.
  Explanation for handling inconsistent client files is relayed at the end.

0. Initialize data and update config
a. Files are loaded by IT into the client's directory
- The path /data/customers/$client/current/$source/$yyyymmdd
  contains new data for that client/source for that day.
  - This is a download of files, as defined above.
- The datascan revolves around a client config:
  - The client config specifies settings.
  - The initial step locates the client, source, and data.
  - The
  #######################################

1. Log File Info
a. The reception of a download of files is logged in dcds_download.
b. The files in this download of files are added to dcds_file_name, if they
  do not previously exist.
  -- ex: across 2 refreshes, file_ref1 and file_ref2 ; these Files
    are essentially the same, just for different periods of time.
c. Standardization of the filenames is attempted.
  -- ex: file_ref1 and file_ref2 are both just 'file' .
  - If the standardized file id does not exist, it is created.
d. Quantity of columns and names are stored for later use in dcds_field_name.
e. client,source,download,file,stdz_file are logged in fact_data_load.
  -- fact_data_load is used later in the datascan to help track where
    the record/file originated from.

- The success of a datascan-refresh depends on the inventory and quality
  of the data compared to Expectation. The Expectation can be defined as:
    1. A defined set of Files
    2. Row counts from the last matched files
    3. Column MATCH from the last matched files.
2a. Compare files with previous results
a. '1.' from the Expectation depends on a user-input expected set of files.
  - If there is a set of files to be expected going forward for a
    client/source pair, then this must be defined within the
    dcds_stdz_file_name and dcds_file_set tables.
    - dcds_stdz_file_name tracks the files within a set, dcds_file_set
      auto-increments the file_set_id to assure there are no repeat file sets.
  - Once the file set is defined, then the received files for that download
      will be compared to the expected file set.
      - If the file set matches, great.
      - If not, the datascan will proceed and listen for any
        missing files or creation/expansion of the file set definition.
        - If the file set id has been used successfully in the past,
          creation (and not modification) of a file set id to
          reflect changes is required.
2b. Update database with Row and Column results
a. '2.' and '3.' from the Expectation depend on previous results
  in fact_datascan
  - Which previous counts should the new counts compare against?
  - There are two possibilities:
    1. The most recent counts
    2. The last refresh_id
    - The datascan ideally will use both.
b. First, refresh_id for the datascan is located
  - The refresh_id associated with a download of files depends
    on the date of the download of files AND the expected frequency of
    reception of new files.
  - To locate, we consider dcds_download (receipt_date and download_date),
    the client's dim_refresh table (id, end_date, active) in (qa, uat, prod),
    and the expected frequency in dcds_frequency.
c. Compare the new row&column counts vs old row&column counts
  - This is done by using the most recent, greatest previous refresh id
    for the matching stdz_file_id .
    - clarification: If a new file was not in a previous refresh,
      it will use the most recent match for stdz_file_id.
d. fact_datascan is populated with the following info:
  id, client_id, source_idd, data_load_id, stdz_file_id,
  run_date, refresh_id, column_match, old_count, new_count, issue_id
    - where, issue_id comes from dim_issue and denotes the problem
e. fact_datascan_refresh is populated with the following info:
  id, client_id, source_id, refresh_id, file_set_id, is_complete(0/1)
  - the pair of client_id, source_id, refresh_id are unique.
  - Using the results in fact_datascan, the final inventory is checked.
    - Namely, 1. matched file set, 2. row counts, 3. column counts.

3. If Inventory data checks out, run Statistics for files
- If the inventory checks out, then the datascan-refresh is complete.
- If not, the datascan will halt, before running statistics.
  - User override can set the datascan refresh as complete.
    Use with caution!
- There are two possibilities for the datascan-refresh being incomplete:
  1. The datascan-refresh is truly incomplete and more files or updating of
    files is needed to necessitate completeness.
  2. We have the files we want and are okay with the mismatches in inventory.
    - If so, the datascan actor will need to modify is_complete to '1'
      - Then, run the statistics script.
- It is also possible that a user will want to run statistics ad-hoc.
  - In this case, the statistics will be output, but not published to the DB.

4. Irrespective of step 3, the results will be displayed in the portal.

-----------------------------------

Handling of inconsistent data:

- dcds_frequency will be the decider of consistency vs inconsistency.
- If the client has a frequency of 'indeterminate' they will not attempt
  to use automatically set a refresh id
  - All datascans and selection or non-selection of refresh-id
    should be manual. Potentially, normalized file names as well.
    - There will be a feature within the manual datascan tht assists
    with selecting the matching data.


-----------------------------------

Viewing of ad-hoc datascans in portal:

- There ought to be an option to view non-refresh id compliant datascans
  for a client.
  - That way they are not muddled together. But if someone wants to run
    an ad-hoc run and wants to display it in portal, there won't be muddle.
