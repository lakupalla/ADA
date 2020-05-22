use ${hivevar:db_name};

set hive.exec.dynamic.partition=true;
set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts=-Xmx6144m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;
set hive.session.id=${hivevar:step_job_name};
set hive.vectorized.execution.enabled=false;
set hive.optimize.sort.dynamic.partition=true;
set tez.grouping.min-size=544874240;
set tez.grouping.max-size=644874240;
set mapreduce.input.fileinputformat.split.minsize=544874240;
set mapreduce.input.fileinputformat.split.maxsize=644874240;

--Find non-duplicate records
INSERT INTO NSCVZ70_ADA_INP_NONDUP_ST_HTE
SELECT
v1.sha_key,
v1.server_time,
v1.raw_payload,
v1.country_code,
v1.partition_date,
v1.process_status,
v1.process_status_details,
v1.process_status_date_time_utc
FROM (
SELECT
st.sha_key,
st.server_time,
st.raw_payload,
st.country_code,
st.partition_date,
st.process_status,
st.process_status_details,
st.process_status_date_time_utc,
row_number() over (partition by sha_key) as r_num

FROM NSCVS70_ADA_INP_ST_HTE st
LEFT JOIN NSCVC70_ADA_INP_SEC_HTE sp ON st.sha_key = sp.scvc70_sha_k
WHERE sp.scvc70_sha_k is null) v1
WHERE v1.r_num = 1;

--Find duplicate records
INSERT INTO NSCVE70_ADA_INP_TEMP_DUP_ST_HTE
SELECT
   v1.sha_key
FROM (
SELECT
   st.sha_key,
   nondup.sha_key as nondup_shakey,
   row_number() over (partition by st.sha_key) as r_num
FROM NSCVS70_ADA_INP_ST_HTE st
LEFT JOIN NSCVZ70_ADA_INP_NONDUP_ST_HTE nondup
ON st.sha_key = nondup.sha_key) v1
WHERE v1.r_num > 1 OR
v1.nondup_shakey is null;
