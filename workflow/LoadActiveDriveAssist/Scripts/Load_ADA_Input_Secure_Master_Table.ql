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

INSERT INTO TABLE NSCVC70_ADA_INP_SEC_HTE
PARTITION(scvc70_cntry_c,scvc70_partition_date_x)
SELECT
sha_key                                         as  scvc70_sha_k,
server_time                                     as  scvc70_srvr_time_s,
raw_payload                                     as  scvc70_raw_payload_x_3,
process_status                                  as  scvc70_process_stat_c,
process_status_details                          as  scvc70_process_stat_dtl_x,
process_status_date_time_utc                    as  scvc70_process_stat_utc_s,
"${current_user}"                               as  scvc70_created_by_c,
FROM_UNIXTIME(UNIX_TIMESTAMP())                 as  scvc70_created_on_s,
country_code                                    as  scvc70_cntry_c,
partition_date                                  as  scvc70_partition_date_x
FROM NSCVZ70_ADA_INP_NONDUP_ST_HTE;
