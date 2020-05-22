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


--Load Input table Duplicates
INSERT INTO TABLE NSCVD70_ADA_INP_DUP_HTE
SELECT
      cvde70_sha_k                    as cvdd70_sha_k,
      FROM_UNIXTIME(UNIX_TIMESTAMP()) as cvdd70_created_on_s,
      "${current_user}"               as cvdd70_created_by_c
FROM  NSCVE70_ADA_INP_TEMP_DUP_ST_HTE;

--Load Master table Duplicates
INSERT INTO TABLE NSCVD69_ADA_DUP_HTE
SELECT
      scve69_sha_k                    as scvd69_sha_k,
      FROM_UNIXTIME(UNIX_TIMESTAMP()) as scvd69_created_on_s,
      "${current_user}"               as scvd69_created_by_c
FROM  NSCVE69_ADA_TEMP_DUP_ST_HTE;
