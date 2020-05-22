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
INSERT INTO NSCVZ69_ADA_NONDUP_ST_HTE
SELECT
   v1.sha_key,
   v1.sha_raw_payload,
   v1.server_time,
   v1.vin11,
   v1.vehicle_id,
   v1.schema_version,
   v1.build,
   v1.part_number_map,
   v1.part_number_host_software,
   v1.part_number_tree_runner_software,
   v1.country_code,
   v1.app_version,
   v1.category,
   v1.action,
   v1.event_id,
   v1.dsmc_message_text,
   v1.handsoff_warning,
   v1.trafficjam_assist_status,
   v1.trafficjam_assist_message_text,
   v1.trafficjam_assist_warning,
   v1.ehr_mini_formofway,
   v1.in_bluezone_area,
   v1.invalid_lane_scenarios,
   v1.invalid_vehicle_condition,
   v1.invalid_weather,
   v1.traffic_jam_assist_lockout,
   v1.traffic_jam_assist_status,
   v1.gps_latitude_decimal_degrees,
   v1.gps_longitude_decimal_degrees,
   v1.gps_hemisphere_latitude_southern,
   v1.gps_hemisphere_longitude_eastern,
   v1.gps_hdop,
   v1.gps_heading,
   v1.gps_fault,
   v1.utc_epoch_seconds,
   v1.spp_path_curvature,
   v1.spp_model_type,
   v1.spp_lane_width,
   v1.spp_left_lane_confidence,
   v1.spp_path_confidence,
   v1.spp_right_lane_confidence,
   v1.extended_invariant_condition,
   v1.host_vehicle_lateral_velocity,
   v1.host_vehicle_longitudinal_velocity,
   v1.host_vehicle_yaw_rate,
   v1.sns_gen_current_time_host,
   v1.sns_gen_timestamp_vision,
   v1.sns_lane_hostleft_a0,
   v1.sns_lane_hostleft_a2,
   v1.sns_lane_hostleft_confidence,
   v1.sns_lane_hostleft_rangeend,
   v1.sns_lane_hostleft_type,
   v1.sns_lane_hostright_a0,
   v1.sns_lane_hostright_a2,
   v1.sns_lane_hostright_confidence,
   v1.sns_lane_hostright_range_end,
   v1.sns_lane_hostright_type,
   v1.sns_lane_releft_a0,
   v1.sns_lane_releft_confidence,
   v1.sns_lane_releft_type,
   v1.sns_lane_reright_a0,
   v1.sns_lane_reright_confidence,
   v1.sns_lane_reright_type,
   v1.ignition_status,
   v1.front_wiper_status,
   v1.vehicle_velocity,
   v1.vehicle_velocity_quality_factor,
   v1.allow_extended_mode,
   v1.event_time,
   v1.trip_id,
   v1.partition_date,
   v1.process_status,
   v1.process_status_details,
   v1.process_status_date_time_utc

FROM (
SELECT
   st.sha_key,
   st.sha_raw_payload,
   st.server_time,
   st.vin11,
   st.vehicle_id,
   st.schema_version,
   st.build,
   st.part_number_map,
   st.part_number_host_software,
   st.part_number_tree_runner_software,
   st.country_code,
   st.app_version,
   st.category,
   st.action,
   st.event_id,
   st.dsmc_message_text,
   st.handsoff_warning,
   st.trafficjam_assist_status,
   st.trafficjam_assist_message_text,
   st.trafficjam_assist_warning,
   st.ehr_mini_formofway,
   st.in_bluezone_area,
   st.invalid_lane_scenarios,
   st.invalid_vehicle_condition,
   st.invalid_weather,
   st.traffic_jam_assist_lockout,
   st.traffic_jam_assist_status,
   st.gps_latitude_decimal_degrees,
   st.gps_longitude_decimal_degrees,
   st.gps_hemisphere_latitude_southern,
   st.gps_hemisphere_longitude_eastern,
   st.gps_hdop,
   st.gps_heading,
   st.gps_fault,
   st.utc_epoch_seconds,
   st.spp_path_curvature,
   st.spp_model_type,
   st.spp_lane_width,
   st.spp_left_lane_confidence,
   st.spp_path_confidence,
   st.spp_right_lane_confidence,
   st.extended_invariant_condition,
   st.host_vehicle_lateral_velocity,
   st.host_vehicle_longitudinal_velocity,
   st.host_vehicle_yaw_rate,
   st.sns_gen_current_time_host,
   st.sns_gen_timestamp_vision,
   st.sns_lane_hostleft_a0,
   st.sns_lane_hostleft_a2,
   st.sns_lane_hostleft_confidence,
   st.sns_lane_hostleft_rangeend,
   st.sns_lane_hostleft_type,
   st.sns_lane_hostright_a0,
   st.sns_lane_hostright_a2,
   st.sns_lane_hostright_confidence,
   st.sns_lane_hostright_range_end,
   st.sns_lane_hostright_type,
   st.sns_lane_releft_a0,
   st.sns_lane_releft_confidence,
   st.sns_lane_releft_type,
   st.sns_lane_reright_a0,
   st.sns_lane_reright_confidence,
   st.sns_lane_reright_type,
   st.ignition_status,
   st.front_wiper_status,
   st.vehicle_velocity,
   st.vehicle_velocity_quality_factor,
   st.allow_extended_mode,
   st.event_time,
   st.trip_id,
   st.partition_date,
   st.process_status,
   st.process_status_details,
   st.process_status_date_time_utc,
   row_number() over (partition by sha_key) as r_num

FROM NSCVS69_ADA_ST_HTE st
LEFT JOIN NSCVC69_ADA_SEC_HTE sp ON st.sha_key = sp.scvc69_sha_k
WHERE sp.scvc69_sha_k is null) v1
WHERE v1.r_num = 1;

--Find duplicate records
INSERT INTO NSCVE69_ADA_TEMP_DUP_ST_HTE
SELECT
   v1.sha_key
FROM (
SELECT
   st.sha_key,
   nondup.sha_key as nondup_shakey,
   row_number() over (partition by st.sha_key) as r_num
FROM NSCVS69_ADA_ST_HTE st
LEFT JOIN NSCVZ69_ADA_NONDUP_ST_HTE nondup
ON st.sha_key = nondup.sha_key) v1
WHERE v1.r_num > 1 OR
v1.nondup_shakey is null;
