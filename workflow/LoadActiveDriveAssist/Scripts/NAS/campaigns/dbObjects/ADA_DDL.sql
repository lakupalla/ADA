--MasterTable
create external table if not exists NSCVC70_ADA_INP_SEC_HTE
(
   scvc70_sha_k string,
   scvc70_srvr_time_s timestamp,
   scvc70_raw_payload_x_3 string,
   scvc70_process_stat_c string,
   scvc70_process_stat_dtl_x string,
   scvc70_process_stat_utc_s timestamp,
   scvc70_created_by_c varchar(10),
   scvc70_created_on_s timestamp
  )
PARTITIONED BY (scvc70_cntry_c string, scvc70_partition_date_x string)
STORED AS ORC
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Secure/ActiveDriveAssist/nscvc70_ada_inp_sec_hte'
tblproperties ('orc.compress.size'='8192');

create table if not exists NSCVC69_ADA_SEC_HTE
(
   scvc69_sha_k string,
   scvc69_sha_rawpayload_k string,
   scvc69_srvr_time_s timestamp,
   scvc69_vin_11_x string,
   scvc69_veh_d string,
   scvc69_schema_ver_x string,
   scvc69_build_x string,
   scvc69_part_num_map_x string,
   scvc69_part_num_hst_sfwe_x string,
   scvc69_part_num_tree_runner_sfwe_x string,
   scvc69_app_ver_x string,
   scvc69_catg_x string,
   scvc69_actn_x string,
   scvc69_event_d string,
   scvc69_dsmc_msg_x string,
   scvc69_handsoff_wrng_x string,
   scvc69_trffc_jam_asst_stat_x string,
   scvc69_trffc_jam_asst_msg_x string,
   scvc69_trffc_jam_asst_wrng_x string,
   scvc69_ehr_mini_form_of_way_x string,
   scvc69_in_bluezone_area_x string,
   scvc69_invld_lane_scenarios_x string,
   scvc69_invld_veh_cond_x string,
   scvc69_invld_wthr_x string,
   scvc69_trffc_jam_asst_lockout_x string,
   scvc69_trffc_jam_asst_ftr_stat_x string,
   scvc69_gps_lat_decm_deg_r_3 Double,
   scvc69_gps_long_decm_deg_r_3 Double,
   scvc69_gps_hemisphere_lat_southern_r Double,
   scvc69_gps_hemisphere_long_eastern_r Double,
   scvc69_gps_hdop_x Double,
   scvc69_gps_heading_x Double,
   scvc69_gps_fault_x string,
   scvc69_utc_epoch_secs_x string,
   scvc69_spp_path_cuv_r Double,
   scvc69_spp_mdl_typ_x string,
   scvc69_spp_lane_wid_r Double,
   scvc69_spp_left_lane_confid_r int,
   scvc69_spp_path_confid_r int,
   scvc69_spp_right_lane_confid_r int,
   scvc69_extndd_invariant_cond_x string,
   scvc69_hst_veh_ltrl_vlcy_r Double,
   scvc69_hst_veh_long_vlcy_r Double,
   scvc69_hst_veh_yaw_rate_r Double,
   scvc69_sns_gen_curr_time_hst_r BigInt,
   scvc69_sns_gen_dts_vsn_r BigInt,
   scvc69_sns_lane_hst_left_a0_r Double,
   scvc69_sns_lane_hst_left_a2_r Double,
   scvc69_sns_lane_hst_left_confid_r string,
   scvc69_sns_lane_hst_left_rng_end_r Double,
   scvc69_sns_lane_hst_left_typ_x string,
   scvc69_sns_lane_hst_right_a0_r Double,
   scvc69_sns_lane_hst_right_a2_r Double,
   scvc69_sns_lane_hst_right_confid_x string,
   scvc69_sns_lane_hst_right_rng_end_r Double,
   scvc69_sns_lane_hst_right_typ_x string,
   scvc69_sns_lane_releft_a0_r Double,
   scvc69_sns_lane_releft_confid_x string,
   scvc69_sns_lane_releft_typ_x string,
   scvc69_sns_lane_reright_a0_r Double,
   scvc69_sns_lane_reright_confid_x string,
   scvc69_sns_lane_reright_typ_x string,
   scvc69_ign_stat_x string,
   scvc69_front_wiper_stat_x string,
   scvc69_veh_vlcy_r Double,
   scvc69_veh_vlcy_qlty_fctr_x string,
   scvc69_alw_extndd_mode_x string,
   scvc69_event_m timestamp,
   scvc69_trip_d string,
   scvc69_proc_stat_c string,
   scvc69_proc_stat_dtl_x string,
   scvc69_proc_stat_utc_s timestamp,
   scvc69_created_on_s timestamp,
   scvc69_created_by_c varchar(10)
)
partitioned by (scvc69_cntry_c string, scvc69_partition_date_x string)
stored as orc
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Secure/ActiveDriveAssist/nscvc69_ada_sec_hte'
tblproperties ('orc.compress.size'='8192');

--Dup Table
create external table if not exists NSCVD70_ADA_INP_DUP_HTE
(
   cvdd70_sha_k string,
   cvdd70_created_on_s TIMESTAMP,
   cvdd70_created_by_c VARCHAR(10)
)
stored as orc
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Public/ActiveDriveAssist/nscvd70_ada_inp_dup_hte';

create external table if not exists NSCVD69_ADA_DUP_HTE
(
   scvd69_sha_k string,
   scvd69_created_on_s TIMESTAMP,
   scvd69_created_by_c VARCHAR(10)
)
stored as orc
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Public/ActiveDriveAssist/nscvd69_ada_dup_hte';

--TempDup Table
create external table if not exists NSCVE70_ADA_INP_TEMP_DUP_ST_HTE
(
   cvde70_sha_k string
)
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Staging/ActiveDriveAssist/nscve70_ada_inp_temp_dup_st_hte';

create external table if not exists NSCVE69_ADA_TEMP_DUP_ST_HTE
(
   scve69_sha_k string
)
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Staging/ActiveDriveAssist/nscve69_ada_temp_dup_st_hte';

--NonDup Table

create external table if not exists NSCVZ70_ADA_INP_NONDUP_ST_HTE
(
   sha_key string,
   server_time timestamp,
   raw_payload string,
   country_code string,
   partition_date string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Staging/ActiveDriveAssist/nscvz70_ada_inp_nondup_st_hte';

create external table if not exists NSCVZ69_ADA_NONDUP_ST_HTE
(
   sha_key string,
   sha_raw_payload string,
   server_time timestamp,
   vin11 string,
   vehicle_id string,
   schema_version string,
   build string,
   part_number_map string,
   part_number_host_software string,
   part_number_tree_runner_software string,
   country_code string,
   app_version string,
   category string,
   action string,
   event_id string,
   dsmc_message_text string,
   handsoff_warning string,
   trafficjam_assist_status string,
   trafficjam_assist_message_text string,
   trafficjam_assist_warning string,
   ehr_mini_formofway string,
   in_bluezone_area string,
   invalid_lane_scenarios string,
   invalid_vehicle_condition string,
   invalid_weather string,
   traffic_jam_assist_lockout string,
   traffic_jam_assist_status string,
   gps_latitude_decimal_degrees Double,
   gps_longitude_decimal_degrees Double,
   gps_hemisphere_latitude_southern Double,
   gps_hemisphere_longitude_eastern Double,
   gps_hdop Double,
   gps_heading Double,
   gps_fault string,
   utc_epoch_seconds string,
   spp_path_curvature Double,
   spp_model_type string,
   spp_lane_width Double,
   spp_left_lane_confidence int,
   spp_path_confidence int,
   spp_right_lane_confidence int,
   extended_invariant_condition string,
   host_vehicle_lateral_velocity Double,
   host_vehicle_longitudinal_velocity Double,
   host_vehicle_yaw_rate Double,
   sns_gen_current_time_host BigInt,
   sns_gen_timestamp_vision BigInt,
   sns_lane_hostleft_a0 Double,
   sns_lane_hostleft_a2 Double,
   sns_lane_hostleft_confidence string,
   sns_lane_hostleft_rangeend Double,
   sns_lane_hostleft_type string,
   sns_lane_hostright_a0 Double,
   sns_lane_hostright_a2 Double,
   sns_lane_hostright_confidence string,
   sns_lane_hostright_range_end Double,
   sns_lane_hostright_type string,
   sns_lane_releft_a0 Double,
   sns_lane_releft_confidence string,
   sns_lane_releft_type string,
   sns_lane_reright_a0 Double,
   sns_lane_reright_confidence string,
   sns_lane_reright_type string,
   ignition_status string,
   front_wiper_status string,
   vehicle_velocity Double,
   vehicle_velocity_quality_factor string,
   allow_extended_mode string,
   event_time timestamp,
   trip_id string,
   partition_date string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Staging/ActiveDriveAssist/nscvz69_ada_nondup_st_hte';
--Staging Table
create external table if not exists NSCVS70_ADA_INP_ST_HTE
(
   sha_key string,
   server_time timestamp,
   raw_payload string,
   country_code string,
   partition_date string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Staging/ActiveDriveAssist/nscvs70_ada_inp_st_hte';

create external table if not exists NSCVS69_ADA_ST_HTE
(
   sha_key string,
   sha_raw_payload string,
   server_time timestamp,
   vin11 string,
   vehicle_id string,
   schema_version string,
   build string,
   part_number_map string,
   part_number_host_software string,
   part_number_tree_runner_software string,
   country_code string,
   app_version string,
   category string,
   action string,
   event_id string,
   dsmc_message_text string,
   handsoff_warning string,
   trafficjam_assist_status string,
   trafficjam_assist_message_text string,
   trafficjam_assist_warning string,
   ehr_mini_formofway string,
   in_bluezone_area string,
   invalid_lane_scenarios string,
   invalid_vehicle_condition string,
   invalid_weather string,
   traffic_jam_assist_lockout string,
   traffic_jam_assist_status string,
   gps_latitude_decimal_degrees Double,
   gps_longitude_decimal_degrees Double,
   gps_hemisphere_latitude_southern Double,
   gps_hemisphere_longitude_eastern Double,
   gps_hdop Double,
   gps_heading Double,
   gps_fault string,
   utc_epoch_seconds string,
   spp_path_curvature Double,
   spp_model_type string,
   spp_lane_width Double,
   spp_left_lane_confidence int,
   spp_path_confidence int,
   spp_right_lane_confidence int,
   extended_invariant_condition string,
   host_vehicle_lateral_velocity Double,
   host_vehicle_longitudinal_velocity Double,
   host_vehicle_yaw_rate Double,
   sns_gen_current_time_host BigInt,
   sns_gen_timestamp_vision BigInt,
   sns_lane_hostleft_a0 Double,
   sns_lane_hostleft_a2 Double,
   sns_lane_hostleft_confidence string,
   sns_lane_hostleft_rangeend Double,
   sns_lane_hostleft_type string,
   sns_lane_hostright_a0 Double,
   sns_lane_hostright_a2 Double,
   sns_lane_hostright_confidence string,
   sns_lane_hostright_range_end Double,
   sns_lane_hostright_type string,
   sns_lane_releft_a0 Double,
   sns_lane_releft_confidence string,
   sns_lane_releft_type string,
   sns_lane_reright_a0 Double,
   sns_lane_reright_confidence string,
   sns_lane_reright_type string,
   ignition_status string,
   front_wiper_status string,
   vehicle_velocity Double,
   vehicle_velocity_quality_factor string,
   allow_extended_mode string,
   event_time timestamp,
   trip_id string,
   partition_date string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/CVDP/CVDP${hivevar:hdfs_env}/Warehouse/Staging/ActiveDriveAssist/nscvs69_ada_st_hte';
