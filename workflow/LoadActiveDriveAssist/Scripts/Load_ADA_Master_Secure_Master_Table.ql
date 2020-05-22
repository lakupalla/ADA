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

INSERT INTO TABLE NSCVC69_ADA_SEC_HTE
PARTITION(scvc69_cntry_c,scvc69_partition_date_x)
SELECT
sha_key                               as         scvc69_sha_k,
sha_raw_payload                       as         scvc69_sha_rawpayload_k,
server_time                           as         scvc69_srvr_time_s,
vin11                                 as         scvc69_vin_11_x,
vehicle_id                            as         scvc69_veh_d,
schema_version                        as         scvc69_schema_ver_x,
build                                 as         scvc69_build_x,
part_number_map                       as         scvc69_part_num_map_x,
part_number_host_software             as         scvc69_part_num_hst_sfwe_x,
part_number_tree_runner_software      as         scvc69_part_num_tree_runner_sfwe_x,
app_version                           as         scvc69_app_ver_x,
category                              as         scvc69_catg_x,
action                                as         scvc69_actn_x,
event_id                              as         scvc69_event_d,
dsmc_message_text                     as         scvc69_dsmc_msg_x,
handsoff_warning                      as         scvc69_handsoff_wrng_x,
trafficjam_assist_status              as         scvc69_trffc_jam_asst_stat_x,
trafficjam_assist_message_text        as         scvc69_trffc_jam_asst_msg_x,
trafficjam_assist_warning             as         scvc69_trffFc_jam_asst_wrng_x,
ehr_mini_formofway                    as         scvc69_ehr_mini_form_of_way_x,
in_bluezone_area                      as         scvc69_in_bluezone_area_x,
invalid_lane_scenarios                as         scvc69_invld_lane_scenarios_x,
invalid_vehicle_condition             as         scvc69_invld_veh_cond_x,
invalid_weather                       as         scvc69_invld_wthr_x,
traffic_jam_assist_lockout            as         scvc69_trffc_jam_asst_lockout_x,
traffic_jam_assist_status             as         scvc69_trffc_jam_asst_ftr_stat_x,
gps_latitude_decimal_degrees          as         scvc69_gps_lat_decm_deg_r_3,
gps_longitude_decimal_degrees         as         scvc69_gps_long_decm_deg_r_3,
gps_hemisphere_latitude_southern      as         scvc69_gps_hemisphere_lat_southern_r,
gps_hemisphere_longitude_eastern      as         scvc69_gps_hemisphere_long_eastern_r,
gps_hdop                              as         scvc69_gps_hdop_x,
gps_heading                           as         scvc69_gps_heading_x,
gps_fault                             as         scvc69_gps_fault_x,
utc_epoch_seconds                     as         scvc69_utc_epoch_secs_x,
spp_path_curvature                    as         scvc69_spp_path_cuv_r,
spp_model_type                        as         scvc69_spp_mdl_typ_x,
spp_lane_width                        as         scvc69_spp_lane_wid_r,
spp_left_lane_confidence              as         scvc69_spp_left_lane_confid_r,
spp_path_confidence                   as         scvc69_spp_path_confid_r,
spp_right_lane_confidence             as         scvc69_spp_right_lane_confid_r,
extended_invariant_condition          as         scvc69_extndd_invariant_cond_x,
host_vehicle_lateral_velocity         as         scvc69_hst_veh_ltrl_vlcy_r,
host_vehicle_longitudinal_velocity    as         scvc69_hst_veh_long_vlcy_r,
host_vehicle_yaw_rate                 as         scvc69_hst_veh_yaw_rate_r,
sns_gen_current_time_host             as         scvc69_sns_gen_curr_time_hst_r,
sns_gen_timestamp_vision              as         scvc69_sns_gen_dts_vsn_r,
sns_lane_hostleft_a0                  as         scvc69_sns_lane_hst_left_a0_r,
sns_lane_hostleft_a2                  as         scvc69_sns_lane_hst_left_a2_r,
sns_lane_hostleft_confidence          as         scvc69_sns_lane_hst_left_confid_r,
sns_lane_hostleft_rangeend            as         scvc69_sns_lane_hst_left_rng_end_r,
sns_lane_hostleft_type                as         scvc69_sns_lane_hst_left_typ_x,
sns_lane_hostright_a0                 as         scvc69_sns_lane_hst_right_a0_r,
sns_lane_hostright_a2                 as         scvc69_sns_lane_hst_right_a2_r,
sns_lane_hostright_confidence         as         scvc69_sns_lane_hst_right_confid_x,
sns_lane_hostright_range_end          as         scvc69_sns_lane_hst_right_rng_end_r,
sns_lane_hostright_type               as         scvc69_sns_lane_hst_right_typ_x,
sns_lane_releft_a0                    as         scvc69_sns_lane_releft_a0_r,
sns_lane_releft_confidence            as         scvc69_sns_lane_releft_confid_x,
sns_lane_releft_type                  as         scvc69_sns_lane_releft_typ_x,
sns_lane_reright_a0                   as         scvc69_sns_lane_reright_a0_r,
sns_lane_reright_confidence           as         scvc69_sns_lane_reright_confid_x,
sns_lane_reright_type                 as         scvc69_sns_lane_reright_typ_x,
ignition_status                       as         scvc69_ign_stat_x,
front_wiper_status                    as         scvc69_front_wiper_stat_x,
vehicle_velocity                      as         scvc69_veh_vlcy_r,
vehicle_velocity_quality_factor       as         scvc69_veh_vlcy_qlty_fctr_x,
allow_extended_mode                   as         scvc69_alw_extndd_mode_x,
event_time                            as         scvc69_event_m,
trip_id                               as         scvc69_trip_d,
process_status                        as         scvc69_proc_stat_c,
process_status_details                as         scvc69_proc_stat_dtl_x,
process_status_date_time_utc          as         scvc69_proc_stat_utc_s,
FROM_UNIXTIME(UNIX_TIMESTAMP())	      as         scvc69_created_on_s,
'${current_user}'	                  as         scvc69_created_by_c,
country_code                          as         scvc69_cntry_c,
partition_date                        as         scvc69_partition_date_x
from NSCVZ69_ADA_NONDUP_ST_HTE;

   
