CREATE MATERIALIZED VIEW press.cav_time_daily WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       press.dm_time dt
GROUP BY start_date, tenantid,org_id ,unit_id ,department_id,v2tenant;





SELECT add_continuous_aggregate_policy('press.cav_time_daily ',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
   
  
   
   
   

CREATE MATERIALIZED VIEW press.cav_oee_daily WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       press.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid,org_id ,unit_id ,department_id  ;

SELECT add_continuous_aggregate_policy('press.cav_oee_daily',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW press.cav_production_daily WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       press.dm_production  dt
GROUP BY start_date,v2tenant, tenantid,org_id ,unit_id ,department_id  ;

SELECT add_continuous_aggregate_policy('press.cav_production_daily',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   

SELECT drop_continuous_aggregate_policy('press.cav_production_daily');
   
   
select create_hypertable('press.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW press.cav_losses_daily WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       press.dm_losses dt
GROUP BY start_date,v2tenant, tenantid,org_id ,unit_id ,department_id  ;

SELECT add_continuous_aggregate_policy('press.cav_losses_daily',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    
   
 --------------------------------------------------------------------------------------------------------------------------------------------  
 
CREATE MATERIALIZED VIEW press.cav_time_weekly  WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       press.dm_time dt
GROUP BY start_date, tenantid,v2tenant

SELECT add_continuous_aggregate_policy('press.cav_time_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    



CREATE MATERIALIZED VIEW press.cav_oee_weekly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       press.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('press.cav_oee_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW press.cav_production_weekly WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       press.dm_production  dt
GROUP BY start_date,v2tenant, tenantid;

SELECT add_continuous_aggregate_policy('press.cav_production_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');


   
   
select create_hypertable('press.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW press.cav_losses_weekly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       press.dm_losses dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('press.cav_losses_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
   
 ------------------------------------------------------------------------------------------------------------------
    
 CREATE MATERIALIZED VIEW press.cav_time_monthly  WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       press.dm_time dt
GROUP BY start_date, tenantid,v2tenant

SELECT add_continuous_aggregate_policy('press.cav_time_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    



CREATE MATERIALIZED VIEW press.cav_oee_monthly WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       press.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('press.cav_oee_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW press.cav_production_monthly WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       press.dm_production  dt
GROUP BY start_date,v2tenant, tenantid;

SELECT add_continuous_aggregate_policy('press.cav_production_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');


   
   
select create_hypertable('press.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW press.cav_losses_monthly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       press.dm_losses dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('press.cav_losses_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
 -----------------------------------------------------------------------------------------------------------------------------------------------------------
 
CREATE MATERIALIZED VIEW press.cav_time_yearly  WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       press.dm_time dt
GROUP BY start_date, tenantid,v2tenant

SELECT add_continuous_aggregate_policy('press.cav_time_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    



CREATE MATERIALIZED VIEW press.cav_oee_yearly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       press.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('press.cav_oee_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW press.cav_production_yearly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       press.dm_production  dt
GROUP BY start_date,v2tenant, tenantid;

SELECT add_continuous_aggregate_policy('press.cav_production_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');


   
   
select create_hypertable('press.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW press.cav_losses_yearly WITH (timescaledb.continuous) AS
SELECT 
      time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       press.dm_losses dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('press.cav_losses_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
   
-------------------------------------------CNC SCHEMAS CAV-----------------------------------------------------------------------------------------------------------------------------
   
  select  create_hypertable('cnc.dm_time','date',migrate_data =>true)
  
  
select create_hypertable('cnc.dm_losses','date',migrate_data => true)   
  
  
  
  
 select create_hypertable('fl2_avg_machine_available_shiftwise', 'date', migrate_data => true);
   
   
   
   
   
   
CREATE MATERIALIZED VIEW cnc.cav_time_daily WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(total_unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       cnc.dm_time dt
GROUP BY start_date, tenantid,org_id ,unit_id ,department_id,v2tenant;





SELECT add_continuous_aggregate_policy('cnc.cav_time_daily ',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
   
  
   
   
   

CREATE MATERIALIZED VIEW cnc.cav_oee_daily WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       cnc.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid,org_id ,unit_id ,department_id  ;

SELECT add_continuous_aggregate_policy('cnc.cav_oee_daily',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW cnc.cav_production_daily WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       cnc.dm_production  dt
GROUP BY start_date,v2tenant, tenantid,org_id ,unit_id ,department_id  ;

SELECT add_continuous_aggregate_policy('cnc.cav_production_daily',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   

SELECT drop_continuous_aggregate_policy('cnc.cav_production_daily');
   
   
select create_hypertable('cnc.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW cnc.cav_losses_daily WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 day', date) AS start_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       cnc.dm_losses dt
GROUP BY start_date,v2tenant, tenantid,org_id ,unit_id ,department_id  ;

SELECT add_continuous_aggregate_policy('cnc.cav_losses_daily',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    
   
 --------------------------------------------------------------------------------------------------------------------------------------------  
 
CREATE MATERIALIZED VIEW cnc.cav_time_weekly  WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(total_unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       cnc.dm_time dt
GROUP BY start_date, tenantid,v2tenant

SELECT add_continuous_aggregate_policy('cnc.cav_time_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    



CREATE MATERIALIZED VIEW cnc.cav_oee_weekly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       cnc.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('cnc.cav_oee_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW cnc.cav_production_weekly WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       cnc.dm_production  dt
GROUP BY start_date,v2tenant, tenantid;

SELECT add_continuous_aggregate_policy('cnc.cav_production_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');


   
   
select create_hypertable('cnc.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW cnc.cav_losses_weekly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 week', date)::date AS start_date,
    (time_bucket('1 week', date) + INTERVAL '6 days')::date AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       cnc.dm_losses dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('cnc.cav_losses_weekly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
   
 ------------------------------------------------------------------------------------------------------------------
    
 CREATE MATERIALIZED VIEW cnc.cav_time_monthly  WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(total_unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       cnc.dm_time dt
GROUP BY start_date, tenantid,v2tenant

SELECT add_continuous_aggregate_policy('cnc.cav_time_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    



CREATE MATERIALIZED VIEW cnc.cav_oee_monthly WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       cnc.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('cnc.cav_oee_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW cnc.cav_production_monthly WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       cnc.dm_production  dt
GROUP BY start_date,v2tenant, tenantid;

SELECT add_continuous_aggregate_policy('cnc.cav_production_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');


   
   
select create_hypertable('cnc.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW cnc.cav_losses_monthly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 month', date) AS start_Date,
    (time_bucket('1 month', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       cnc.dm_losses dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('cnc.cav_losses_monthly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
 -----------------------------------------------------------------------------------------------------------------------------------------------------------
 
CREATE MATERIALIZED VIEW cnc.cav_time_yearly  WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(machine_up_time) AS machine_up_time,
    SUM(planned_production_time) AS planned_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(total_unplanned_downtime) AS unplanned_downtime,
    SUM(actual_production_time) AS actual_production_time,
    SUM(total_machine_downtime) AS total_machine_downtime,
    round(avg(time_between_job_parts)) AS time_between_job_parts,
    round( AVG(actual_cycletime)) AS actual_cycletime, 
    SUM(breakdowntime) AS breakdowntime
FROM 
       cnc.dm_time dt
GROUP BY start_date, tenantid,v2tenant

SELECT add_continuous_aggregate_policy('cnc.cav_time_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
    



CREATE MATERIALIZED VIEW cnc.cav_oee_yearly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    
    round (avg(oee)) AS oee,
    round( avg(oee_by_time)) AS oee_by_time,
    round(avg(capacity_utilized_percent)) as capacity_utilized_percent ,
    round (SUM(oee_in_availability)) AS oee_in_availability,
    round (avg(ooe)) AS ooe,  
    round (avg(teep)) AS teep,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    round (avg(asset_utilization_percent)) AS asset_utilization_percent,
    round(avg(manufacturing_cost_percent)) AS manufacturing_cost_percent,
    round (avg(overall_machine_efficiency) )as overall_machine_efficiency,
    round(avg(maintenance_efficiency)) as maintenance_efficiency,
    round(avg(corrective_maintenance_cost_percent)) as corrective_maintenance_cost_percent,
    round( avg(maintenance_cost_percent)) as maintenance_cost_percent,
    round(avg(average_production_cost_item)) as average_production_cost_item,
    sum(return_on_asset_investment) as return_on_asset_investment,
    SUM(total_maintenance_cost) AS total_maintenance_cost
  
FROM 
       cnc.dm_oee  dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('cnc.cav_oee_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');






CREATE MATERIALIZED VIEW cnc.cav_production_yearly WITH (timescaledb.continuous) AS
SELECT 
     time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(parts_per_minute)) AS parts_per_minute,
    round( avg(part_per_hour)) AS part_per_hour,
    sum(total_parts_produced) as total_parts_produced ,
    sum(target_parts) as target_parts,
    sum(no_of_parts_rejected) as no_of_parts_rejected,
    sum(good_parts) as good_parts,
    round (SUM(machine_availability_percent)) AS machine_availability_percent,
    round (avg(machine_performance_percent)) AS machine_performance_percent,
    round (avg(quality_percent)) AS quality_percent, 
    round (avg(designed_capacity_of_plant)) AS designed_capacity_of_plant, 
    round (avg(maximum_machine_capacity)) AS maximum_machine_capacity,
    SUM(total_volume_processed) AS total_volume_processed,
    round (avg(throughput_rate) )as throughput_rate,
    round(avg(quality_variability)) as quality_variability,
    round(avg(ratio_actual_to_projected_unit_production_cost)) as ratio_actual_to_projected_unit_production_cost,
    round( avg(production_to_wage_ratio)) as production_to_wage_ratio,
    round(avg(throughput)) as throughput,
    round(avg(rate_of_return)) as rate_of_return,
     SUM(total_production_per_batch) AS total_production_per_batch,
    round (avg(pass_yield)) AS pass_yield
  
FROM 
       cnc.dm_production  dt
GROUP BY start_date,v2tenant, tenantid;

SELECT add_continuous_aggregate_policy('cnc.cav_production_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');


   
   
select create_hypertable('cnc.dm_losses', 'date', migrate_data => true);  



CREATE MATERIALIZED VIEW cnc.cav_losses_yearly WITH (timescaledb.continuous) AS
SELECT 
      time_bucket('1 year', date) AS start_date,
    (time_bucket('1 year', date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY')::date  AS end_date,
    Max(dt."timestamp") AS timestamp,
    tenantid,
    v2tenant,
    round (avg(cycletime_deviation)) AS cycletime_deviation,
    round( avg(cycletime_loss)) AS cycletime_loss,
    sum(machine_idle_time) as machine_idle_time ,
    round (SUM(idletime_loss_percent)) AS idletime_loss_percent,
    SUM(unplanned_downtime_loss) AS unplanned_downtime_loss,
    round (avg(availability_loss_percent)) AS availability_loss_percent,
    round (avg(performance_loss_percent)) AS performance_loss_percent,
    round (avg(quality_loss)) AS quality_loss,
    round (avg(production_loss)) AS production_loss,
    SUM(availability_loss_time) AS availability_loss_time,
    SUM(speed_loss) AS speed_loss,
    round (avg(manufacturing_capacity_loss_percent)) AS manufacturing_capacity_loss_percent,
    sum(planned_downtime) AS planned_downtime,
    sum(downtime_loss) as downtime_loss
FROM 
       cnc.dm_losses dt
GROUP BY start_date,v2tenant, tenantid ;

SELECT add_continuous_aggregate_policy('cnc.cav_losses_yearly',
    start_offset => NULL,
    end_offset => INTERVAL '1 min',
    schedule_interval => INTERVAL '1 hour');
   
   
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   
SELECT * FROM timescaledb_information.jobs;

