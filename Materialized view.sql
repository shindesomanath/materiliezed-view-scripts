CREATE OR REPLACE FUNCTION press.consolidate_data()
RETURNS TABLE(
    tenantid int4,
    unit_id uuid,
    department_id uuid,
    machineid text,
    "date" date,
    "hour" int2,
    "timestamp" int8,
    machine_up_time int4,
    total_machine_runtime int4,
    planned_production_time int4,
    actual_production_time int4,
    machine_idle_time int4,
    time_between_job_parts float4,
    actual_cycletime float8,
    total_planned_downtime int4,
    total_machine_downtime int4,
    breakdowntime int4,
    manhour_per_unit float4,
    unplanned_downtime int4,
    total_parts_produced int4,
    parts_per_minute int4,
    part_per_hour int4,
    target_parts int4,
    no_of_parts_rejected int4,
    good_parts int4,
    machine_availability_percent float8,
    machine_performance_percent float8,
    quality_percent float8,
    pass_yield float4,
    throughput_rate float4,
    quality_variability float4,
    ratio_actual_to_projected_unit_production_cost float4,
    production_to_wage_ratio float4,
    throughput float4,
    rate_of_return float4,
    total_production_per_batch int4,
    cycletime_deviation float4,
    cycletime_loss float4,
    idletime_loss_percent float4,
    downtime_loss float4,
    unplanned_downtime_loss float4,
    planned_downtime_loss float4,
    availability_loss_time int4,
    availability_loss_percent float4,
    performance_loss_percent float4,
    quality_loss float4,
    production_loss float4,
    manufacturing_capacity_loss_percent float4,
    line_balancing_loss float4,
    speed_loss float4,
    oee float4,
    oee_by_time float4,
    oee_in_availability float4,
    ooe float4,
    teep float4,
    capacity_utilized_percent float8,
    asset_utilization_percent float8,
    manufacturing_cost_percent float4,
    overall_machine_efficiency float4,
    maintenance_efficiency float4,
    corrective_maintenance_cost_percent float4,
    maintenance_cost_percent float4,
    average_production_cost_item numeric,
    total_maintenance_cost numeric,
    unit_cost numeric,
    return_on_asset_investment float4
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.tenantid,
        t.unit_id,
        t.department_id,
        t.machineid,
        t.date,
        EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp"))::int2 AS hour,
        t."timestamp",
        t.machine_up_time,
        t.total_machine_runtime,
        t.planned_production_time,
        t.actual_production_time,
        t.machine_idle_time,
        t.time_between_job_parts,
        t.actual_cycletime,
        t.total_planned_downtime,
        t.total_machine_downtime,
        t.breakdowntime,
        t.manhour_per_unit,
        t.unplanned_downtime,
        p.total_parts_produced,
        p.parts_per_minute,
        p.part_per_hour,
        p.target_parts,
        p.no_of_parts_rejected,
        p.good_parts,
        p.machine_availability_percent,
        p.machine_performance_percent,
        p.quality_percent,
        p.pass_yield,
        p.throughput_rate,
        p.quality_variability,
        p.ratio_actual_to_projected_unit_production_cost,
        p.production_to_wage_ratio,
        p.throughput,
        p.rate_of_return,
        p.total_production_per_batch,
        l.cycletime_deviation,
        l.cycletime_loss,
        l.idletime_loss_percent,
        l.downtime_loss,
        l.unplanned_downtime_loss,
        l.planned_downtime_loss,
        l.availability_loss_time,
        l.availability_loss_percent,
        l.performance_loss_percent,
        l.quality_loss,
        l.production_loss,
        l.manufacturing_capacity_loss_percent,
        l.line_balancing_loss,
        l.speed_loss,
        o.oee,
        o.oee_by_time,
        o.oee_in_availability,
        o.ooe,
        o.teep,
        o.capacity_utilized_percent,
        o.asset_utilization_percent,
        o.manufacturing_cost_percent,
        o.overall_machine_efficiency,
        o.maintenance_efficiency,
        o.corrective_maintenance_cost_percent,
        o.maintenance_cost_percent,
        o.average_production_cost_item,
        o.total_maintenance_cost,
        o.unit_cost,
        o.return_on_asset_investment
    FROM
        press."time" t
        LEFT JOIN press.production p ON t.tenantid = p.tenantid AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
        LEFT JOIN press.losses l ON t.tenantid = l.tenantid AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
        LEFT JOIN press.deviation d ON t.tenantid = d.tenantid AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
        LEFT JOIN press.oee o ON t.tenantid = o.tenantid AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"));
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION press.consolidate_data()
RETURNS TABLE(
    tenantid int4,
    unit_id uuid,
    department_id uuid,
    machineid text,
    "date" date,
    "hour" int2,
    "timestamp" int8,
    machine_up_time int4,
    total_machine_runtime int4,
    planned_production_time int4,
    actual_production_time int4,
    machine_idle_time int4,
    time_between_job_parts float4,
    actual_cycletime float8,
    total_planned_downtime int4,
    total_machine_downtime int4,
    breakdowntime int4,
    manhour_per_unit float4,
    unplanned_downtime int4,
    total_parts_produced int4,
    parts_per_minute int4,
    part_per_hour int4,
    target_parts int4,
    no_of_parts_rejected int4,
    good_parts int4,
    machine_availability_percent float8,
    machine_performance_percent float8,
    quality_percent float8,
    pass_yield float4,
    throughput_rate float4,
    quality_variability float4,
    ratio_actual_to_projected_unit_production_cost float4,
    production_to_wage_ratio float4,
    throughput float4,
    rate_of_return float4,
    total_production_per_batch int4,
    cycletime_deviation float4,
    cycletime_loss float4,
    idletime_loss_percent float4,
    downtime_loss float4,
    unplanned_downtime_loss float4,
    planned_downtime_loss float4,
    availability_loss_time int4,
    availability_loss_percent float4,
    performance_loss_percent float4,
    quality_loss float4,
    production_loss float4,
    manufacturing_capacity_loss_percent float4,
    line_balancing_loss float4,
    speed_loss float4,
    oee float4,
    oee_by_time float4,
    oee_in_availability float4,
    ooe float4,
    teep float4,
    capacity_utilized_percent float8,
    asset_utilization_percent float8,
    manufacturing_cost_percent float4,
    overall_machine_efficiency float4,
    maintenance_efficiency float4,
    corrective_maintenance_cost_percent float4,
    maintenance_cost_percent float4,
    average_production_cost_item numeric,
    total_maintenance_cost numeric,
    unit_cost numeric,
    return_on_asset_investment float4
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
        COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
        COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
        COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
        COALESCE(t."date", p."date", l."date", d."date", o."date") AS "date",
        COALESCE(EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")), 
                 EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp")))::int2 AS hour,
        COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") AS "timestamp",
        t.machine_up_time,
        t.total_machine_runtime,
        t.planned_production_time,
        t.actual_production_time,
        t.machine_idle_time,
        t.time_between_job_parts,
        t.actual_cycletime,
        t.total_planned_downtime,
        t.total_machine_downtime,
        t.breakdowntime,
        t.manhour_per_unit,
        t.unplanned_downtime,
        p.total_parts_produced,
        p.parts_per_minute,
        p.part_per_hour,
        p.target_parts,
        p.no_of_parts_rejected,
        p.good_parts,
        p.machine_availability_percent,
        p.machine_performance_percent,
        p.quality_percent,
        p.pass_yield,
        p.throughput_rate,
        p.quality_variability,
        p.ratio_actual_to_projected_unit_production_cost,
        p.production_to_wage_ratio,
        p.throughput,
        p.rate_of_return,
        p.total_production_per_batch,
        l.cycletime_deviation,
        l.cycletime_loss,
        l.idletime_loss_percent,
        l.downtime_loss,
        l.unplanned_downtime_loss,
        l.planned_downtime_loss,
        l.availability_loss_time,
        l.availability_loss_percent,
        l.performance_loss_percent,
        l.quality_loss,
        l.production_loss,
        l.manufacturing_capacity_loss_percent,
        l.line_balancing_loss,
        l.speed_loss,
        o.oee,
        o.oee_by_time,
        o.oee_in_availability,
        o.ooe,
        o.teep,
        o.capacity_utilized_percent,
        o.asset_utilization_percent,
        o.manufacturing_cost_percent,
        o.overall_machine_efficiency,
        o.maintenance_efficiency,
        o.corrective_maintenance_cost_percent,
        o.maintenance_cost_percent,
        o.average_production_cost_item,
        o.total_maintenance_cost,
        o.unit_cost,
        o.return_on_asset_investment
    FROM
        press."time" t
        FULL OUTER JOIN press.production p ON t.tenantid = p.tenantid AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
        FULL OUTER JOIN press.losses l ON t.tenantid = l.tenantid AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
        FULL OUTER JOIN press.deviation d ON t.tenantid = d.tenantid AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
        FULL OUTER JOIN press.oee o ON t.tenantid = o.tenantid AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"));
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION press.consolidate_data()

CREATE OR REPLACE FUNCTION press.consolidate_data()
RETURNS TABLE(
     "date" date,
    "hour" int2,
    "timestamp" int8,
    tenantid int4,
     v2tenant uuid,
     org_id uuid,
    unit_id uuid,
    department_id uuid,
    machineid text,
    operator_id uuid,
    supervisor_id uuid,
    edgeid text,
    shiftid int2,
    machine_up_time int4,
    total_machine_runtime int4,
    planned_production_time int4,
    actual_production_time int4,
    machine_idle_time int4,
    time_between_job_parts float4,
    actual_cycletime float8,
    total_planned_downtime int4,
    total_machine_downtime int4,
    breakdowntime int4,
    manhour_per_unit float4,
    unplanned_downtime int4,
    total_parts_produced int4,
    parts_per_minute int4,
    part_per_hour int4,
    target_parts int4,
    no_of_parts_rejected int4,
    good_parts int4,
    machine_availability_percent float8,
    machine_performance_percent float8,
    quality_percent float8,
    pass_yield float4,
    throughput_rate float4,
    quality_variability float4,
    ratio_actual_to_projected_unit_production_cost float4,
    production_to_wage_ratio float4,
    throughput float4,
    rate_of_return float4,
    total_production_per_batch int4,
    cycletime_deviation float4,
    cycletime_loss float4,
    idletime_loss_percent float4,
    downtime_loss float4,
    unplanned_downtime_loss float4,
    planned_downtime_loss float4,
    availability_loss_time int4,
    availability_loss_percent float4,
    performance_loss_percent float4,
    quality_loss float4,
    production_loss float4,
    manufacturing_capacity_loss_percent float4,
    line_balancing_loss float4,
    speed_loss float4,
    oee float4,
    oee_by_time float4,
    oee_in_availability float4,
    ooe float4,
    teep float4,
    capacity_utilized_percent float8,
    asset_utilization_percent float8,
    manufacturing_cost_percent float4,
    overall_machine_efficiency float4,
    maintenance_efficiency float4,
    corrective_maintenance_cost_percent float4,
    maintenance_cost_percent float4,
    average_production_cost_item numeric,
    total_maintenance_cost numeric,
    unit_cost numeric,
    return_on_asset_investment float4,
    operating_time_deviation float4,
    throughput_deviation int4,
    oee_deviation float4,
    ole_deviation float4
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
        COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
        COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
        COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
        COALESCE(t."date", p."date", l."date", d."date", o."date") AS "date",
        COALESCE(EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")), 
                 EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp")))::int2 AS hour,
        COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") AS "timestamp",
        t.machine_up_time,
        t.total_machine_runtime,
        t.planned_production_time,
        t.actual_production_time,
        t.machine_idle_time,
        t.time_between_job_parts,
        t.actual_cycletime,
        t.total_planned_downtime,
        t.total_machine_downtime,
        t.breakdowntime,
        t.manhour_per_unit,
        t.unplanned_downtime,
        p.total_parts_produced,
        p.parts_per_minute,
        p.part_per_hour,
        p.target_parts,
        p.no_of_parts_rejected,
        p.good_parts,
        p.machine_availability_percent,
        p.machine_performance_percent,
        p.quality_percent,
        p.pass_yield,
        p.throughput_rate,
        p.quality_variability,
        p.ratio_actual_to_projected_unit_production_cost,
        p.production_to_wage_ratio,
        p.throughput,
        p.rate_of_return,
        p.total_production_per_batch,
        l.cycletime_deviation,
        l.cycletime_loss,
        l.idletime_loss_percent,
        l.downtime_loss,
        l.unplanned_downtime_loss,
        l.planned_downtime_loss,
        l.availability_loss_time,
        l.availability_loss_percent,
        l.performance_loss_percent,
        l.quality_loss,
        l.production_loss,
        l.manufacturing_capacity_loss_percent,
        l.line_balancing_loss,
        l.speed_loss,
        o.oee,
        o.oee_by_time,
        o.oee_in_availability,
        o.ooe,
        o.teep,
        o.capacity_utilized_percent,
        o.asset_utilization_percent,
        o.manufacturing_cost_percent,
        o.overall_machine_efficiency,
        o.maintenance_efficiency,
        o.corrective_maintenance_cost_percent,
        o.maintenance_cost_percent,
        o.average_production_cost_item,
        o.total_maintenance_cost,
        o.unit_cost,
        o.return_on_asset_investment,
        d.v2tenant,
        d.org_id,
        d.operator_id,
        d.supervisor_id,
        d.edgeid,
        d.shiftid,
        d.operating_time_deviation,
        d.throughput_deviation,
        d.oee_deviation,
        d.ole_deviation
    FROM
        press."time" t
        FULL OUTER JOIN press.production p ON t.tenantid = p.tenantid AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
        FULL OUTER JOIN press.losses l ON t.tenantid = l.tenantid AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
        FULL OUTER JOIN press.deviation d ON t.tenantid = d.tenantid AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
        FULL OUTER JOIN press.oee o ON t.tenantid = o.tenantid AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"));
END;
$$ LANGUAGE plpgsql;


SELECT * FROM press.consolidate_data();

--- fucntion for cosolidate the data
DROP FUNCTION press.consolidate_data() 

CREATE OR REPLACE FUNCTION press.consolidate_data()
RETURNS TABLE(
    tenantid int4,
    org_id uuid,
    unit_id uuid,
    v2tenant uuid,
    department_id uuid,
    machineid text,
    "date" date,
    "hour" int2,
    "timestamp" int8,
    machine_up_time int4,
    total_machine_runtime int4,
    planned_production_time int4,
    actual_production_time int4,
    machine_idle_time int4,
    time_between_job_parts float4,
    actual_cycletime float8,
    total_planned_downtime int4,
    total_machine_downtime int4,
    breakdowntime int4,
    manhour_per_unit float4,
    unplanned_downtime int4,
    total_parts_produced int4,
    parts_per_minute int4,
    part_per_hour int4,
    target_parts int4,
    no_of_parts_rejected int4,
    good_parts int4,
    machine_availability_percent float8,
    machine_performance_percent float8,
    quality_percent float8,
    pass_yield float4,
    throughput_rate float4,
    quality_variability float4,
    ratio_actual_to_projected_unit_production_cost float4,
    production_to_wage_ratio float4,
    throughput float4,
    rate_of_return float4,
    total_production_per_batch int4,
    cycletime_deviation float4,
    cycletime_loss float4,
    idletime_loss_percent float4,
    downtime_loss float4,
    unplanned_downtime_loss float4,
    planned_downtime_loss float4,
    availability_loss_time int4,
    availability_loss_percent float4,
    performance_loss_percent float4,
    quality_loss float4,
    production_loss float4,
    manufacturing_capacity_loss_percent float4,
    line_balancing_loss float4,
    speed_loss float4,
    oee float4,
    oee_by_time float4,
    oee_in_availability float4,
    ooe float4,
    teep float4,
    capacity_utilized_percent float8,
    asset_utilization_percent float8,
    manufacturing_cost_percent float4,
    overall_machine_efficiency float4,
    maintenance_efficiency float4,
    corrective_maintenance_cost_percent float4,
    maintenance_cost_percent float4,
    average_production_cost_item numeric,
    total_maintenance_cost numeric,
    unit_cost numeric,
    return_on_asset_investment float4,
    operator_id uuid,
    supervisor_id uuid,
    edgeid text,
    shiftid int2,
    operating_time_deviation float4,
    throughput_deviation int4,
    oee_deviation float4,
    ole_deviation float4
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
        COALESCE(t.v2tenant , p.v2tenant , l.v2tenant , d.v2tenant , o.v2tenant) AS v2tenant,
        COALESCE(t.org_id , p.org_id , l.org_id , d.org_id , o.org_id) AS org_id,
        COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
        COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
        COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
        COALESCE(t."date", p."date", l."date", d."date", o."date") AS "date",
        COALESCE(EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")), 
                 EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp")),
                 EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp")))::int2 AS hour,
        COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") AS "timestamp",
        t.machine_up_time,
        t.total_machine_runtime,
        t.planned_production_time,
        t.actual_production_time,
        t.machine_idle_time,
        t.time_between_job_parts,
        t.actual_cycletime,
        t.total_planned_downtime,
        t.total_machine_downtime,
        t.breakdowntime,
        t.manhour_per_unit,
        t.unplanned_downtime,
        p.total_parts_produced,
        p.parts_per_minute,
        p.part_per_hour,
        p.target_parts,
        p.no_of_parts_rejected,
        p.good_parts,
        p.machine_availability_percent,
        p.machine_performance_percent,
        p.quality_percent,
        p.pass_yield,
        p.throughput_rate,
        p.quality_variability,
        p.ratio_actual_to_projected_unit_production_cost,
        p.production_to_wage_ratio,
        p.throughput,
        p.rate_of_return,
        p.total_production_per_batch,
        l.cycletime_deviation,
        l.cycletime_loss,
        l.idletime_loss_percent,
        l.downtime_loss,
        l.unplanned_downtime_loss,
        l.planned_downtime_loss,
        l.availability_loss_time,
        l.availability_loss_percent,
        l.performance_loss_percent,
        l.quality_loss,
        l.production_loss,
        l.manufacturing_capacity_loss_percent,
        l.line_balancing_loss,
        l.speed_loss,
        o.oee,
        o.oee_by_time,
        o.oee_in_availability,
        o.ooe,
        o.teep,
        o.capacity_utilized_percent,
        o.asset_utilization_percent,
        o.manufacturing_cost_percent,
        o.overall_machine_efficiency,
        o.maintenance_efficiency,
        o.corrective_maintenance_cost_percent,
        o.maintenance_cost_percent,
        o.average_production_cost_item,
        o.total_maintenance_cost,
        o.unit_cost,
        o.return_on_asset_investment,
        d.operator_id,
        d.supervisor_id,
        d.edgeid,
        d.shiftid,
        d.operating_time_deviation,
        d.throughput_deviation,
        d.oee_deviation,
        d.ole_deviation
    FROM
        press."time" t
        FULL OUTER JOIN press.production p ON t.tenantid = p.tenantid AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
        FULL OUTER JOIN press.losses l ON t.tenantid = l.tenantid AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
        FULL OUTER JOIN press.deviation d ON t.tenantid = d.tenantid AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
        FULL OUTER JOIN press.oee o ON t.tenantid = o.tenantid AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"));
END;
$$ LANGUAGE plpgsql;

--create materilized view
CREATE MATERIALIZED VIEW press.foundation_dm AS
SELECT *
FROM press.consolidate_data();

--
CREATE OR REPLACE FUNCTION refresh_consolidated_data_view()
RETURNS trigger AS $$
BEGIN
    PERFORM REFRESH MATERIALIZED VIEW press.consolidated_data_view;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--ccreate a triger
CREATE TRIGGER refresh_mv_after_insert
AFTER INSERT OR UPDATE OR DELETE ON press.deviation
FOR EACH STATEMENT EXECUTE FUNCTION refresh_consolidated_data_view();







select create_hypertable('press.time', 'date', migrate_data => true);
select create_hypertable('press.production', 'date', migrate_data => true);
select create_hypertable('press.oee', 'date', migrate_data => true);
select create_hypertable('press.deviation', 'date', migrate_data => true);












CREATE MATERIALIZED VIEW press.foundation
WITH (timescaledb.continuous) AS
select
    --MAX(t."timestamp") as timestamp,
    
    COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.v2tenant , p.v2tenant , l.v2tenant , d.v2tenant , o.v2tenant) AS v2tenant,
    COALESCE(t.org_id , p.org_id , l.org_id , d.org_id , o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    max(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
     -- Example aggregate function
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS avg_time_between_job_parts,
    AVG(t.actual_cycletime) AS avg_actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS avg_parts_per_minute,
    AVG(p.part_per_hour) AS avg_part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS avg_machine_availability_percent,
    AVG(p.machine_performance_percent) AS avg_machine_performance_percent,
    AVG(p.quality_percent) AS avg_quality_percent,
    AVG(p.pass_yield) AS avg_pass_yield,
    AVG(p.throughput_rate) AS avg_throughput_rate,
    AVG(p.quality_variability) AS avg_quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS avg_ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS avg_production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS avg_rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS avg_cycletime_deviation,
    AVG(l.cycletime_loss) AS avg_cycletime_loss,
    AVG(l.idletime_loss_percent) AS avg_idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS avg_availability_loss_percent,
    AVG(l.performance_loss_percent) AS avg_performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS avg_manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS avg_line_balancing_loss,
    AVG(l.speed_loss) AS avg_speed_loss,
    AVG(o.oee) AS avg_oee,
    AVG(o.oee_by_time) AS avg_oee_by_time,
    AVG(o.oee_in_availability) AS avg_oee_in_availability,
    AVG(o.ooe) AS avg_ooe,
    AVG(o.teep) AS avg_teep,
    AVG(o.capacity_utilized_percent) AS avg_capacity_utilized_percent,
    AVG(o.asset_utilization_percent) AS avg_asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS avg_manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS avg_overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS avg_maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS avg_corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS avg_maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS avg_average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS avg_unit_cost,
    AVG(o.return_on_asset_investment) AS avg_return_on_asset_investment,
    MAX(CAST(d.operator_id AS text)) AS operator_id,
	MAX(CAST(d.supervisor_id AS text)) AS supervisor_id,
	MAX(CAST(d.edgeid AS text)) AS edgeid,
	MAX(d.shiftid) AS shiftid, -- Assuming one shift ID per hour
    AVG(d.operating_time_deviation) AS avg_operating_time_deviation,
    AVG(d.throughput_deviation) AS avg_throughput_deviation,
    AVG(d.oee_deviation) AS avg_oee_deviation,
    AVG(d.ole_deviation) AS avg_ole_deviation
FROM
    press."time" t
    FULL OUTER JOIN press.production p ON t.tenantid = p.tenantid AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN press.losses l ON t.tenantid = l.tenantid AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN press.deviation d ON t.tenantid = d.tenantid AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN press.oee o ON t.tenantid = o.tenantid AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP BY
    COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.v2tenant , p.v2tenant , l.v2tenant , d.v2tenant , o.v2tenant),
    COALESCE(t.org_id , p.org_id , l.org_id , d.org_id , o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;
   
   
   
   
   ------*******______
CREATE MATERIALIZED VIEW press.foundation
WITH (timescaledb.continuous) AS
WITH time_data AS (
    SELECT
        tenantid,
        v2tenant,
        org_id,
        unit_id,
        department_id,
        machineid,
        time_bucket('1 hour', to_timestamp("timestamp" / 1000)) AS bucketed_hour,
        SUM(machine_up_time) AS total_machine_up_time,
        SUM(total_machine_runtime) AS total_machine_runtime,
        SUM(planned_production_time) AS planned_production_time,
        SUM(actual_production_time) AS actual_production_time,
        SUM(machine_idle_time) AS machine_idle_time,
        AVG(time_between_job_parts) AS avg_time_between_job_parts,
        AVG(actual_cycletime) AS avg_actual_cycletime,
        SUM(total_planned_downtime) AS total_planned_downtime,
        SUM(total_machine_downtime) AS total_machine_downtime,
        SUM(breakdowntime) AS breakdowntime,
        AVG(manhour_per_unit) AS avg_manhour_per_unit,
        SUM(unplanned_downtime) AS unplanned_downtime
    FROM press."time"
    GROUP BY tenantid, v2tenant, org_id, unit_id, department_id, machineid, bucketed_hour
),
production_data AS (
    SELECT
        tenantid,
        v2tenant,
        org_id,
        unit_id,
        department_id,
        machineid,
        time_bucket('1 hour', to_timestamp("timestamp" / 1000)) AS bucketed_hour,
        SUM(total_parts_produced) AS total_parts_produced,
        AVG(parts_per_minute) AS avg_parts_per_minute,
        AVG(part_per_hour) AS avg_part_per_hour,
        SUM(target_parts) AS total_target_parts,
        SUM(no_of_parts_rejected) AS total_no_of_parts_rejected,
        SUM(good_parts) AS total_good_parts,
        AVG(machine_availability_percent) AS avg_machine_availability_percent,
        AVG(machine_performance_percent) AS avg_machine_performance_percent,
        AVG(quality_percent) AS avg_quality_percent
    FROM press.production
    GROUP BY tenantid, v2tenant, org_id, unit_id, department_id, machineid, bucketed_hour
),
losses_data AS (
    SELECT
        tenantid,
        v2tenant,
        org_id,
        unit_id,
        department_id,
        machineid,
        time_bucket('1 hour', to_timestamp("timestamp" / 1000)) AS bucketed_hour,
        AVG(cycletime_deviation) AS avg_cycletime_deviation,
        AVG(cycletime_loss) AS avg_cycletime_loss,
        AVG(idletime_loss_percent) AS avg_idletime_loss_percent,
        SUM(downtime_loss) AS total_downtime_loss,
        SUM(unplanned_downtime_loss) AS total_unplanned_downtime_loss
    FROM press.losses
    GROUP BY tenantid, v2tenant, org_id, unit_id, department_id, machineid, bucketed_hour
),
deviation_data AS (
    SELECT
        tenantid,
        v2tenant,
        org_id,
        unit_id,
        department_id,
        machineid,
        time_bucket('1 hour', to_timestamp("timestamp" / 1000)) AS bucketed_hour,
        AVG(operating_time_deviation) AS avg_operating_time_deviation,
        AVG(throughput_deviation) AS avg_throughput_deviation
    FROM press.deviation
    GROUP BY tenantid, v2tenant, org_id, unit_id, department_id, machineid, bucketed_hour
),
oee_data AS (
    SELECT
        tenantid,
        v2tenant,
        org_id,
        unit_id,
        department_id,
        machineid,
        time_bucket('1 hour', to_timestamp("timestamp" / 1000)) AS bucketed_hour,
        AVG(oee) AS avg_oee,
        AVG(oee_by_time) AS avg_oee_by_time
    FROM press.oee
    GROUP BY tenantid, v2tenant, org_id, unit_id, department_id, machineid, bucketed_hour
)
SELECT
    COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.v2tenant , p.v2tenant , l.v2tenant , d.v2tenant , o.v2tenant) AS v2tenant,
    COALESCE(t.org_id , p.org_id , l.org_id , d.org_id , o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    t.bucketed_hour,
    t.total_machine_up_time,
    p.total_parts_produced,
    l.total_downtime_loss,
    d.avg_operating_time_deviation,
    o.avg_oee
FROM time_data t
FULL OUTER JOIN production_data p
    ON t.tenantid = p.tenantid
    AND t.unit_id = p.unit_id
    AND t.department_id = p.department_id
    AND t.machineid = p.machineid
    AND t.bucketed_hour = p.bucketed_hour
FULL OUTER JOIN losses_data l
    ON t.tenantid = l.tenantid
    AND t.unit_id = l.unit_id
    AND t.department_id = l.department_id
    AND t.machineid = l.machineid
    AND t.bucketed_hour = l.bucketed_hour
FULL OUTER JOIN deviation_data d
    ON t.tenantid = d.tenantid
    AND t.unit_id = d.unit_id
    AND t.department_id = d.department_id
    AND t.machineid = d.machineid
    AND t.bucketed_hour = d.bucketed_hour
FULL OUTER JOIN oee_data o
    ON t.tenantid = o.tenantid
    AND t.unit_id = o.unit_id
    AND t.department_id = o.department_id
    AND t.machineid = o.machineid
    AND t.bucketed_hour = o.bucketed_hour;

   
   
CREATE MATERIALIZED VIEW press.press_foundation AS
SELECT
    COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS avg_time_between_job_parts,
    AVG(t.actual_cycletime) AS avg_actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS avg_parts_per_minute,
    AVG(p.part_per_hour) AS avg_part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS avg_machine_availability_percent,
    AVG(p.machine_performance_percent) AS avg_machine_performance_percent,
    AVG(p.quality_percent) AS avg_quality_percent,
    AVG(p.pass_yield) AS avg_pass_yield,
    AVG(p.throughput_rate) AS avg_throughput_rate,
    AVG(p.quality_variability) AS avg_quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS avg_ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS avg_production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS avg_rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS avg_cycletime_deviation,
    AVG(l.cycletime_loss) AS avg_cycletime_loss,
    AVG(l.idletime_loss_percent) AS avg_idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS avg_availability_loss_percent,
    AVG(l.performance_loss_percent) AS avg_performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS avg_manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS avg_line_balancing_loss,
    AVG(l.speed_loss) AS avg_speed_loss,
    AVG(o.oee) AS avg_oee,
    AVG(o.oee_by_time) AS avg_oee_by_time,
    AVG(o.oee_in_availability) AS avg_oee_in_availability,
    AVG(o.ooe) AS avg_ooe,
    AVG(o.teep) AS avg_teep,
    AVG(o.capacity_utilized_percent) AS avg_capacity_utilized_percent,
    AVG(o.asset_utilization_percent) AS avg_asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS avg_manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS avg_overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS avg_maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS avg_corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS avg_maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS avg_average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS avg_unit_cost,
    AVG(o.return_on_asset_investment) AS avg_return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(t.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS avg_operating_time_deviation,
    AVG(d.throughput_deviation) AS avg_throughput_deviation,
    AVG(d.oee_deviation) AS avg_oee_deviation,
    AVG(d.ole_deviation) AS avg_ole_deviation
FROM
    press.time t
    FULL OUTER JOIN press.production p ON t.tenantid = p.tenantid AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN press.losses l ON t.tenantid = l.tenantid AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN press.deviation d ON t.tenantid = d.tenantid AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN press.oee o ON t.tenantid = o.tenantid AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP BY
    COALESCE(t.tenantid, p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;
   
   
   
   
DROP MATERIALIZED VIEW IF EXISTS cnc.foundation  ;
   
   
   
   
   
   
   -----------------------------------MV-----------------------------------------
   
   -------------------------------------------------------------------------------------------------------------------------------------------------
   
CREATE MATERIALIZED VIEW press.press_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(t.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    press.dm_time t 
    FULL OUTER JOIN press.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN press.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN press.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN press.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;


 drop materialized view press.press_mv_foundation 
 drop materialized view cnc.cnc_mv_foundation 

   
REFRESH MATERIALIZED VIEW press.foundation;
-------------------------------------------------------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW cnc.cnc_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,

    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_prod_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.total_unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    cnc.dm_time t 
    FULL OUTER JOIN cnc.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN cnc.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN cnc.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN cnc.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) ,

    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;


REFRESH MATERIALIZED VIEW press.press_mv_foundation;

-------------------------------------------------------------------------------------------------------------------------------------------------

CREATE MATERIALIZED view press.org_hierarchy AS
SELECT * FROM press.press_mv_foundation pmf 
UNION ALL
SELECT * FROM cnc.cnc_mv_foundation cmf 

----------------------------------------------------------------vmc ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW vmc.vmc_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    vmc.dm_time t 
    FULL OUTER JOIN vmc.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER join vmc.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN vmc.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN vmc.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;


------------------------------------------------------------------hmc--------------------------------------------------------------------------
CREATE MATERIALIZED VIEW hmc.hmc_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    hmc.dm_time t 
    FULL OUTER JOIN hmc.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN hmc.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN hmc.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN hmc.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;


---------------------------------------------------------extrusion_bopp-----------------------------------------------------------------------------------

CREATE MATERIALIZED view extrusion_bopp.extrusion_bopp_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    extrusion_bopp.dm_time t 
    FULL OUTER JOIN extrusion_bopp.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN extrusion_bopp.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN extrusion_bopp.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN extrusion_bopp.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;
   

-----------------------------------------------extrusion_bopt-------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW extrusion_bopt.extrusion_bopt_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    extrusion_bopt.dm_time t 
    FULL OUTER JOIN extrusion_bopt.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN extrusion_bopt.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN extrusion_bopt.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN extrusion_bopt.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

-----------------------------------------------------------------------vmc---------------------------------------------------------------------
CREATE MATERIALIZED VIEW extrusion_metalizer.extrusion_metalizer_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    extrusion_metalizer.dm_time t 
    FULL OUTER JOIN extrusion_metalizer.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN extrusion_metalizer.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN extrusion_metalizer.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN extrusion_metalizer.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

------------------------------------------------------------------------extrusion_pp--------------------------------------------------------------------
CREATE MATERIALIZED VIEW extrusion_pp.extrusion_pp_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    extrusion_pp.dm_time t 
    FULL OUTER JOIN extrusion_pp.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN extrusion_pp.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN extrusion_pp.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN extrusion_pp.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

----------------------------------------------------------------heattreatment_furnace----------------------------------------------------------------------------

   CREATE MATERIALIZED VIEW heattreatment_furnace.heattreatment_furnace_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
   heattreatment_furnace.dm_time t 
    FULL OUTER JOIN heattreatment_furnace.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN heattreatment_furnace.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN heattreatment_furnace.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN heattreatment_furnace.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

   
---------------------------------------------------------------hp_diecasting-----------------------------------------------------------------------------   
   
   CREATE MATERIALIZED VIEW hp_diecasting.hp_diecasting_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    hp_diecasting.dm_time t 
    FULL OUTER JOIN hp_diecasting.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN hp_diecasting.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN hp_diecasting.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN hp_diecasting.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

   
   
------------------------------------------------------------lp_diecasting--------------------------------------------------------------------------------
   CREATE MATERIALIZED VIEW lp_diecasting.lp_diecasting_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    lp_diecasting.dm_time t 
    FULL OUTER JOIN lp_diecasting.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN lp_diecasting.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN lp_diecasting.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN lp_diecasting.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

---------------------------------------------------------------melting_furnace-----------------------------------------------------------------------------   
CREATE MATERIALIZED VIEW melting_furnace.melting_furnace_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    melting_furnace.dm_time t 
    FULL OUTER JOIN melting_furnace.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN melting_furnace.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN melting_furnace.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN melting_furnace.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

   
---------------------------------------------------------metal_die_casting-----------------------------------------------------------------------------------   
   
CREATE MATERIALIZED VIEW metal_die_casting.metal_die_casting_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    metal_die_casting.dm_time t 
    FULL OUTER JOIN metal_die_casting.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN metal_die_casting.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN metal_die_casting.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN metal_die_casting.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;

--------------------------------------------------------------------metal_moulding------------------------------------------------------------------------   
CREATE MATERIALIZED VIEW metal_moulding.metal_moulding_mv_foundation AS
SELECT
    coalesce(t."date",p."date",l."date",d."date",o."date") as date,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant) AS v2tenant,
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid) AS tenantid,
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id) AS org_id,
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id) AS unit_id,
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id) AS department_id,
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid) AS machineid,
    MAX(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp")) AS "timestamp",
    time_bucket('1 hour', to_timestamp(COALESCE(t."timestamp", p."timestamp", l."timestamp", d."timestamp", o."timestamp") / 1000)) AS bucketed_hour,
    SUM(t.machine_up_time) AS total_machine_up_time,
    SUM(t.total_machine_runtime) AS total_machine_runtime,
    SUM(t.planned_production_time) AS planned_production_time,
    SUM(t.actual_production_time) AS actual_production_time,
    SUM(t.machine_idle_time) AS machine_idle_time,
    AVG(t.time_between_job_parts) AS time_between_job_parts,
    AVG(t.actual_cycletime) AS actual_cycletime,
    SUM(t.total_planned_downtime) AS total_planned_downtime,
    SUM(t.total_machine_downtime) AS total_machine_downtime,
    SUM(t.breakdowntime) AS breakdowntime,
    AVG(t.manhour_per_unit) AS avg_manhour_per_unit,
    SUM(t.unplanned_downtime) AS unplanned_downtime,
    SUM(p.total_parts_produced) AS total_parts_produced,
    AVG(p.parts_per_minute) AS parts_per_minute,
    AVG(p.part_per_hour) AS part_per_hour,
    SUM(p.target_parts) AS total_target_parts,
    SUM(p.no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(p.good_parts) AS total_good_parts,
    AVG(p.machine_availability_percent) AS machine_availability_percent,
    AVG(p.machine_performance_percent) As machine_performance_percent,
    AVG(p.quality_percent) AS quality_percent,
    AVG(p.pass_yield) AS pass_yield,
    AVG(p.throughput_rate) AS throughput_rate,
    AVG(p.quality_variability) AS quality_variability,
    AVG(p.ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(p.production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(p.throughput) AS total_throughput,
    AVG(p.rate_of_return) AS rate_of_return,
    SUM(p.total_production_per_batch) AS total_production_per_batch,
    AVG(l.cycletime_deviation) AS cycletime_deviation,
    AVG(l.cycletime_loss) AS cycletime_loss,
    AVG(l.idletime_loss_percent) AS idletime_loss_percent,
    SUM(l.downtime_loss) AS total_downtime_loss,
    SUM(l.unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(l.planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(l.availability_loss_time) AS total_availability_loss_time,
    AVG(l.availability_loss_percent) AS availability_loss_percent,
    AVG(l.performance_loss_percent) AS performance_loss_percent,
    SUM(l.quality_loss) AS total_quality_loss,
    SUM(l.production_loss) AS total_production_loss,
    AVG(l.manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(l.line_balancing_loss) AS line_balancing_loss,
    AVG(l.speed_loss) AS speed_loss,
    AVG(o.oee) AS oee,
    AVG(o.oee_by_time) AS oee_by_time,
    AVG(o.oee_in_availability) AS oee_in_availability,
    AVG(o.ooe) AS ooe,
    AVG(o.teep) AS teep,
    AVG(o.capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(o.asset_utilization_percent) as asset_utilization_percent,
    AVG(o.manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(o.overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(o.maintenance_efficiency) AS maintenance_efficiency,
    AVG(o.corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(o.maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(o.average_production_cost_item) AS average_production_cost_item,
    SUM(o.total_maintenance_cost) AS total_maintenance_cost,
    AVG(o.unit_cost) AS unit_cost,
    AVG(o.return_on_asset_investment) AS return_on_asset_investment,
    MAX(d.operator_id::text) AS operator_id,
    MAX(d.supervisor_id::text) AS supervisor_id,
    MAX(d.edgeid::text) AS edgeid,
    MAX(d.shiftid) AS shiftid,
    AVG(d.operating_time_deviation) AS operating_time_deviation,
    AVG(d.throughput_deviation) AS throughput_deviation,
    AVG(d.oee_deviation) AS oee_deviation,
    AVG(d.ole_deviation) AS ole_deviation
FROM
    metal_moulding.dm_time t 
    FULL OUTER JOIN metal_moulding.dm_production p ON t.v2tenant = p.v2tenant AND t.unit_id = p.unit_id AND t.department_id = p.department_id AND t.machineid = p.machineid AND t."date" = p."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(p."timestamp"))
    FULL OUTER JOIN metal_moulding.dm_losses l ON t.v2tenant = l.v2tenant AND t.unit_id = l.unit_id AND t.department_id = l.department_id AND t.machineid = l.machineid AND t."date" = l."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(l."timestamp"))
    FULL OUTER JOIN metal_moulding.dm_deviation d ON t.v2tenant = d.v2tenant AND t.unit_id = d.unit_id AND t.department_id = d.department_id AND t.machineid = d.machineid AND t."date" = d."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(d."timestamp"))
    FULL OUTER JOIN metal_moulding.dm_oee o ON t.v2tenant = o.v2tenant AND t.unit_id = o.unit_id AND t.department_id = o.department_id AND t.machineid = o.machineid AND t."date" = o."date" AND EXTRACT(HOUR FROM TO_TIMESTAMP(t."timestamp")) = EXTRACT(HOUR FROM TO_TIMESTAMP(o."timestamp"))
GROUP by
    coalesce(t."date",p."date",l."date",d."date",o."date") ,
    COALESCE(t.v2tenant, p.v2tenant, l.v2tenant, d.v2tenant, o.v2tenant),
    COALESCE(t.tenantid , p.tenantid, l.tenantid, d.tenantid, o.tenantid),
    COALESCE(t.org_id, p.org_id, l.org_id, d.org_id, o.org_id),
    COALESCE(t.unit_id, p.unit_id, l.unit_id, d.unit_id, o.unit_id),
    COALESCE(t.department_id, p.department_id, l.department_id, d.department_id, o.department_id),
    COALESCE(t.machineid, p.machineid, l.machineid, d.machineid, o.machineid),
    bucketed_hour;
-----------------------------------------------------------------------------------------------------------------------------------
   
CREATE MATERIALIZED view press.org_hierarchy AS
SELECT * FROM press.press_mv_foundation pmf 
UNION ALL
SELECT * FROM cnc.cnc_mv_foundation cmf 
union all 
select * from extrusion_bopp.extrusion_bopp_mv_foundation ebmf 
union all 
select * from 

select * from press.dm_time
where date = '2024-09-04'

 ---------------------------------------------------------------------------------------------------------------------------
   refresh materialized view press.press_mv_foundation
   refresh materialized view cnc.cnc_mv_foundation
   refresh materialized view universal.org_hierarchy 
---------------------------------------------------------------------------------------------------------------------------
   select * from universal.org_hierarchy oh  
   where date ='2024-09-06'
   
   select* from   press.press_mv_foundation pmf  
       date,
       v2tenant,
       machineid,
       tenantid,
       (p.machine_availability_percent) AS machine_availability_percent,
    (p.machine_performance_percent) As machine_performance_percent,
    (p.quality_percent) AS quality_percent
    from universal.org_hierarchy p
    where machineid ='VMC03'  and date ='2024-09-05'   
    
   
  ------------------------------------------------
    
  CREATE MATERIALIZED VIEW universal.org_hierarchy_machinewise_daily AS
  SELECT
    time_bucket('1 day', date) AS start_date,
    Max("timestamp") AS timestamp,
    tenantid,
    v2tenant,
    org_id ,
    unit_id,
    department_id,
    machineid,
    SUM(total_machine_up_time) AS total_machine_up_time,
    SUM(total_machine_runtime) AS total_machine_runtime,
    SUM(planned_production_time) AS planned_production_time,
    SUM(actual_production_time) AS actual_production_time,
    SUM(machine_idle_time) AS machine_idle_time,
    AVG(time_between_job_parts) AS time_between_job_parts,
    AVG(actual_cycletime) AS actual_cycletime,
    SUM(total_planned_downtime) AS total_planned_downtime,
    SUM(total_machine_downtime) AS total_machine_downtime,
    SUM(breakdowntime) AS breakdowntime,
    AVG(avg_manhour_per_unit) AS avg_manhour_per_unit,
    SUM(unplanned_downtime) AS unplanned_downtime,
    SUM(total_parts_produced) AS total_parts_produced,
    AVG(parts_per_minute) AS parts_per_minute,
    AVG(part_per_hour) AS part_per_hour,
    SUM(total_target_parts) AS total_target_parts,
    SUM(total_no_of_parts_rejected) AS total_no_of_parts_rejected,
    SUM(total_good_parts) AS total_good_parts,
    AVG(machine_availability_percent) AS machine_availability_percent,
    AVG(machine_performance_percent) AS machine_performance_percent,
    AVG(quality_percent) AS quality_percent,
    AVG(pass_yield) AS pass_yield,
    AVG(throughput_rate) AS throughput_rate,
    AVG(quality_variability) AS quality_variability,
    AVG(ratio_actual_to_projected_unit_production_cost) AS ratio_actual_to_projected_unit_production_cost,
    AVG(production_to_wage_ratio) AS production_to_wage_ratio,
    SUM(total_throughput) AS total_throughput,
    AVG(rate_of_return) AS rate_of_return,
    SUM(total_production_per_batch) AS total_production_per_batch,
    AVG(cycletime_deviation) AS cycletime_deviation,
    AVG(cycletime_loss) AS cycletime_loss,
    AVG(idletime_loss_percent) AS idletime_loss_percent,
    SUM(total_downtime_loss) AS total_downtime_loss,
    SUM(total_unplanned_downtime_loss) AS total_unplanned_downtime_loss,
    SUM(total_planned_downtime_loss) AS total_planned_downtime_loss,
    SUM(total_availability_loss_time) AS total_availability_loss_time,
    AVG(availability_loss_percent) AS availability_loss_percent,
    AVG(performance_loss_percent) AS performance_loss_percent,
    SUM(total_quality_loss) AS total_quality_loss,
    SUM(total_production_loss) AS total_production_loss,
    AVG(manufacturing_capacity_loss_percent) AS manufacturing_capacity_loss_percent,
    AVG(line_balancing_loss) AS line_balancing_loss,
    AVG(speed_loss) AS speed_loss,
    AVG(oee) AS oee,
    AVG(oee_by_time) AS oee_by_time,
    AVG(oee_in_availability) AS oee_in_availability,
    AVG(ooe) AS ooe,
    AVG(teep) AS teep,
    AVG(capacity_utilized_percent) AS capacity_utilized_percent,
    AVG(asset_utilization_percent) AS asset_utilization_percent,
    AVG(manufacturing_cost_percent) AS manufacturing_cost_percent,
    AVG(overall_machine_efficiency) AS overall_machine_efficiency,
    AVG(maintenance_efficiency) AS maintenance_efficiency,
    AVG(corrective_maintenance_cost_percent) AS corrective_maintenance_cost_percent,
    AVG(maintenance_cost_percent) AS maintenance_cost_percent,
    AVG(average_production_cost_item) AS average_production_cost_item,
    SUM(total_maintenance_cost) AS total_maintenance_cost,
    AVG(unit_cost) AS unit_cost,
    AVG(return_on_asset_investment) AS return_on_asset_investment,
    AVG(operating_time_deviation) AS operating_time_deviation,
    AVG(throughput_deviation) AS throughput_deviation,
    AVG(oee_deviation) AS oee_deviation,
    AVG(ole_deviation) AS ole_deviation
   from universal.org_hierarchy 
   group by tenantid,v2tenant ,org_id ,unit_id ,department_id ,machineid ,start_date 
   

refresh materialized view universal.org_hierarchy_machinewise_daily
    
  ------------------------------------------------- 
   
 
   
   select * from universal.org_hierarchy_machinewise_daily 
   where start_date  ='2024-09-0'
   
   
   
   drop materialized view press.org_hierarchy

select * from universal.org_hierarchy




select * from cnc.foundation
'
select * from press.foundation f 



   
   
CREATE UNIQUE INDEX foundation_idx
ON foundation (date, tenantid);

DROP INDEX dm_org_tenant_weekly_view_idx;
   
 
CREATE TRIGGER refresh_materialized_view_trigger
AFTER INSERT OR UPDATE OR DELETE
ON press."time" 
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_materialized_view();


CREATE OR REPLACE FUNCTION refresh_materialized_view_function()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY press.foundation;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

SELECT refresh_materialized_view_function();




SELECT * FROM timescaledb_information.jobs;


select * from press.dm_time  
WHERE date < '2024-05-01' AND tenantid = 18;

UPDATE press.dm_oee  
SET 
    org_id = '86ce13e4-3c18-4963-a759-6dcbff05fae2',
    unit_id = 'b0d86845-adf3-4c0e-bac3-7149703a5be5',
    department_id = '38ec403e-6462-452f-a208-b3e68345527c',
    v2tenant = '57bc407b-1138-4663-9b25-35d425800c73'
WHERE 
    date < '2024-05-01' AND tenantid = 18;





