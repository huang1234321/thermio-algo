-- algo-optimizer-seed.sql（DAT-165 / IMPL-19 集成夹具）：E2E 冷源站房种子数据口径。
-- 在 e2e-seed + algo-fdd-seed 之上叠加优化器语义面（幂等：ON CONFLICT + UPDATE 可重跑）。
--
-- 站房（独立系统，与 FDD 夹具的 E2E 冷冻水系统隔离——GW-SIM-001 点位段 0100.. 不与
-- FDD 夹具 0000..0015 重叠，回放互不污染）：
--   系统 OP-SYS（chilled_water）×1；冷机 CH-OP-1/CH-OP-2（rated_params 按 §6.6-2 键名：
--   rated_cooling_capacity_kw=4220 ≈ 1200RT、rated_input_power_kw=840、min_unload_ratio=0.25、
--   rated_cop=5.024）；冷却塔 CT-OP-1（rated_fan_power_kw=30）。
-- 点位映射（网关 GW-SIM-001 的 SIM_0100..0112，quantity_type 为 IMPL-19 增量值）：
--   0100 CH-OP-1 chw_supply_temp（degC）；0101 CH-OP-1 chw_return_temp（degC）
--   0102 CH-OP-1 power（kW）；0103 CH-OP-1 run_status（枚态）
--   0104 CH-OP-1 unit_enable（dimensionless 0/1，readwrite + clamp 0..1）
--   0105 CH-OP-1 chw_supply_temp_setpoint（degC，readwrite + clamp 5..9，R1 目标）
--   0106 CH-OP-2 chw_supply_temp_setpoint（同上；单点粒度 advisory 闸负路径用）
--   0107 CH-OP-2 power；0108 CH-OP-2 run_status；0109 CH-OP-2 unit_enable（R2/R3 目标）
--   0110 CT-OP-1 cooling_water_supply_temp（degC）；0111 CT-OP-1 tower_fan_power（kW）
--   0112 CH-OP-1 cw_supply_temp_setpoint（degC，readwrite + clamp 18..32，R4 目标）
-- 纪律：目标点 is_controllable=true + clamp 值域（M8 闸门参数面）；control_mode 保持
-- advisory（MVP 红线：策略只对 advisory 点位产提案）。
BEGIN;

INSERT INTO hvac_system (id, tenant_id, building_id, system_type, name) VALUES
  ('44444444-0000-0000-0000-000000000003', '11111111-1111-1111-1111-111111111111',
   '22222222-2222-2222-2222-222222222222', 'chilled_water', 'E2E 优化器冷源系统')
ON CONFLICT (id) DO NOTHING;

INSERT INTO equipment (id, tenant_id, system_id, equipment_type, name, local_id, rated_params) VALUES
  ('55555555-0000-0000-0000-000000000011', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000003', 'chiller', 'E2E 优化器一号冷机', '1#机',
   '{"rated_cooling_capacity_kw": 4220, "rated_input_power_kw": 840, "min_unload_ratio": 0.25, "rated_cop": 5.024, "rated_power_kw": 840}'),
  ('55555555-0000-0000-0000-000000000012', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000003', 'chiller', 'E2E 优化器二号冷机', '2#机',
   '{"rated_cooling_capacity_kw": 4220, "rated_input_power_kw": 840, "min_unload_ratio": 0.25, "rated_cop": 5.024, "rated_power_kw": 840}'),
  ('55555555-0000-0000-0000-000000000013', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000003', 'cooling_tower', 'E2E 优化器一号冷却塔', '1#塔',
   '{"rated_fan_power_kw": 30}')
ON CONFLICT (id) DO NOTHING;

-- 点位语义 UPDATE（网关 1；幂等定位 gateway + raw_name）
WITH gw AS (
  SELECT id FROM gateway WHERE mqtt_client_id = 'GW-SIM-001'
)
UPDATE point p SET
  equipment_id   = v.equipment_id,
  quantity_type  = v.quantity_type,
  unit_raw       = v.unit_raw,
  unit_std       = v.unit_std,
  direction      = v.direction,
  is_controllable = v.is_controllable,
  clamp_min      = v.clamp_min,
  clamp_max      = v.clamp_max,
  control_mode   = 'advisory'
FROM (VALUES
  ('SIM_0100', '55555555-0000-0000-0000-000000000011'::uuid, 'chw_supply_temp',            'degC',         'degC',          'read',      false, NULL::numeric, NULL::numeric),
  ('SIM_0101', '55555555-0000-0000-0000-000000000011'::uuid, 'chw_return_temp',            'degC',         'degC',          'read',      false, NULL, NULL),
  ('SIM_0102', '55555555-0000-0000-0000-000000000011'::uuid, 'power',                      'kW',           'kW',            'read',      false, NULL, NULL),
  ('SIM_0103', '55555555-0000-0000-0000-000000000011'::uuid, 'run_status',                 NULL,           NULL,            'read',      false, NULL, NULL),
  ('SIM_0104', '55555555-0000-0000-0000-000000000011'::uuid, 'unit_enable',                'dimensionless','dimensionless', 'readwrite', true,  0, 1),
  ('SIM_0105', '55555555-0000-0000-0000-000000000011'::uuid, 'chw_supply_temp_setpoint',   'degC',         'degC',          'readwrite', true,  5, 9),
  ('SIM_0106', '55555555-0000-0000-0000-000000000012'::uuid, 'chw_supply_temp_setpoint',   'degC',         'degC',          'readwrite', true,  5, 9),
  ('SIM_0107', '55555555-0000-0000-0000-000000000012'::uuid, 'power',                      'kW',           'kW',            'read',      false, NULL, NULL),
  ('SIM_0108', '55555555-0000-0000-0000-000000000012'::uuid, 'run_status',                 NULL,           NULL,            'read',      false, NULL, NULL),
  ('SIM_0109', '55555555-0000-0000-0000-000000000012'::uuid, 'unit_enable',                'dimensionless','dimensionless', 'readwrite', true,  0, 1),
  ('SIM_0110', '55555555-0000-0000-0000-000000000013'::uuid, 'cooling_water_supply_temp',  'degC',         'degC',          'read',      false, NULL, NULL),
  ('SIM_0111', '55555555-0000-0000-0000-000000000013'::uuid, 'tower_fan_power',            'kW',           'kW',            'read',      false, NULL, NULL),
  ('SIM_0112', '55555555-0000-0000-0000-000000000011'::uuid, 'cw_supply_temp_setpoint',    'degC',         'degC',          'readwrite', true,  18, 32)
) AS v(raw_name, equipment_id, quantity_type, unit_raw, unit_std, direction,
       is_controllable, clamp_min, clamp_max)
JOIN gw ON true
WHERE p.raw_name = v.raw_name AND p.gateway_id = gw.id;

COMMIT;

-- 自检：优化器语义面就位
SELECT 'optimizer_equipments' AS k, count(*)::text AS v FROM equipment
WHERE system_id = '44444444-0000-0000-0000-000000000003'
UNION ALL
SELECT 'optimizer_points_linked', count(*)::text FROM point
WHERE raw_name LIKE 'SIM_01%' AND equipment_id IS NOT NULL;
