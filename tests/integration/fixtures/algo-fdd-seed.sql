-- algo-fdd-seed.sql（DAT-154 集成夹具）：在 e2e-seed 之上叠加 FDD 语义面。
-- 幂等：INSERT ON CONFLICT DO NOTHING + UPDATE 天然可重跑（standing dev 栈复用）。
-- 设备（挂 E2E 一号楼冷冻/冷却水系统）：
--   CH-1 冷机（额定 50kW）、CH-2 冷机（语义不完整 → 二维路由跳过的负路径）、
--   CT-1 冷却塔、P-1 冷冻水泵（40kW）、P-2 冷却水泵（40kW）。
-- 点位映射（网关 GW-SIM-001 的 SIM_0000..0015）：
--   0000 CH-1 chw_supply_temp（degC 恒等）；0001 CH-1 chw_return_temp（degF→degC，喂单位归一路径）
--   0002 CH-1 power（kW）；0003 CT-1 冷却水供水（degC）；0004 CT-1 冷却水回水（degC）
--   0005 P-1 power；0006 P-2 power；0007 CH-2 chw_supply_temp（唯一量 → 规则不适用）
--   0008..0011 未指派数值点（保持 seed 原样）；0012..0015 run_status=running（CH-1/CT-1/P-1/P-2）
BEGIN;

INSERT INTO hvac_system (id, tenant_id, building_id, system_type, name) VALUES
  ('44444444-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111',
   '22222222-2222-2222-2222-222222222222', 'chilled_water', 'E2E 冷冻水系统'),
  ('44444444-0000-0000-0000-000000000002', '11111111-1111-1111-1111-111111111111',
   '22222222-2222-2222-2222-222222222222', 'cooling_water', 'E2E 冷却水系统')
ON CONFLICT (id) DO NOTHING;

INSERT INTO equipment (id, tenant_id, system_id, equipment_type, name, local_id, rated_params) VALUES
  ('55555555-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000001', 'chiller', 'E2E 一号冷机', '1#冷机',
   '{"rated_power_kw": 50}'),
  ('55555555-0000-0000-0000-000000000002', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000001', 'chiller', 'E2E 二号冷机', '2#冷机',
   '{"rated_power_kw": 50}'),
  ('55555555-0000-0000-0000-000000000003', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000002', 'cooling_tower', 'E2E 一号冷却塔', '1#塔', '{}'),
  ('55555555-0000-0000-0000-000000000004', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000001', 'chwp_pump', 'E2E 冷冻水泵', 'CHWP-1',
   '{"rated_power_kw": 40}'),
  ('55555555-0000-0000-0000-000000000005', '11111111-1111-1111-1111-111111111111',
   '44444444-0000-0000-0000-000000000002', 'cwp_pump', 'E2E 冷却水泵', 'CWP-1',
   '{"rated_power_kw": 40}')
ON CONFLICT (id) DO NOTHING;

-- 点位语义 UPDATE（网关 1；point 复合唯一不含 raw_name，按 gateway+raw_name 定位）
WITH gw AS (
  SELECT id FROM gateway WHERE mqtt_client_id = 'GW-SIM-001'
)
UPDATE point p SET
  equipment_id   = v.equipment_id,
  quantity_type  = v.quantity_type,
  unit_raw       = v.unit_raw,
  unit_std       = v.unit_std
FROM (VALUES
  ('SIM_0000', '55555555-0000-0000-0000-000000000001'::uuid, 'chw_supply_temp',            'degC', 'degC'),
  ('SIM_0001', '55555555-0000-0000-0000-000000000001'::uuid, 'chw_return_temp',            'degF', 'degC'),
  ('SIM_0002', '55555555-0000-0000-0000-000000000001'::uuid, 'power',                      'kW',   'kW'),
  ('SIM_0003', '55555555-0000-0000-0000-000000000003'::uuid, 'cooling_water_supply_temp',  'degC', 'degC'),
  ('SIM_0004', '55555555-0000-0000-0000-000000000003'::uuid, 'cooling_water_return_temp',  'degC', 'degC'),
  ('SIM_0005', '55555555-0000-0000-0000-000000000004'::uuid, 'power',                      'kW',   'kW'),
  ('SIM_0006', '55555555-0000-0000-0000-000000000005'::uuid, 'power',                      'kW',   'kW'),
  ('SIM_0007', '55555555-0000-0000-0000-000000000002'::uuid, 'chw_supply_temp',            'degC', 'degC'),
  ('SIM_0012', '55555555-0000-0000-0000-000000000001'::uuid, 'run_status',                 NULL,   NULL),
  ('SIM_0013', '55555555-0000-0000-0000-000000000003'::uuid, 'run_status',                 NULL,   NULL),
  ('SIM_0014', '55555555-0000-0000-0000-000000000004'::uuid, 'run_status',                 NULL,   NULL),
  ('SIM_0015', '55555555-0000-0000-0000-000000000005'::uuid, 'run_status',                 NULL,   NULL)
) AS v(raw_name, equipment_id, quantity_type, unit_raw, unit_std)
JOIN gw ON true
WHERE p.raw_name = v.raw_name AND p.gateway_id = gw.id;

COMMIT;

-- 自检：语义面就位（it-fdd.sh 校验期望计数）
SELECT 'fdd_equipments' AS k, count(*)::text AS v FROM equipment
UNION ALL SELECT 'fdd_points_linked', count(*)::text FROM point WHERE equipment_id IS NOT NULL;
