"""saving 公式库与置信度公式单测（optimizer.md §8——确定性可复算，单测钉死）。"""

from __future__ import annotations

import pytest
from algo.optimizer.confidence import (
    F_DATA_PROXY,
    F_FCST_PERSISTENCE,
    F_PARAM_DEFAULT,
    ConfidenceInput,
    compute,
    f_stab,
)
from algo.optimizer.saving import (
    REGISTRY,
    AffineModel,
    e1_temp_reset,
    e2_stage_down,
    e3_stage_split,
    e4_condenser,
    fit_affine,
    rated_linearize,
)


class TestFormulas:
    def test_registry_covers_all_four(self) -> None:
        assert REGISTRY.ids() == frozenset(
            {"E1_temp_reset", "E2_stage_down", "E3_stage_split", "E4_condenser"}
        )

    def test_e1_dimension(self) -> None:
        # 2%/°C × 0.5°C × 600 kW = 6.0 kW（§6.1 算例）
        assert e1_temp_reset(0.5, 600.0, 0.02) == pytest.approx(6.0)

    def test_e2_positive_for_high_idle_unit(self) -> None:
        stop = AffineModel(p0_kw=56.0, m_kw_per_kwth=0.186)
        remain = AffineModel(p0_kw=56.0, m_kw_per_kwth=0.186)
        assert e2_stage_down(stop, remain, q_stop_kw_th=753.0) == pytest.approx(56.0)

    def test_e3_negative_when_splitting_adds_idle(self) -> None:
        model = AffineModel(p0_kw=56.0, m_kw_per_kwth=0.1858)
        # 单机 400 kW（Q≈2010 kW_th）分两台各半：多付一台怠机截距 → 负节省（保护性）
        p_now = 400.0
        saving = e3_stage_split(p_now, [(model, 1004.0), (model, 1004.0)])
        assert saving == pytest.approx(400.0 - 2 * (56.0 + 0.1858 * 1004.0))
        assert saving < 0

    def test_e4_net_of_fan_penalty(self) -> None:
        # 1%/°C × 1°C × 700 = 7；fan 5 kW × min(1, 1×0.5) = 2.5 → 净 4.5
        assert e4_condenser(1.0, 700.0, 0.01, 5.0, 0.5) == pytest.approx(4.5)
        # fan_penalty 封顶 1.0×step×pct
        assert e4_condenser(2.0, 700.0, 0.01, 5.0, 0.5) == pytest.approx(14.0 - 5.0)


class TestAffineFit:
    def test_perfect_line_fits(self) -> None:
        samples = [(q, 50.0 + 0.2 * q) for q in range(100, 148, 2)]  # 24 点
        m = fit_affine(samples)
        assert m is not None and not m.fallback
        assert m.m_kw_per_kwth == pytest.approx(0.2, rel=1e-9)
        assert m.p0_kw == pytest.approx(50.0, rel=1e-9)

    def test_insufficient_samples_fallback(self) -> None:
        assert fit_affine([(100, 50), (200, 60)]) is None

    def test_constant_q_unidentifiable(self) -> None:
        assert fit_affine([(100.0, 50.0 + i * 0.1) for i in range(30)]) is None

    def test_low_r2_rejected(self) -> None:
        # 大噪声 → R² < 0.7 → None
        noisy = [(q, 50.0 + 0.2 * q + (i % 7) * 40.0) for i, q in enumerate(range(100, 148, 2))]
        assert fit_affine(noisy) is None

    def test_negative_intercept_rejected(self) -> None:
        # 陡斜率下拟合出负截距（物理不成立）→ 弃用
        samples = [(q, 0.9 * q - 30.0) for q in range(100, 148, 2)]
        assert fit_affine(samples) is None

    def test_rated_linearize_conservative_positive_intercept(self) -> None:
        m = rated_linearize(4220.0, 840.0, 0.25, 0.3)
        assert m.fallback
        assert m.p0_kw == pytest.approx(56.0, rel=1e-3)  # 0.3×840 − m×0.25×4220
        assert m.at(4220.0) == pytest.approx(840.0, rel=1e-9)  # 额定点穿过


class TestConfidence:
    def test_design_example_r2(self) -> None:
        # §8.3 算例：0.80 × 1.0 × 0.75 × 0.8 × 0.8 = 0.38（f_stab=0.75 ⇔ CV=0.05）
        v = compute(
            ConfidenceInput(
                cap=0.80, f_data=1.0, cv=0.05, f_param=F_PARAM_DEFAULT, f_fcst=F_FCST_PERSISTENCE
            )
        )
        assert v == 0.38

    def test_proxy_lower_than_measured(self) -> None:
        base = dict(cap=0.85, cv=None, f_param=1.0)
        assert compute(ConfidenceInput(f_data=F_DATA_PROXY, **base)) < compute(
            ConfidenceInput(f_data=1.0, **base)
        )

    def test_monotonic_in_cv(self) -> None:
        vals = [
            compute(ConfidenceInput(cap=0.85, cv=cv, f_param=1.0))
            for cv in (0.0, 0.05, 0.10, 0.20, 0.30)
        ]
        assert vals == sorted(vals, reverse=True)

    def test_bounded(self) -> None:
        assert compute(ConfidenceInput(cap=1.0, cv=-5, f_param=1.0)) == 1.0
        assert compute(ConfidenceInput(cap=0.5, cv=None, f_param=0.0)) == 0.0

    def test_f_stab_clamps(self) -> None:
        assert f_stab(None) == 1.0
        assert f_stab(0.0) == 1.0
        assert f_stab(0.20) == 0.0
        assert f_stab(0.10) == pytest.approx(0.5)
