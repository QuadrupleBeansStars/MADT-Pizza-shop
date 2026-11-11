"""
test_pizza_pipeline.py: the checks that CI runs on every pull request.

Run it yourself:   pip install pytest pandas numpy   then   pytest pipeline -q

How to read a test: it has a name that says what must be true, then three steps.
    arrange (set up the data), act (run the code), assert (check the result).
If one assert is false the test fails, the pull request turns red, and nobody can merge.
"""
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))
import pizza_pipeline as pp  # noqa: E402

D = lambda i: pp.HISTORY_START + timedelta(days=i)  # noqa: E731


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """Run the nightly pipeline for 28 days once, and share the result between tests."""
    work = tmp_path_factory.mktemp("lab")
    lake, src = pp.new_lake(work / "lake"), work / "sources"
    truth = pp.build_history(lake, src, 28)
    return {"lake": lake, "src": src, "truth": truth, "mart": pp.read_mart(lake)}


# ---------- 1. business rules: small, fast, no files needed ----------------------------------------
def test_order_after_midnight_belongs_to_the_day_before():
    ts = pd.Series([pd.Timestamp("2026-09-12 00:40", tz=pp.BKK), pd.Timestamp("2026-09-12 10:00", tz=pp.BKK)])
    assert list(pp.business_date(ts)) == [date(2026, 9, 11), date(2026, 9, 12)]


def test_every_item_in_every_app_has_a_menu_key():
    for sku in pp.SKUS:
        assert ("GRAB", pp.GRAB_NAME[sku]) in pp.NAME_MAP
        assert ("LINEMAN", pp.LM_NAME[sku]) in pp.NAME_MAP


# ---------- 2. the pipeline end to end ---------------------------------------------------------------
def test_mart_recovers_the_units_we_planted(built):
    got = built["mart"].groupby("business_date").units_sold.sum()
    want = pd.Series({d: sum(v.values()) for d, v in built["truth"].items()})
    assert (got == want.reindex(got.index)).all()


def test_messy_sources_really_were_messy_and_got_cleaned(built):
    stats = pp.process_day(built["lake"], D(3))
    assert stats["duplicates_removed"] > 0          # POS twice-exported orders, repeated Grab orders


def test_rerunning_a_day_gives_the_same_answer(built):
    assert pp.process_day(built["lake"], D(5)) == pp.process_day(built["lake"], D(5))


def test_no_negative_or_missing_numbers_in_the_mart(built):
    m = built["mart"]
    assert (m.units_sold >= 0).all() and m.notna().all().all()


def test_unknown_item_name_stops_the_job(tmp_path):
    lake, src = pp.new_lake(tmp_path / "lake"), tmp_path / "src"
    day = D(0)
    pp.generate_day(day, src, rain=False)
    f = next((src / "grab").glob("*.json"))
    f.write_text(f.read_text().replace("Large Pepperoni Pizza", "Pepperoni XXL (new!)"))   # the app renamed an item
    with pytest.raises(pp.QualityError):
        pp.run_nightly(lake, src, day)
    assert not (lake / "trusted" / "fct_order_line").exists()      # nothing was published


# ---------- 3. the model ------------------------------------------------------------------------------
def test_model_beats_the_baseline_on_calm_days(built):
    d = pp.with_calendar(built["mart"], pp.calendar(pp.HISTORY_START, 28))
    train, valid = pp.split_by_date(d, D(0), D(20)), pp.split_by_date(d, D(21), D(27))
    model = pp.RidgeForecaster().fit(train)
    assert pp.wape(valid.units_sold, model.predict(valid)) < pp.wape(valid.units_sold, pp.baseline_predict(d, valid))


def test_forecasts_are_never_negative(built):
    d = pp.with_calendar(built["mart"], pp.calendar(pp.HISTORY_START, 28))
    assert (pp.RidgeForecaster().fit(d).predict(d) >= 0).all()


# ---------- 4. the promotion rule and the registry ----------------------------------------------------
@pytest.mark.parametrize("cand, prod, base, expected", [
    ({"wape": 0.30}, None, 0.40, True),                  # first model, beats baseline
    ({"wape": 0.45}, None, 0.40, False),                 # worse than baseline
    ({"wape": 0.33}, {"wape": 0.38}, 0.40, True),        # clearly better than production
    ({"wape": 0.379}, {"wape": 0.38}, 0.40, False),      # better, but not by enough
    ({"wape": float("nan")}, None, 0.40, False),         # broken metric
])
def test_promotion_rule(cand, prod, base, expected):
    assert pp.promotion_decision(cand, prod, base)[0] is expected


def test_registry_keeps_one_production_and_can_roll_back(tmp_path, built):
    d = pp.with_calendar(built["mart"], pp.calendar(pp.HISTORY_START, 28))
    reg = pp.ModelRegistry(tmp_path / "registry.json")
    v1 = reg.register(pp.RidgeForecaster().fit(d), {"wape": 0.3}, (D(0), D(27)))
    reg.promote(v1)
    v2 = reg.register(pp.RidgeForecaster(alpha=5).fit(d), {"wape": 0.25}, (D(0), D(27)))
    reg.promote(v2)
    assert [x["stage"] for x in reg.data["versions"]] == ["archived", "production"]
    assert reg.rollback() == v1
    assert reg.production()["version"] == v1
