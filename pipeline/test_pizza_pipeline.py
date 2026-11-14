"""
test_pizza_pipeline.py: the checks that CI runs on every pull request.

Run it yourself:   pip install pytest pandas numpy   then   pytest pipeline -q

How to read a test: it has a name that says what must be true, then three steps.
    arrange (set up the data), act (run the code), assert (check the result).
If one assert is false the test fails, the pull request turns red, and nobody can merge.
"""
import shutil
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


# ---------- 4. when the job fails: retry or not, and who is told ---------------------------------------
def _sources(tmp_path, day):
    full = tmp_path / "full"
    pp.generate_day(day, full, rain=False)
    partial = tmp_path / "partial"
    shutil.copytree(full / "pos", partial / "pos")
    shutil.copytree(full / "lineman", partial / "lineman")          # Grab has not delivered yet
    return full, partial


def test_a_late_file_is_retried_and_nobody_is_paged(tmp_path):
    lake, day = pp.new_lake(tmp_path / "lake"), D(0)
    full, partial = _sources(tmp_path, day)
    waits = []

    def grab_arrives(seconds):
        waits.append(seconds)
        shutil.copytree(full / "grab", partial / "grab", dirs_exist_ok=True)

    out = pp.run_with_retry(lambda: pp.run_nightly(lake, partial, day), sleep=grab_arrives)
    assert out["status"] == "ok" and out["attempts"] == 2 and waits == [45 * 60]
    assert not [a for a in out["alerts"] if a["level"] == "page"]


def test_a_file_that_never_comes_pages_a_human_after_the_last_try(tmp_path):
    lake, day = pp.new_lake(tmp_path / "lake"), D(0)
    _, partial = _sources(tmp_path, day)
    out = pp.run_with_retry(lambda: pp.run_nightly(lake, partial, day), sleep=lambda s: None)
    assert out["status"] == "failed" and out["attempts"] == 3
    assert [a["level"] for a in out["alerts"]].count("page") == 1
    assert not (lake / "trusted" / "fct_order_line").exists()      # a half-empty day was NOT published


def test_bad_data_is_not_retried(tmp_path):
    lake, src, day = pp.new_lake(tmp_path / "lake"), tmp_path / "src", D(0)
    pp.generate_day(day, src, rain=False)
    f = next((src / "grab").glob("*.json"))
    f.write_text(f.read_text().replace("Large Pepperoni Pizza", "Pepperoni XXL (new!)"))
    out = pp.run_with_retry(lambda: pp.run_nightly(lake, src, day), sleep=lambda s: None)
    assert out["status"] == "failed" and out["attempts"] == 1       # same input, same failure: do not wait
    assert out["alerts"][-1]["level"] == "page"


# ---------- 5. ordering stock: the forecast must reach the end of the order cycle -----------------------
MONDAY = date(2026, 10, 5)


def test_fresh_stock_cycles_are_three_and_four_days():
    mon = pp.order_cycle(MONDAY, (0, 3), 1)
    thu = pp.order_cycle(MONDAY + timedelta(days=3), (0, 3), 1)
    assert (mon["days_covered"], mon["horizon_days"]) == (3, 3)      # Mon order: Tue, Wed, Thu
    assert (thu["days_covered"], thu["horizon_days"]) == (4, 4)      # Thu order: Fri, Sat, Sun, Mon


def test_weekly_stock_needs_the_longest_horizon():
    c = pp.order_cycle(MONDAY, (0,), 2)
    assert (c["days_covered"], c["horizon_days"]) == (7, 8)


def test_order_cycles_leave_no_gap_and_no_overlap():
    day, covered = MONDAY, []
    for _ in range(6):
        c = pp.order_cycle(day, (0, 3), 1)
        covered += [c["first_day"] + timedelta(days=k) for k in range(c["days_covered"])]
        day = c["last_day"] + timedelta(days=1) - timedelta(days=1)       # the next order day is the day before the next arrival
        while day.weekday() not in (0, 3):
            day += timedelta(days=1)
    assert covered == sorted(set(covered))
    assert all((b - a).days == 1 for a, b in zip(covered, covered[1:]))
