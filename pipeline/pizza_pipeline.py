"""
pizza_pipeline.py: the whole pizza shop data pipeline in one file, plus the forecast model.

Read it top to bottom. Each section is one job:

    1. Reference data      menu, prices, how each delivery app names our pizzas
    2. Fake sources        what the POS, Grab and LINE MAN hand us (messy on purpose)
    3. Lake folders        landing -> raw -> trusted -> refined, all plain files
    4. Pipeline            land, ingest, process (clean + dedupe + gates), build marts
    5. Forecast model      baseline, ridge model, scoring
    6. Model registry      versions, stages, the promotion rule
    7. Monitoring          predicted vs actual, drift check

It only needs pandas and numpy, so it runs anywhere. The storage is CSV and JSON files
so you can open every file by hand. In the real platform these would be Parquet files and
a DuckDB / warehouse (see notebooks/data_flow_demo.ipynb). The logic does not change.

The notebook notebooks/cicd_mlops_education.ipynb walks through this file step by step.
"""
from __future__ import annotations

import json
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

BKK = "Asia/Bangkok"


# =====================================================================================
# 1. REFERENCE DATA
# =====================================================================================
MENU = pd.DataFrame(
    [  # menu_item_nk, item_name, category, size_code, base_price_thb
        ("PEP-L", "Pepperoni", "pizza", "L", 329),
        ("PEP-M", "Pepperoni", "pizza", "M", 259),
        ("MAR-M", "Margherita", "pizza", "M", 219),
        ("HAW-L", "Hawaiian", "pizza", "L", 309),
        ("COKE", "Coke 325ml", "drink", "-", 35),
    ],
    columns=["menu_item_nk", "item_name", "category", "size_code", "base_price_thb"],
)
SKUS = list(MENU.menu_item_nk)
PRICE = dict(zip(MENU.menu_item_nk, MENU.base_price_thb))
CHANNELS = ["QR_DINEIN", "PHONE", "GRAB", "LINEMAN"]

# each delivery app has its own name and its own (higher) price for our items
GRAB_NAME = {"PEP-L": "Large Pepperoni Pizza", "PEP-M": "Medium Pepperoni Pizza",
             "MAR-M": "Medium Margherita Pizza", "HAW-L": "Large Hawaiian Pizza", "COKE": "Coca-Cola 325ml"}
LM_NAME = {"PEP-L": "Pepperoni Pizza (L)", "PEP-M": "Pepperoni Pizza (M)",
           "MAR-M": "Margherita Pizza (M)", "HAW-L": "Hawaiian Pizza (L)", "COKE": "Coke"}
APP_PRICE = {"PEP-L": 359, "PEP-M": 285, "MAR-M": 239, "HAW-L": 339, "COKE": 40}

# the ONE place that knows how every channel names our items
NAME_MAP = {("GRAB", n): sku for sku, n in GRAB_NAME.items()}
NAME_MAP.update({("LINEMAN", n): sku for sku, n in LM_NAME.items()})


def business_date(ts: pd.Series) -> pd.Series:
    """The shop trades 10:00 to 02:00. Anything before 10:00 belongs to the day before."""
    return (ts - pd.Timedelta(hours=10)).dt.date


# =====================================================================================
# 2. FAKE SOURCES  (what the outside world sends us)
# =====================================================================================
# The "truth" behind the fake sales. Real life has no such table; we use it here so we can
# check that the pipeline and the model find what we planted.
ITEM_BASE = {"PEP-L": 40, "PEP-M": 22, "MAR-M": 28, "HAW-L": 20, "COKE": 45}       # units a day, all channels
CHANNEL_SHARE = {"QR_DINEIN": 0.40, "PHONE": 0.12, "GRAB": 0.28, "LINEMAN": 0.20}
WEEKDAY_MULT = [0.80, 0.80, 0.90, 1.00, 1.35, 1.50, 1.20]                          # Mon .. Sun
HISTORY_START = date(2026, 8, 1)
SHOCK_DAY = date(2026, 10, 6)       # a new competitor opens next door: walk-in (QR dine-in) sales drop 40%
SHOCK_CHANNEL, SHOCK_MULT = "QR_DINEIN", 0.60


def calendar(start: date, days: int, seed: int = 11) -> pd.DataFrame:
    """Things we know BEFORE the day happens: weekday, payday, and the rain forecast."""
    rng = np.random.default_rng(seed)
    d = pd.date_range(start, periods=days, freq="D")
    return pd.DataFrame({"business_date": d.date, "weekday": d.weekday,
                         "payday": [(x.day >= 25 or x.day <= 2) for x in d],
                         "rain": rng.random(days) < 0.30})


def expected_units(day: date, rain: bool) -> dict:
    """Hidden recipe for how many units we expect per (item, channel) on one day."""
    wk = WEEKDAY_MULT[day.weekday()]
    pay = 1.15 if (day.day >= 25 or day.day <= 2) else 1.0
    out = {}
    for sku in SKUS:
        for ch in CHANNELS:
            rain_m = (1.25 if ch in ("GRAB", "LINEMAN") else 0.80) if rain else 1.0
            shock = SHOCK_MULT if (day >= SHOCK_DAY and ch == SHOCK_CHANNEL) else 1.0
            out[(sku, ch)] = ITEM_BASE[sku] * CHANNEL_SHARE[ch] * wk * pay * rain_m * shock
    return out


def _bkk(day: date, minutes: float) -> pd.Timestamp:
    return (pd.Timestamp(day, tz=BKK) + pd.Timedelta(hours=10) + pd.Timedelta(minutes=float(minutes))).round("s")


def generate_day(day: date, out_dir: Path, rain: bool, seed: int = 7) -> dict:
    """Write the three source files for one business day. Returns the true units sold."""
    rng = np.random.default_rng(seed * 100_003 + (day - HISTORY_START).days)
    exp = expected_units(day, rain)
    truth = {k: int(rng.poisson(v)) for k, v in exp.items()}

    def minute() -> float:                      # lunch peak, dinner peak, a late tail (up to 02:00)
        u = rng.random()
        m = rng.normal(150, 60) if u < 0.35 else rng.normal(570, 100) if u < 0.85 else rng.uniform(0, 960)
        return float(np.clip(m, 0, 959))

    def make_orders(ch: str) -> list:
        pool = [sku for sku in SKUS for _ in range(truth[(sku, ch)])]
        rng.shuffle(pool)
        orders, i = [], 0
        while i < len(pool):
            n = int(rng.integers(1, 4))
            lines = {}
            for sku in pool[i:i + n]:
                lines[sku] = lines.get(sku, 0) + 1
            orders.append((_bkk(day, minute()), lines))
            i += n
        return orders

    tag = day.strftime("%Y%m%d")
    for sub in ("pos", "grab", "lineman"):
        (out_dir / sub).mkdir(parents=True, exist_ok=True)

    # ---- POS (QR and phone): ISO time with +07:00; every order is exported TWICE (placed, then paid)
    pos = []
    for ch in ("QR_DINEIN", "PHONE"):
        for k, (ts, lines) in enumerate(make_orders(ch)):
            for status in ("PLACED", "PAID"):
                pos.append({"pos_order_id": f"P-{tag}-{ch[:2]}{k:03d}", "ordered_at": ts.isoformat(),
                            "channel": ch, "status": status,
                            "lines": [{"sku": s, "qty": q, "unit_price": PRICE[s]} for s, q in lines.items()]})
    (out_dir / "pos" / f"orders_{tag}.json").write_text(json.dumps(pos))

    # ---- Grab: UTC time with a Z, Grab item names, 8% of orders repeated, plus a few cancelled extras
    grab = []
    for k, (ts, lines) in enumerate(make_orders("GRAB")):
        rec = {"orderID": f"GF-{tag}-{k:03d}", "orderTime": ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
               "orderStatus": "COMPLETED", "commissionPct": 30,
               "items": [{"itemName": GRAB_NAME[s], "quantity": q, "itemPrice": APP_PRICE[s]} for s, q in lines.items()]}
        grab.append(rec)
        if rng.random() < 0.08:
            grab.append(dict(rec))
    for k in range(int(rng.integers(0, 3))):
        grab.append({"orderID": f"GF-{tag}-X{k}", "orderTime": _bkk(day, minute()).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "orderStatus": "CANCELLED", "commissionPct": 30,
                     "items": [{"itemName": GRAB_NAME["HAW-L"], "quantity": 1, "itemPrice": APP_PRICE["HAW-L"]}]})
    (out_dir / "grab" / f"orders_{tag}.json").write_text(json.dumps(grab))

    # ---- LINE MAN: local time as dd/mm/yyyy HH:MM, different field names, wrapped in an object
    lm = []
    for k, (ts, lines) in enumerate(make_orders("LINEMAN")):
        lm.append({"order_no": f"LM-{tag}-{k:03d}", "created_at": ts.strftime("%d/%m/%Y %H:%M"),
                   "menu": [{"name": LM_NAME[s], "qty": q, "subtotal": APP_PRICE[s] * q} for s, q in lines.items()]})
    (out_dir / "lineman" / f"export_{tag}.json").write_text(json.dumps({"orders": lm}))
    return truth


# =====================================================================================
# 3. LAKE FOLDERS
# =====================================================================================
ZONES = ["landing", "raw", "trusted", "refined"]


def new_lake(root: Path) -> Path:
    """Make an empty lake. Every zone is a folder."""
    root = Path(root)
    if root.exists():
        shutil.rmtree(root)
    for z in ZONES:
        (root / z).mkdir(parents=True)
    return root


# =====================================================================================
# 4. THE PIPELINE:  landing -> raw -> trusted -> refined
# =====================================================================================
class QualityError(Exception):
    """A quality gate failed. The job stops and nothing is published. Retrying would give the same answer."""


class LateFileError(Exception):
    """A source has not delivered its file yet. This one often fixes itself, so it is worth a retry."""


def land(lake: Path, src_dir: Path, day: date) -> None:
    """LANDING: copy the files exactly as they are. Files arrive the morning after (T+1)."""
    arrival = (day + timedelta(days=1)).isoformat()
    for source in ("pos", "grab", "lineman"):
        for f in sorted((src_dir / source).glob(f"*{day.strftime('%Y%m%d')}*.json")):
            dest = lake / "landing" / source / arrival / f.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, dest)
            if dest.stat().st_size == 0:
                raise QualityError(f"empty file landed: {dest}")


def ingest_raw(lake: Path, ingest_date: date) -> int:
    """RAW: one row per order record, whole record kept as text. Only 3 columns are added."""
    n = 0
    for f in sorted((lake / "landing").rglob(f"{ingest_date.isoformat()}/*.json")):
        source = f.parent.parent.name
        doc = json.loads(f.read_text())
        recs = doc["orders"] if isinstance(doc, dict) else doc      # LINE MAN wraps its list
        df = pd.DataFrame({"payload": [json.dumps(r) for r in recs]})
        df.insert(0, "source_system", source.upper())
        df.insert(1, "ingest_ts", f"{ingest_date.isoformat()}T06:00:00+07:00")
        df.insert(2, "source_file", f.name)
        out = lake / "raw" / source / f"ingest_date={ingest_date.isoformat()}" / (f.stem + ".csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        n += len(df)
    return n


COMMON = ["source_system", "external_order_id", "channel_code", "order_ts", "order_status",
          "platform_item_name", "menu_item_nk", "quantity", "unit_price_thb", "ingest_ts", "source_file"]


def adapt_pos(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for m, r in zip(raw.itertuples(), raw.payload.map(json.loads)):
        for ln in r["lines"]:
            rows.append(dict(source_system="POS", external_order_id=r["pos_order_id"], channel_code=r["channel"],
                             order_ts=pd.Timestamp(r["ordered_at"]).tz_convert(BKK), order_status=r["status"],
                             platform_item_name=ln["sku"], menu_item_nk=ln["sku"], quantity=int(ln["qty"]),
                             unit_price_thb=float(ln["unit_price"]), ingest_ts=m.ingest_ts, source_file=m.source_file))
    return pd.DataFrame(rows, columns=COMMON)


def adapt_grab(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for m, r in zip(raw.itertuples(), raw.payload.map(json.loads)):
        for it in r["items"]:
            rows.append(dict(source_system="GRAB", external_order_id=r["orderID"], channel_code="GRAB",
                             order_ts=pd.Timestamp(r["orderTime"]).tz_convert(BKK),      # Z means UTC
                             order_status=r["orderStatus"].upper(), platform_item_name=it["itemName"], menu_item_nk=None,
                             quantity=int(it["quantity"]), unit_price_thb=float(it["itemPrice"]),
                             ingest_ts=m.ingest_ts, source_file=m.source_file))
    return pd.DataFrame(rows, columns=COMMON)


def adapt_lineman(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for m, r in zip(raw.itertuples(), raw.payload.map(json.loads)):
        for it in r["menu"]:
            rows.append(dict(source_system="LINEMAN", external_order_id=r["order_no"], channel_code="LINEMAN",
                             order_ts=pd.Timestamp(datetime.strptime(r["created_at"], "%d/%m/%Y %H:%M"), tz=BKK),
                             order_status="COMPLETED", platform_item_name=it["name"], menu_item_nk=None,
                             quantity=int(it["qty"]), unit_price_thb=float(it["subtotal"]) / int(it["qty"]),
                             ingest_ts=m.ingest_ts, source_file=m.source_file))
    return pd.DataFrame(rows, columns=COMMON)


ADAPTERS = {"pos": adapt_pos, "grab": adapt_grab, "lineman": adapt_lineman}
STATUS_RANK = {"PLACED": 1, "PAID": 2, "COMPLETED": 2, "CANCELLED": 3}


def quality_gates(lines: pd.DataFrame) -> list:
    """Each gate is (name, passed, detail). The same list is used by the job AND by the tests."""
    bad = lines[lines.menu_item_nk.isna()]
    unmapped = bad.platform_item_name.unique().tolist() if "platform_item_name" in bad else len(bad)   # trusted tables do not keep the app's name
    return [
        ("every item is mapped to a menu key", not unmapped, f"unmapped: {unmapped}"),
        ("every line has a channel", bool(lines.channel_code.notna().all()), ""),
        ("quantity and price are positive", bool(((lines.quantity > 0) & (lines.unit_price_thb > 0)).all()), ""),
        ("no duplicate lines left", not lines.duplicated(["source_system", "external_order_id", "menu_item_nk"]).any(), ""),
    ]


def process_day(lake: Path, day: date) -> dict:
    """TRUSTED: clean one business day. Parse, one timezone, business day, conform names, dedupe, gates."""
    parts = []
    for source, adapter in ADAPTERS.items():
        # a business day's files arrive the next morning, so look at ingest dates day .. day+2 only
        files = sorted(f for k in range(3) for f in
                       (lake / "raw" / source / f"ingest_date={(day + timedelta(days=k)).isoformat()}").glob("*.csv"))
        if files:
            parts.append(adapter(pd.concat(pd.read_csv(f) for f in files)))
    lines = pd.concat(parts, ignore_index=True)

    lines["business_date"] = business_date(lines.order_ts)
    lines = lines[lines.business_date == day].copy()
    n_in = len(lines)

    # conform item names: one map, one place
    keys = list(zip(lines.channel_code, lines.platform_item_name))
    lines["menu_item_nk"] = lines.menu_item_nk.fillna(pd.Series([NAME_MAP.get(k) for k in keys], index=lines.index))

    # dedupe: keep the latest snapshot of each order, then drop exact repeated lines
    lines["snap"] = lines.ingest_ts + "|" + lines.order_status.map(STATUS_RANK).fillna(0).astype(int).astype(str)
    best = lines.groupby(["source_system", "external_order_id"]).snap.transform("max")
    lines = lines[lines.snap == best].drop(columns="snap")
    lines = lines.drop_duplicates(["source_system", "external_order_id", "platform_item_name", "quantity", "unit_price_thb"])
    n_removed = n_in - len(lines)

    for name, ok, detail in quality_gates(lines):
        if not ok:
            raise QualityError(f"{day}: gate failed: {name}. {detail}")

    lines["line_net_thb"] = lines.quantity * lines.unit_price_thb
    lines["is_cancelled"] = lines.order_status.eq("CANCELLED")
    lines["order_ts"] = lines.order_ts.map(lambda t: t.isoformat())
    cols = ["business_date", "channel_code", "menu_item_nk", "source_system", "external_order_id",
            "is_cancelled", "quantity", "unit_price_thb", "line_net_thb", "order_ts"]
    out = lake / "trusted" / "fct_order_line" / f"business_date={day.isoformat()}" / "part.csv"
    out.parent.mkdir(parents=True, exist_ok=True)       # one folder per day: re-running a day replaces it
    lines[cols].to_csv(out, index=False)
    return {"business_date": day.isoformat(), "lines_in": n_in, "duplicates_removed": n_removed,
            "lines_out": len(lines), "orders": int(lines.groupby(["source_system", "external_order_id"]).ngroups)}


def read_trusted(lake: Path) -> pd.DataFrame:
    files = sorted((lake / "trusted" / "fct_order_line").rglob("part.csv"))
    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    df["business_date"] = pd.to_datetime(df.business_date).dt.date
    return df


def build_marts(lake: Path) -> pd.DataFrame:
    """REFINED: mart_sales_daily = business date x channel x item. Zero days are real data too."""
    t = read_trusted(lake)
    t = t[~t.is_cancelled]
    mart = (t.groupby(["business_date", "channel_code", "menu_item_nk"], as_index=False)
              .agg(units_sold=("quantity", "sum"), net_sales_thb=("line_net_thb", "sum"),
                   orders=("external_order_id", "nunique")))
    days = sorted(mart.business_date.unique())
    grid = pd.MultiIndex.from_product([days, CHANNELS, SKUS], names=["business_date", "channel_code", "menu_item_nk"])
    mart = mart.set_index(["business_date", "channel_code", "menu_item_nk"]).reindex(grid, fill_value=0).reset_index()
    mart.to_csv(lake / "refined" / "mart_sales_daily.csv", index=False)
    return mart


def run_nightly(lake: Path, src_dir: Path, day: date) -> dict:
    """The nightly job for one business day. Land, ingest, process, build marts."""
    land(lake, src_dir, day)
    missing = check_sources_landed(lake, day)
    if missing:                                    # without this, a late Grab file silently gives a LOW sales number
        raise LateFileError(f"{day}: no file yet from {', '.join(missing)}")
    ingest_raw(lake, day + timedelta(days=1))
    stats = process_day(lake, day)
    build_marts(lake)
    return stats


def check_sources_landed(lake: Path, day: date) -> list:
    """Freshness check: which of the three sources has no file for this business day yet?"""
    arrival = (day + timedelta(days=1)).isoformat()
    return [s for s in ("pos", "grab", "lineman") if not list((lake / "landing" / s / arrival).glob("*.json"))]


# ---- what happens when the job fails: retry or not, and who hears about it ----------------------
def notify(alerts: list, level: str, message: str) -> None:
    """Level 'log' = written down only. 'chat' = team chat, no sound. 'page' = a phone rings.
    Here we only collect the messages. In real life this line would call LINE, Slack or email."""
    alerts.append({"level": level, "message": message})


def run_with_retry(job, waits_min=(0, 45, 30), sleep=time.sleep, alerts: list | None = None, name: str = "nightly pipeline") -> dict:
    """Run a job. Retry only what can fix itself. Tell a human only when a retry will not help.

    waits_min=(0, 45, 30) means: try at 06:30, again 45 minutes later (07:15), and a last time 30 minutes
    after that (07:45). The owner needs the data at 08:00, so there is no point in a later try."""
    alerts = [] if alerts is None else alerts
    last = None
    for attempt, wait in enumerate(waits_min, start=1):
        if wait:
            sleep(wait * 60)
        try:
            result = job()
        except (LateFileError, OSError) as e:                  # may fix itself: late file, busy disk
            last = e
            notify(alerts, "log", f"{name}: try {attempt} failed ({e})")
            continue
        except QualityError as e:                              # bad data: same input, same failure. Do not retry
            notify(alerts, "page", f"{name} STOPPED, bad data, not retried: {e}")
            return {"status": "failed", "attempts": attempt, "alerts": alerts}
        except Exception as e:                                 # our own bug: do not hide it behind retries
            notify(alerts, "page", f"{name} CRASHED, probably our code: {e!r}")
            return {"status": "failed", "attempts": attempt, "alerts": alerts}
        if attempt > 1:
            notify(alerts, "chat", f"{name} worked on try {attempt}. Nothing to do.")
        return {"status": "ok", "attempts": attempt, "result": result, "alerts": alerts}
    notify(alerts, "page", f"{name} still failing after {len(waits_min)} tries: {last}")
    return {"status": "failed", "attempts": len(waits_min), "alerts": alerts}


# ---- ordering stock: how far ahead must we forecast? -------------------------------------------
def order_cycle(order_day: date, order_weekdays=(0, 3), lead_days: int = 1) -> dict:
    """An order placed on order_day arrives lead_days later and must last until the NEXT order arrives."""
    nxt = order_day + timedelta(days=1)
    while nxt.weekday() not in order_weekdays:
        nxt += timedelta(days=1)
    first = order_day + timedelta(days=lead_days)
    last = nxt + timedelta(days=lead_days) - timedelta(days=1)
    return {"order_day": order_day, "first_day": first, "last_day": last,
            "days_covered": (last - first).days + 1, "horizon_days": (last - order_day).days}


def read_mart(lake: Path) -> pd.DataFrame:
    df = pd.read_csv(lake / "refined" / "mart_sales_daily.csv")
    df["business_date"] = pd.to_datetime(df.business_date).dt.date
    return df


def build_history(lake: Path, src_dir: Path, days: int, seed: int = 7) -> dict:
    """Run the pipeline night after night. Returns the planted truth so tests can compare."""
    cal = calendar(HISTORY_START, days)
    truth = {}
    for r in cal.itertuples():
        t = generate_day(r.business_date, src_dir, bool(r.rain), seed)
        truth[r.business_date] = t
        run_nightly(lake, src_dir, r.business_date)
    return truth


# =====================================================================================
# 5. FORECAST MODEL:  how many units of each item, per channel, tomorrow?
# =====================================================================================
def with_calendar(df: pd.DataFrame, cal: pd.DataFrame) -> pd.DataFrame:
    return df.merge(cal, on="business_date", how="left")


def design(df: pd.DataFrame) -> tuple:
    """Turn rows into numbers: one column per series, per weekday, plus payday and rain."""
    series = sorted(f"{i}|{c}" for i in SKUS for c in CHANNELS)
    s = df.menu_item_nk + "|" + df.channel_code
    cols = {f"series:{n}": (s == n).astype(float) for n in series}
    for w in range(1, 7):                                        # Monday is the reference weekday
        cols[f"weekday:{w}"] = (df.weekday == w).astype(float)
    cols["payday"] = df.payday.astype(float)
    cols["rain_x_delivery"] = (df.rain & df.channel_code.isin(["GRAB", "LINEMAN"])).astype(float)
    cols["rain_x_store"] = (df.rain & ~df.channel_code.isin(["GRAB", "LINEMAN"])).astype(float)
    X = pd.DataFrame(cols, index=df.index)
    return X.to_numpy(), list(X.columns)


class RidgeForecaster:
    """A small linear model on log(1 + units). Easy to read: every weight means something."""

    def __init__(self, alpha: float = 1.0, half_life_days: float | None = None):
        self.alpha, self.half_life_days = alpha, half_life_days
        self.coef_, self.names_ = None, None

    def fit(self, df: pd.DataFrame) -> "RidgeForecaster":
        X, self.names_ = design(df)
        y = np.log1p(df.units_sold.to_numpy(dtype=float))
        w = np.ones(len(df))
        if self.half_life_days:                                   # newer days count more
            age = (max(df.business_date) - df.business_date).map(lambda d: d.days).to_numpy()
            w = 0.5 ** (age / self.half_life_days)
        Xw = X * w[:, None]
        self.coef_ = np.linalg.solve(X.T @ Xw + self.alpha * np.eye(X.shape[1]), Xw.T @ y)
        return self

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        X, _ = design(df)
        return np.clip(np.expm1(X @ self.coef_), 0, None)

    def to_params(self) -> dict:
        return {"alpha": self.alpha, "half_life_days": self.half_life_days,
                "names": self.names_, "coef": [float(c) for c in self.coef_]}

    @classmethod
    def from_params(cls, p: dict) -> "RidgeForecaster":
        m = cls(p["alpha"], p["half_life_days"])
        m.names_, m.coef_ = p["names"], np.array(p["coef"])
        return m


def baseline_predict(history: pd.DataFrame, target: pd.DataFrame) -> np.ndarray:
    """The dumb rival every model must beat: same weekday, last week."""
    last = history.set_index(["business_date", "channel_code", "menu_item_nk"]).units_sold
    keys = [(d - timedelta(days=7), c, i) for d, c, i in zip(target.business_date, target.channel_code, target.menu_item_nk)]
    return np.array([float(last.get(k, np.nan)) for k in keys])


def wape(actual, pred) -> float:
    """Weighted absolute percentage error: total miss divided by total units. Lower is better."""
    actual, pred = np.asarray(actual, float), np.asarray(pred, float)
    return float(np.abs(actual - pred).sum() / max(actual.sum(), 1.0))


def mae(actual, pred) -> float:
    return float(np.abs(np.asarray(actual, float) - np.asarray(pred, float)).mean())


def split_by_date(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    return df[(df.business_date >= start) & (df.business_date <= end)].copy()


# =====================================================================================
# 6. MODEL REGISTRY:  a tiny, readable MLflow. A JSON file with versions and stages.
# =====================================================================================
class ModelRegistry:
    """Every trained model becomes a version. Only ONE version is 'production' at a time."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {"versions": []}

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=1))

    def register(self, model: RidgeForecaster, metrics: dict, trained_on: tuple, note: str = "") -> int:
        v = len(self.data["versions"]) + 1
        self.data["versions"].append({"version": v, "stage": "candidate", "metrics": metrics, "note": note,
                                      "trained_on": [str(trained_on[0]), str(trained_on[1])], "params": model.to_params()})
        self._save()
        return v

    def production(self) -> dict | None:
        return next((x for x in self.data["versions"] if x["stage"] == "production"), None)

    def promote(self, version: int) -> None:
        for x in self.data["versions"]:
            if x["stage"] == "production":
                x["stage"] = "archived"
            if x["version"] == version:
                x["stage"] = "production"
        self._save()

    def rollback(self) -> int:
        """Put the most recent archived version back as production."""
        old = [x for x in self.data["versions"] if x["stage"] == "archived"]
        if not old:
            raise RuntimeError("nothing to roll back to")
        cur = self.production()
        if cur:
            cur["stage"] = "archived"
        old[-1]["stage"] = "production"
        self._save()
        return old[-1]["version"]

    def load(self, version: int | None = None) -> RidgeForecaster:
        row = self.production() if version is None else self.data["versions"][version - 1]
        return RidgeForecaster.from_params(row["params"])

    def table(self) -> pd.DataFrame:
        return pd.DataFrame([{"version": x["version"], "stage": x["stage"], "trained_on": " to ".join(x["trained_on"]),
                              **x["metrics"], "note": x["note"]} for x in self.data["versions"]])


def promotion_decision(candidate: dict, production: dict | None, baseline_wape: float, min_gain: float = 0.02) -> tuple:
    """The rule, written down so a machine can follow it. Returns (promote?, why)."""
    if not np.isfinite(candidate["wape"]):
        return False, "candidate metric is not a number"
    if candidate["wape"] >= baseline_wape:
        return False, f"does not beat the baseline ({candidate['wape']:.3f} vs {baseline_wape:.3f})"
    if production is None:
        return True, "first model that beats the baseline"
    if candidate["wape"] <= production["wape"] * (1 - min_gain):
        return True, f"beats production by at least {min_gain:.0%} ({candidate['wape']:.3f} vs {production['wape']:.3f})"
    return False, f"not better enough than production ({candidate['wape']:.3f} vs {production['wape']:.3f})"


# =====================================================================================
# 7. FORECASTS, MONITORING, DRIFT
# =====================================================================================
def write_forecast(lake: Path, df: pd.DataFrame, preds: np.ndarray, model_version: int) -> pd.DataFrame:
    """fct_forecast: grain = item x day x channel x model version. Predictions are saved, never overwritten."""
    out = df[["business_date", "channel_code", "menu_item_nk"]].copy()
    out["forecast_units"] = np.round(preds, 2)
    out["model_version"] = model_version
    path = lake / "refined" / "fct_forecast.csv"
    if path.exists():
        old = pd.read_csv(path)
        out = pd.concat([old, out.assign(business_date=out.business_date.astype(str))], ignore_index=True)
    else:
        out["business_date"] = out.business_date.astype(str)
    out.to_csv(path, index=False)
    return out


def forecast_vs_actual(lake: Path) -> pd.DataFrame:
    f = pd.read_csv(lake / "refined" / "fct_forecast.csv")
    f["business_date"] = pd.to_datetime(f.business_date).dt.date
    m = read_mart(lake)[["business_date", "channel_code", "menu_item_nk", "units_sold"]]
    return f.merge(m, on=["business_date", "channel_code", "menu_item_nk"], how="inner")


def weekly_error(fva: pd.DataFrame) -> pd.DataFrame:
    d = fva.copy()
    d["week"] = d.business_date.map(lambda x: (x - HISTORY_START).days // 7)
    g = d.groupby("week").apply(lambda x: pd.Series({"from": min(x.business_date), "wape": wape(x.units_sold, x.forecast_units)}),
                                include_groups=False)
    return g.reset_index()


def bias_report(fva: pd.DataFrame, by: str, last_days: int = 14) -> pd.DataFrame:
    """Where is the model wrong in ONE direction? bias = (forecast - actual) / actual over the last days.
    Near 0 is fine. A big positive number means: we keep forecasting more than we sell."""
    last = max(fva.business_date)
    d = fva[fva.business_date > last - timedelta(days=last_days)]
    g = d.groupby(by).apply(lambda x: pd.Series({"actual": x.units_sold.sum(), "forecast": x.forecast_units.sum()}),
                            include_groups=False)
    g["bias"] = ((g.forecast - g.actual) / g.actual).round(3)
    g["verdict"] = np.where(g.bias.abs() > 0.20, "DRIFT", np.where(g.bias.abs() > 0.10, "watch", "ok"))
    return g.reset_index()


# =====================================================================================
# 8. CI GATE:  what a pull request must pass before it can merge
# =====================================================================================
def ci_checks(workdir: Path, days: int = 21) -> list:
    """Build a small history from scratch and check the numbers. Returns (name, passed, detail)."""
    workdir = Path(workdir)
    lake, src = new_lake(workdir / "lake"), workdir / "sources"
    if src.exists():
        shutil.rmtree(src)
    truth = build_history(lake, src, days)
    mart = read_mart(lake)
    got = mart.groupby("business_date").units_sold.sum()
    want = pd.Series({d: sum(v.values()) for d, v in truth.items()})
    results = [("mart units equal the units we planted, every day", bool((got == want.reindex(got.index)).all()),
                f"{int((got != want.reindex(got.index)).sum())} days differ")]
    first = process_day(lake, min(truth))
    again = process_day(lake, min(truth))
    results.append(("re-running a day gives the same result", first == again, ""))
    results.append(("no negative or missing numbers in the mart", bool((mart.units_sold >= 0).all() and mart.notna().all().all()), ""))
    return results
