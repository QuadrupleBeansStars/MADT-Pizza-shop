# Pizza Shop Data Platform — Pipeline & ER Model

Class project. Data platform design for a single-location, owner-operated pizza shop in Bangkok
selling across QR dine-in, phone, and four delivery aggregators.

This repository is the full write-up: how data gets from five source systems into a warehouse, the
ER model at each layer, and the decisions worth defending in a review.

## Files

| File | What it is |
|---|---|
| `README.md` | This document — the full write-up, with all four diagrams inline |
| `docs/data-platform.html` | Styled web version of the same write-up |
| `diagrams/01-pipeline-flow.mmd` | Source → raw → staging → core → mart → consumer flow |
| `diagrams/02-er-sales-orders.mmd` | ER: orders, lines, payments, promotions, channels, customers |
| `diagrams/03-er-inventory-supply.mmd` | ER: ingredients, recipes, stock movements, suppliers, POs |
| `diagrams/04-er-people-shifts.mmd` | ER: staff, roles, shifts |

**Reading it.** The diagrams below render directly on GitHub, and in VS Code (Markdown Preview),
Obsidian, Typora and Notion. For the styled version, clone the repo and open
`docs/data-platform.html` in a browser — GitHub shows `.html` as raw source, so that file only
works locally, and it needs an internet connection on first load because it pulls Mermaid from a
CDN. The `.mmd` files paste straight into [mermaid.live](https://mermaid.live) if you need PNG/SVG
exports for slides.

---

## 1 · Scope and assumptions

One store, so there is no `dim_store` — location is a constant, not a dimension. Everything is
modelled at the grain a single manager can act on: a day, a menu item, a channel, an ingredient,
a shift.

- **Forecast grain is menu item × business day × channel.** The load-bearing assumption: it makes
  the sales mart a daily item-level table rather than revenue-only, which is what makes ingredient
  forecasting possible downstream.
- **Business day ≠ calendar day.** The shop trades past midnight, so a business day runs
  10:00–02:00 Asia/Bangkok. Every fact carries both `order_ts` and `business_date_sk`.
- **Aggregator orders are anonymous.** Grab, LINE MAN, ShopeeFood and Robinhood do not hand over
  customer identity. Membership and segmentation are *own-channel only*.
- **Stock is not "order ±10%".** Consumption is *derived* from a recipe, then *reconciled* against
  physical counts. The 10% is the variance you measure, not a rule you hard-code.

## 2 · Source systems

| Source | What it gives | Format / access | Cadence | Natural key |
|---|---|---|---|---|
| POS / QR ordering | Dine-in and walk-in orders, lines, payments, kitchen status | App DB or POS API | Near real-time (5 min micro-batch) | `pos_order_id` |
| Aggregators ×4 | Platform orders, lines, promo, commission, courier timestamps | Merchant-portal CSV export or partner API | Daily (T+1, some settle T+7) | `(platform, platform_order_id)` |
| Inventory counts | Physical stock on hand by ingredient | Tablet form / Google Sheet | Daily close + weekly full count | `(count_session_id, ingredient_id)` |
| Purchasing | Supplier POs, goods receipts, unit cost | Sheet or supplier invoice entry | On delivery (2–3× / week) | `po_line_id` |
| Staff & shifts | Roster, clock-in/out, role, wage rate | Scheduling app / sheet | Daily | `(staff_id, shift_start)` |

> **Why this matters:** POS is streaming-ish and trustworthy; aggregator data is late, batched,
> schema-drifting, and arrives once per platform with four different column names for "order total".
> Nothing downstream of staging should care which of the five it came from — that is the entire job
> of the conform layer.

## 3 · Pipeline architecture

```mermaid
flowchart LR
  subgraph SRC["SOURCES"]
    direction TB
    S1["POS / QR<br/>orders API"]
    S2["Grab · LINE MAN<br/>ShopeeFood · Robinhood<br/>portal exports"]
    S3["Stock count<br/>form"]
    S4["Supplier PO<br/>sheet"]
    S5["Shift &amp;<br/>timeclock"]
  end

  subgraph RAW["1 · RAW / BRONZE"]
    direction TB
    R1["Immutable append<br/>schema-on-read<br/>+ ingest_ts, source_file"]
  end

  subgraph STG["2 · STAGING / SILVER"]
    direction TB
    T1["Parse · cast · UTC→Asia/Bangkok<br/>Dedup on natural key<br/>Conform channel + item names<br/>Currency to THB"]
    T2["Data quality gates<br/>not-null · uniqueness<br/>referential · row-count drift"]
  end

  subgraph CORE["3 · CORE WAREHOUSE / GOLD"]
    direction TB
    C1["Conformed dimensions<br/>SCD2 where price/role changes"]
    C2["Fact tables<br/>declared grain, surrogate keys"]
  end

  subgraph MART["4 · MARTS"]
    direction TB
    M1["mart_sales_daily"]
    M2["mart_ingredient_usage_daily"]
    M3["mart_delivery_sla"]
    M4["mart_customer_rfm"]
    M5["mart_labor_vs_sales"]
  end

  subgraph OUT["CONSUMERS"]
    direction TB
    O1["Manager dashboard"]
    O2["Demand forecast model"]
    O3["Reorder suggestion"]
    O4["Customer segmentation"]
  end

  S1 -->|"5 min micro-batch"| RAW
  S2 -->|"daily T+1"| RAW
  S3 -->|"daily close"| RAW
  S4 -->|"on receipt"| RAW
  S5 -->|"daily"| RAW

  RAW --> T1 --> T2 --> CORE
  C1 --> C2
  CORE --> MART
  M1 --> O1
  M1 --> O2
  M2 --> O3
  M2 --> O1
  M3 --> O1
  M4 --> O4
  M5 --> O1
  O2 -.->|"forecast written back<br/>as fct_forecast"| CORE
```

### Layer contracts

| Layer | Guarantee | Deliberately not done here |
|---|---|---|
| 1 · Raw | Nothing is ever lost or edited. Every row keeps `source_system`, `ingest_ts`, `source_file`, raw payload. | No typing, no dedup, no joins. |
| 2 · Staging | One row per real-world event. Types cast, timestamps in Asia/Bangkok, channel and item names conformed, duplicates removed. | No business aggregation, no KPI logic. |
| 3 · Core | Star schema. Every fact has a declared grain and only surrogate keys. History preserved via SCD2. | No presentation shaping, no channel-specific hacks. |
| 4 · Mart | One table per question, pre-aggregated, dashboard- and model-ready. | No re-derivation of business rules — those live in core. |

## 4 · ER model — sales and orders

Grain declarations:

- `FCT_ORDER` — one row per order (any channel)
- `FCT_ORDER_LINE` — one row per menu item per order
- `FCT_ORDER_EVENT` — one row per status transition per order
- `FCT_PAYMENT` — one row per tender per order (split payments are normal)

```mermaid
erDiagram
  DIM_DATE ||--o{ FCT_ORDER : "business day"
  DIM_CHANNEL ||--o{ FCT_ORDER : "sold via"
  DIM_CUSTOMER ||--o{ FCT_ORDER : "placed by (own channels only)"
  FCT_ORDER ||--|{ FCT_ORDER_LINE : contains
  FCT_ORDER ||--o{ FCT_ORDER_EVENT : "tracked by"
  FCT_ORDER ||--|{ FCT_PAYMENT : "settled by"
  FCT_ORDER ||--o{ FCT_ORDER_PROMOTION : "discounted by"
  DIM_PROMOTION ||--o{ FCT_ORDER_PROMOTION : applied
  DIM_MENU_ITEM ||--o{ FCT_ORDER_LINE : "item sold"
  DIM_MENU_ITEM ||--o{ BRG_CHANNEL_MENU_ITEM : "listed as"
  DIM_CHANNEL ||--o{ BRG_CHANNEL_MENU_ITEM : "lists"
  DIM_STAFF ||--o{ FCT_ORDER_EVENT : "performed by"

  DIM_DATE {
    int date_sk PK
    date calendar_date
    int day_of_week
    boolean is_weekend
    boolean is_thai_holiday
    string month_name
  }
  DIM_CHANNEL {
    int channel_sk PK
    string channel_code UK "QR_DINEIN, GRAB, LINEMAN, SHOPEE, ROBINHOOD, PHONE"
    string channel_type "dine_in or delivery"
    boolean is_own_channel
    decimal commission_rate_pct
  }
  DIM_CUSTOMER {
    int customer_sk PK
    string customer_nk UK "membership or phone hash"
    date first_order_date
    string signup_source
    boolean is_member
  }
  DIM_MENU_ITEM {
    int menu_item_sk PK
    string menu_item_nk "stable business key"
    string item_name
    string category "pizza, chicken, side, drink"
    string size_code
    decimal base_price_thb
    date valid_from "SCD2"
    date valid_to "SCD2"
    boolean is_current
  }
  BRG_CHANNEL_MENU_ITEM {
    int channel_menu_sk PK
    int channel_sk FK
    int menu_item_sk FK
    string platform_item_id "aggregator's own id"
    decimal channel_price_thb "uplifted platform price"
    boolean is_listed
  }
  DIM_PROMOTION {
    int promotion_sk PK
    string promo_code
    string promo_type "percent, fixed, bogo, free_delivery"
    string funded_by "shop or platform"
    date start_date
    date end_date
  }
  FCT_ORDER {
    bigint order_sk PK
    string external_order_id "platform natural key"
    int channel_sk FK
    int date_sk FK
    int customer_sk FK "null for aggregator orders"
    timestamp order_ts
    decimal gross_amount_thb
    decimal discount_amount_thb
    decimal delivery_fee_thb
    decimal commission_amount_thb
    decimal net_revenue_thb
    string order_status
    boolean is_cancelled
  }
  FCT_ORDER_LINE {
    bigint order_line_sk PK
    bigint order_sk FK
    int menu_item_sk FK
    int quantity
    decimal unit_price_thb "price at time of order"
    decimal line_discount_thb
    decimal line_net_thb
    string modifiers_json "extra cheese, no chili"
  }
  FCT_ORDER_EVENT {
    bigint order_event_sk PK
    bigint order_sk FK
    string event_type "placed, accepted, prep_start, prep_complete, courier_pickup, delivered, cancelled"
    timestamp event_ts
    int staff_sk FK
    int duration_from_prev_sec
  }
  FCT_PAYMENT {
    bigint payment_sk PK
    bigint order_sk FK
    string tender_type "cash, promptpay_qr, card, platform_settled"
    decimal amount_thb
    timestamp paid_ts
    date settlement_date "platform payouts lag"
  }
  FCT_ORDER_PROMOTION {
    bigint order_promo_sk PK
    bigint order_sk FK
    int promotion_sk FK
    decimal discount_amount_thb
  }
```

`BRG_CHANNEL_MENU_ITEM` is what lets a Grab listing and a QR listing of the same pizza roll up to
one item.

## 5 · ER model — inventory and supply

```mermaid
erDiagram
  DIM_MENU_ITEM ||--|{ BRG_RECIPE : "made from"
  DIM_INGREDIENT ||--o{ BRG_RECIPE : "used in"
  DIM_INGREDIENT ||--o{ FCT_INVENTORY_MOVEMENT : "moves"
  DIM_INGREDIENT ||--o{ FCT_INVENTORY_COUNT : "counted"
  DIM_INGREDIENT ||--o{ FCT_PURCHASE_ORDER_LINE : "purchased"
  DIM_SUPPLIER ||--o{ FCT_PURCHASE_ORDER : supplies
  DIM_SUPPLIER ||--o{ DIM_INGREDIENT : "preferred source"
  FCT_PURCHASE_ORDER ||--|{ FCT_PURCHASE_ORDER_LINE : contains
  FCT_PURCHASE_ORDER_LINE ||--o{ FCT_INVENTORY_MOVEMENT : "goods receipt"
  DIM_DATE ||--o{ FCT_INVENTORY_MOVEMENT : "on day"
  DIM_DATE ||--o{ FCT_INVENTORY_COUNT : "on day"

  DIM_INGREDIENT {
    int ingredient_sk PK
    string ingredient_nk UK
    string ingredient_name "mozzarella, 00 flour, pepperoni"
    string category "dairy, dry_good, protein, produce, packaging"
    string base_uom "gram, ml, piece"
    decimal shelf_life_days
    decimal reorder_point_qty
    decimal safety_stock_qty
    int default_supplier_sk FK
  }
  BRG_RECIPE {
    int recipe_sk PK
    int menu_item_sk FK
    int ingredient_sk FK
    decimal qty_per_unit "in ingredient base_uom"
    decimal yield_loss_pct "trim and prep loss"
    date valid_from "SCD2 - recipes change"
    date valid_to
  }
  DIM_SUPPLIER {
    int supplier_sk PK
    string supplier_name
    string contact_phone
    int lead_time_days
    int min_order_value_thb
    string delivery_days "Mon,Thu"
  }
  FCT_PURCHASE_ORDER {
    bigint po_sk PK
    int supplier_sk FK
    int order_date_sk FK
    int received_date_sk FK
    decimal total_cost_thb
    string po_status
  }
  FCT_PURCHASE_ORDER_LINE {
    bigint po_line_sk PK
    bigint po_sk FK
    int ingredient_sk FK
    decimal qty_ordered
    decimal qty_received
    decimal unit_cost_thb "cost at time of purchase"
  }
  FCT_INVENTORY_MOVEMENT {
    bigint movement_sk PK
    int ingredient_sk FK
    int date_sk FK
    timestamp movement_ts
    string movement_type "receipt, theoretical_usage, waste, spoilage, count_adjustment, staff_meal"
    decimal qty_delta "signed, base_uom"
    bigint source_ref_sk "order_line_sk or po_line_sk"
    decimal unit_cost_thb
  }
  FCT_INVENTORY_COUNT {
    bigint count_sk PK
    int ingredient_sk FK
    int date_sk FK
    decimal qty_counted
    decimal qty_expected "system theoretical on-hand"
    decimal variance_qty "counted minus expected"
    decimal variance_value_thb
    int counted_by_staff_sk FK
  }
```

Theoretical usage flows in from `FCT_ORDER_LINE` via `BRG_RECIPE`; physical truth flows in from
`FCT_INVENTORY_COUNT`; the gap between them is the number the manager gets paid to shrink.

## 6 · ER model — people and shifts

```mermaid
erDiagram
  DIM_STAFF ||--o{ FCT_SHIFT : works
  DIM_ROLE ||--o{ DIM_STAFF : "holds"
  DIM_DATE ||--o{ FCT_SHIFT : "on day"
  DIM_STAFF ||--o{ FCT_ORDER_EVENT : "performed by"
  DIM_STAFF ||--o{ FCT_INVENTORY_COUNT : "counted by"

  DIM_STAFF {
    int staff_sk PK
    string staff_nk UK
    string staff_name
    int role_sk FK
    string employment_type "full_time, part_time"
    decimal hourly_wage_thb
    date hire_date
    date valid_from "SCD2 - wage and role change"
    date valid_to
    boolean is_current
  }
  DIM_ROLE {
    int role_sk PK
    string role_name "pizzaiolo, cashier, rider, prep, manager"
    boolean is_kitchen
  }
  FCT_SHIFT {
    bigint shift_sk PK
    int staff_sk FK
    int date_sk FK
    timestamp clock_in_ts
    timestamp clock_out_ts
    decimal hours_worked
    decimal labour_cost_thb
    string shift_type "open, mid, close"
  }
  FCT_ORDER_EVENT {
    bigint order_event_sk PK
    bigint order_sk FK
    int staff_sk FK
    string event_type
    timestamp event_ts
  }
  FCT_INVENTORY_COUNT {
    bigint count_sk PK
    int counted_by_staff_sk FK
    decimal variance_qty
  }
```

Staff data only earns its place if it joins to something. Here it joins twice: to shifts (labour
cost per hour) and to order events (who was on the pass when tickets ran late).

## 7 · Modelling decisions worth defending

**01 · Multi-channel order provenance.** Every order carries `channel_sk` plus the platform's own
`external_order_id`. Dedup and idempotent reload both key on **(channel_sk, external_order_id)**,
so re-running yesterday's Grab export cannot double-count revenue.

**02 · Recipe-driven stock, not a fixed percentage.** `BRG_RECIPE` gives grams-per-pizza, so
*theoretical usage* is derived from order lines; `FCT_INVENTORY_COUNT` supplies *actual on-hand*.
Variance = theoretical − actual, and that variance is the waste/shrinkage KPI. The 10% is an output
of the system, not an input to it.

**03 · Price is stored twice, on purpose.** `FCT_ORDER_LINE.unit_price_thb` is an immutable
snapshot — a receipt reprinted in 2027 must show 2026's price. `DIM_MENU_ITEM` is SCD2 so you can
still ask "what did margin look like before the March price rise". Snapshot answers *what
happened*; SCD2 answers *what was true*.

**04 · Membership only exists on own channels.** `FCT_ORDER.customer_sk` is nullable and mostly
null. RFM segmentation is scoped to QR and phone orders and the dashboard must label it that way.
Drawing a customer FK off every order would make the segmentation story fiction.

**05 · Delivery timing is an event log, not columns.** `FCT_ORDER_EVENT` records each transition,
so any duration is a difference between two rows — prep time, pickup wait, total lead time —
without schema changes. Coverage is honestly uneven: QR gives full kitchen granularity, aggregators
typically give accepted/pickup/delivered only.

**06 · Payment, promotion and commission are separate from the order.** Split tenders (half
PromptPay, half cash) need their own rows. Platform-funded vs shop-funded promotions land on
different lines of the P&L. And `commission_amount_thb` is what turns gross revenue into the only
number that matters — net revenue the shop actually banks, which differs by up to 30% between
channels.

## 8 · Marts, dashboard and ML

| Mart | Grain | Built from | Serves |
|---|---|---|---|
| `mart_sales_daily` | business date × channel × menu item | FCT_ORDER + FCT_ORDER_LINE + dims | Sales dashboard; training set for demand forecast |
| `mart_ingredient_usage_daily` | business date × ingredient | FCT_ORDER_LINE × BRG_RECIPE, vs FCT_INVENTORY_COUNT | Waste %, variance alerts, reorder suggestions |
| `mart_delivery_sla` | order | FCT_ORDER_EVENT pivoted to durations | Prep-time and lead-time distributions by channel and hour |
| `mart_customer_rfm` | customer (own channels) | FCT_ORDER where customer_sk not null | Segmentation, win-back campaigns via LINE |
| `mart_labor_vs_sales` | business date × hour | FCT_SHIFT + FCT_ORDER | Labour cost %, understaffed-hour detection |

### The closed loop

`mart_sales_daily` → forecast units per **item × day × channel** → multiply through `BRG_RECIPE` →
forecast **grams per ingredient** → subtract current on-hand from `FCT_INVENTORY_MOVEMENT`, add
`safety_stock_qty`, respect supplier `lead_time_days` and `delivery_days` → **a purchase order the
manager can approve with one tap.**

Forecasts are written back as `fct_forecast` (grain: item × day × channel × forecast run) so
predicted-vs-actual error is itself a table you can chart. A model you cannot score is a model you
cannot trust.

### Dashboard, top screen

- **Net revenue by channel** — gross minus commission minus shop-funded promos. Almost never what the platform app tells you.
- **Today vs forecast**, live, with the gap called out.
- **Items at risk** — ingredients where projected demand exceeds on-hand before the next delivery day.
- **Variance watch** — ingredients whose theoretical-vs-counted gap breaches its threshold.
- **Prep time p50 / p90 by hour** — where the kitchen breaks, not just that it did.

## 9 · Data quality rules

| Risk | Where it bites | Rule |
|---|---|---|
| Double ingestion | Aggregator export re-downloaded | Merge key `(channel_sk, external_order_id)`; upsert, never append |
| Late-arriving orders | 01:40 order in yesterday's file | Assign `business_date_sk` by 10:00–02:00 window, reprocess a 3-day trailing partition |
| Schema drift | Platform renames a CSV column | Raw layer stores payload as-is; staging fails loudly on unmapped columns rather than silently nulling |
| Unmapped menu item | New platform-only bundle | `BRG_CHANNEL_MENU_ITEM` miss routes to a quarantine table + alert; revenue is never dropped |
| Cancelled / refunded orders | Inflated sales and phantom stock usage | `is_cancelled` excluded from usage derivation; kept in the fact for cancel-rate analysis |
| Recipe changed mid-period | Variance suddenly explodes | `BRG_RECIPE` is SCD2; usage joins on the recipe valid at order time |
