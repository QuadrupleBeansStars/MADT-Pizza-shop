# MADT Pizza — dine-in ordering app (UX/UI prototype)

A clickable mobile prototype of the QR dine-in ordering flow: the source side of the
`QR dine-in` channel in the data platform. One self-contained file, `index.html`, with all
images embedded (about 2.7 MB). Open it in any browser; it fits phones, tablets and desktops.

**Flow:** menu (Pizza, spaghetti, snacks, drinks) → pizza size and toppings → cart → order page
with per-item notes and promo code → confirm order → receipt with order number, service charge 10%
and VAT 7% → kitchen status and order history.

**What it is not:** a front end only. Orders are not sent anywhere, the kitchen timer runs at demo
speed (1 minute = 1.2 s), menu prices are placeholders, and state lives in the browser's
`localStorage`. Thai by default, with an English switch.

## Order feed (added for the data pipeline and the slides)

Every confirmed order is written as two POS-shaped records (`PLACED`, then `PAID`) with item codes,
prices, toppings, discount, service charge and VAT. They are saved in `localStorage['madt-qr-outbox']`,
broadcast on `BroadcastChannel('madt-pizza-orders')`, and available from the console with
`madtExportOrders()`. The menu codes and prices are in `pipeline/qr_menu_catalog.json`;
`pipeline/pizza_pipeline.py` has `validate_qr_order()` to check a record against that menu.
`slides/index.html` listens on the same channel when both pages share an origin.
