CC's LARA / MLCC Master Product Database Backend

Purpose
-------
This service keeps a local master spirits database for the phone app:

GTIN/UPC <-> MLCC Liquor Code <-> Brand/Size/Pack/Price

Current seed
------------
The uploaded "8-2-26 NEW ITEM PRICE LIST EXCEL.xlsx" is preloaded as a seed.
That gives the service 272 current new-item product rows immediately, even
before the full searchable-pricebook sync is run.

MLCC background sync
--------------------
The service opens the public MLCC Search Pricebook in a headless browser.

1. It first tries "Export All to Excel".
2. If that file includes GTIN/UPC, it imports the whole export.
3. If not, it falls back to the Liquor Type dropdown and searches each category.
4. It parses Liquor Code, GTIN/UPC and product details from result blocks.
5. The database is merged by Liquor Code.
6. A nightly refresh runs automatically.
7. POST /api/sync/mlcc triggers an immediate refresh.

Phone-app endpoints
-------------------
GET  /health
GET  /api/products/lookup?gtin=...
GET  /api/products/lookup?liquor_code=...
GET  /api/products/search?q=...
POST /api/products/link
POST /api/sync/mlcc
GET  /api/sync/status

Next step
---------
Deploy this service to Railway, give the phone app the Railway URL, and update
the Safari app so scans look up the GTIN immediately against this service.
