
import asyncio
import io
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler

DB_PATH = os.getenv("DB_PATH", "/data/mlcc_products.db")
SEED_PATH = Path(__file__).parent / "data" / "seed_products.json"
PRICEBOOK_URL = "https://customers.mlcc.michigan.gov/SoM_ProductRegistration/s/search-pricebook"

app = FastAPI(title="CC's LARA MLCC Product Service", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

sync_lock = threading.Lock()
sync_status = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "last_result": None,
    "last_error": None,
}

class MappingIn(BaseModel):
    gtin: str
    liquor_code: str

def db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS products (
        liquor_code TEXT PRIMARY KEY,
        gtin TEXT,
        brand_name TEXT,
        liquor_type TEXT,
        bottle_size TEXT,
        case_size TEXT,
        proof TEXT,
        ada_number TEXT,
        base_price REAL,
        licensee_price REAL,
        minimum_shelf_price REAL,
        source TEXT,
        updated_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_products_gtin ON products(gtin);
    CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand_name);
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    conn.commit()
    conn.close()

def upsert_product(p, source):
    liquor_code = str(p.get("liquor_code") or "").strip()
    if not liquor_code:
        return False
    gtin = normalize_gtin(p.get("gtin"))
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    conn.execute("""
    INSERT INTO products (
        liquor_code, gtin, brand_name, liquor_type, bottle_size, case_size,
        proof, ada_number, base_price, licensee_price, minimum_shelf_price,
        source, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(liquor_code) DO UPDATE SET
        gtin = CASE WHEN excluded.gtin != '' THEN excluded.gtin ELSE products.gtin END,
        brand_name = CASE WHEN excluded.brand_name != '' THEN excluded.brand_name ELSE products.brand_name END,
        liquor_type = CASE WHEN excluded.liquor_type != '' THEN excluded.liquor_type ELSE products.liquor_type END,
        bottle_size = CASE WHEN excluded.bottle_size != '' THEN excluded.bottle_size ELSE products.bottle_size END,
        case_size = COALESCE(excluded.case_size, products.case_size),
        proof = COALESCE(excluded.proof, products.proof),
        ada_number = CASE WHEN excluded.ada_number != '' THEN excluded.ada_number ELSE products.ada_number END,
        base_price = COALESCE(excluded.base_price, products.base_price),
        licensee_price = COALESCE(excluded.licensee_price, products.licensee_price),
        minimum_shelf_price = COALESCE(excluded.minimum_shelf_price, products.minimum_shelf_price),
        source = excluded.source,
        updated_at = excluded.updated_at
    """, (
        liquor_code, gtin, str(p.get("brand_name") or "").strip(),
        str(p.get("liquor_type") or "").strip(), str(p.get("bottle_size") or "").strip(),
        p.get("case_size"), p.get("proof"), str(p.get("ada_number") or "").strip(),
        p.get("base_price"), p.get("licensee_price"), p.get("minimum_shelf_price"),
        source, now
    ))
    conn.commit()
    conn.close()
    return True

def normalize_gtin(value):
    if value is None:
        return ""
    s = re.sub(r"\D", "", str(value))
    return s.lstrip("0") if s else ""

def load_seed():
    if not SEED_PATH.exists():
        return
    items = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    for p in items:
        upsert_product(p, p.get("source", "seed"))

def set_meta(key, value):
    conn = db()
    conn.execute("""
    INSERT INTO metadata(key,value) VALUES (?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, str(value)))
    conn.commit()
    conn.close()

def get_meta(key):
    conn = db()
    row = conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else None

def row_to_product(r):
    return dict(r) if r else None

async def scrape_mlcc():
    """
    Public MLCC Search Pricebook sync.

    Strategy:
    1) Open Search Pricebook in Chromium.
    2) Prefer 'Export All to Excel' because it is the least brittle and fastest.
    3) If the export does not expose GTIN/UPC, fall back to iterating Liquor Type
       options, pressing Search, and scraping result cards/rows including details.
    4) Merge every record by Liquor Code, with GTIN as a searchable secondary key.

    The selectors intentionally use visible labels/text first because the MLCC
    page is a Salesforce app and generated DOM IDs are unstable.
    """
    from playwright.async_api import async_playwright
    import openpyxl

    found = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page(accept_downloads=True)
        await page.goto(PRICEBOOK_URL, wait_until="domcontentloaded", timeout=120000)
        await page.wait_for_timeout(6000)

        # Try the page-level Export All to Excel first.
        export = page.get_by_role("button", name=re.compile(r"Export All to Excel", re.I))
        if await export.count():
            try:
                async with page.expect_download(timeout=60000) as dl_info:
                    await export.first.click()
                download = await dl_info.value
                data = await download.create_read_stream()
                raw = await data.read()
                wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
                ws = wb.active
                rows = list(ws.iter_rows(values_only=True))
                if rows:
                    hdr = [str(x or "").strip() for x in rows[0]]
                    lower = [x.lower() for x in hdr]
                    def idx(*names):
                        for name in names:
                            for i, h in enumerate(lower):
                                if name in h:
                                    return i
                        return None
                    i_code = idx("liquor code", "liq code")
                    i_gtin = idx("gtin", "upc", "barcode")
                    i_brand = idx("brand name", "product name", "description")
                    i_size = idx("size")
                    i_type = idx("liquor type", "type")
                    i_case = idx("case size", "pack")
                    i_proof = idx("proof")
                    i_ada = idx("ada")
                    if i_code is not None:
                        for row in rows[1:]:
                            code = row[i_code] if i_code < len(row) else None
                            if not code:
                                continue
                            item = {
                                "liquor_code": str(code).strip(),
                                "gtin": str(row[i_gtin]).strip() if i_gtin is not None and i_gtin < len(row) and row[i_gtin] is not None else "",
                                "brand_name": str(row[i_brand]).strip() if i_brand is not None and i_brand < len(row) and row[i_brand] is not None else "",
                                "bottle_size": str(row[i_size]).strip() if i_size is not None and i_size < len(row) and row[i_size] is not None else "",
                                "liquor_type": str(row[i_type]).strip() if i_type is not None and i_type < len(row) and row[i_type] is not None else "",
                                "case_size": row[i_case] if i_case is not None and i_case < len(row) else None,
                                "proof": row[i_proof] if i_proof is not None and i_proof < len(row) else None,
                                "ada_number": str(row[i_ada]).strip() if i_ada is not None and i_ada < len(row) and row[i_ada] is not None else "",
                            }
                            found[item["liquor_code"]] = item
            except Exception:
                pass

        # If the export gave us codes but not GTINs, or failed entirely, scrape
        # category results and product detail text.
        has_gtin = any(normalize_gtin(x.get("gtin")) for x in found.values())
        if not found or not has_gtin:
            # Open Liquor Type dropdown and capture every option.
            # This follows the mobile UI shown by the user.
            combo = page.get_by_text(re.compile(r"Select an Option|81-ALCOHOL", re.I))
            if await combo.count():
                await combo.first.click()
                await page.wait_for_timeout(500)

            # Generic option extraction from the visible Salesforce listbox.
            option_texts = await page.locator('[role="option"], lightning-base-combobox-item, .slds-listbox__option').all_inner_texts()
            options = []
            for t in option_texts:
                t = " ".join(t.split())
                if t and t.lower() not in {"none", "select an option"} and t not in options:
                    options.append(t)

            # If the control did not expose roles, use visible text patterns.
            if not options:
                body_text = await page.locator("body").inner_text()
                for m in re.findall(r"(?m)^\s*(\d{1,3}-[A-Z][A-Z0-9 '&()/.-]+)\s*$", body_text):
                    if m not in options:
                        options.append(m)

            # Process each category.
            for opt in options:
                try:
                    # Re-open dropdown each time.
                    current_combo = page.get_by_text(re.compile(r"Select an Option|^\d{1,3}-", re.I))
                    if await current_combo.count():
                        await current_combo.first.click()
                        await page.wait_for_timeout(250)

                    choice = page.get_by_text(opt, exact=True)
                    if await choice.count():
                        await choice.first.click()
                    else:
                        continue

                    search_btn = page.get_by_role("button", name=re.compile(r"^Search$", re.I))
                    if await search_btn.count():
                        await search_btn.first.click()
                    await page.wait_for_timeout(2500)

                    # Scrape visible result rows/cards. Product detail text usually
                    # contains Liquor Code and GTIN/UPC labels.
                    blocks = page.locator("article, tr, .slds-card, .slds-box, li")
                    count = min(await blocks.count(), 5000)
                    for i in range(count):
                        txt = " ".join((await blocks.nth(i).inner_text()).split())
                        if not txt:
                            continue
                        code_m = re.search(r"(?:Liquor\s*Code|Liq(?:uor)?\s*Code)\s*[:#]?\s*(\d{3,7})", txt, re.I)
                        gtin_m = re.search(r"(?:GTIN|UPC|Barcode)\s*[:#]?\s*([0-9]{8,14})", txt, re.I)
                        if not code_m:
                            continue
                        code = code_m.group(1)
                        gtin = gtin_m.group(1) if gtin_m else ""
                        brand_m = re.search(r"(?:Brand\s*Name|Product\s*Name|Description)\s*[:#]?\s*(.+?)(?=\s+(?:Liquor\s*Code|GTIN|UPC|Size|Proof|Pack|ADA)\b|$)", txt, re.I)
                        size_m = re.search(r"(?:Bottle\s*Size|Size)\s*[:#]?\s*([0-9.]+\s*(?:ML|L))", txt, re.I)
                        found[code] = {
                            **found.get(code, {}),
                            "liquor_code": code,
                            "gtin": gtin or found.get(code, {}).get("gtin", ""),
                            "brand_name": brand_m.group(1).strip() if brand_m else found.get(code, {}).get("brand_name", ""),
                            "bottle_size": size_m.group(1).strip() if size_m else found.get(code, {}).get("bottle_size", ""),
                            "liquor_type": opt,
                        }
                except Exception:
                    continue

        await browser.close()

    updated = 0
    for item in found.values():
        if upsert_product(item, "MLCC Search Pricebook"):
            updated += 1

    return {
        "products_seen": len(found),
        "products_upserted": updated,
        "products_with_gtin": sum(1 for x in found.values() if normalize_gtin(x.get("gtin"))),
    }

def run_sync_job():
    if not sync_lock.acquire(blocking=False):
        return
    try:
        sync_status["running"] = True
        sync_status["last_started"] = datetime.now(timezone.utc).isoformat()
        sync_status["last_error"] = None
        result = asyncio.run(scrape_mlcc())
        sync_status["last_result"] = result
        sync_status["last_finished"] = datetime.now(timezone.utc).isoformat()
        set_meta("last_sync", sync_status["last_finished"])
        set_meta("last_sync_result", json.dumps(result))
    except Exception as e:
        sync_status["last_error"] = f"{type(e).__name__}: {e}"
        sync_status["last_finished"] = datetime.now(timezone.utc).isoformat()
    finally:
        sync_status["running"] = False
        sync_lock.release()

@app.on_event("startup")
def startup():
    init_db()
    load_seed()

    # Nightly refresh at 3:00 AM local-ish server time.
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_sync_job, "cron", hour=3, minute=0, id="nightly-mlcc-sync", replace_existing=True)
    scheduler.start()
    app.state.scheduler = scheduler

@app.get("/health")
def health():
    conn = db()
    total = conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
    gtin = conn.execute("SELECT COUNT(*) c FROM products WHERE gtin IS NOT NULL AND gtin != ''").fetchone()["c"]
    conn.close()
    return {
        "ok": True,
        "products": total,
        "gtin_linked": gtin,
        "last_sync": get_meta("last_sync"),
        "sync": sync_status,
    }

@app.get("/api/products/lookup")
def lookup(
    liquor_code: str | None = Query(default=None),
    gtin: str | None = Query(default=None),
):
    if not liquor_code and not gtin:
        raise HTTPException(400, "Provide liquor_code or gtin")
    conn = db()
    row = None
    if liquor_code:
        row = conn.execute("SELECT * FROM products WHERE liquor_code=?", (str(liquor_code).strip(),)).fetchone()
    else:
        ng = normalize_gtin(gtin)
        row = conn.execute(
            "SELECT * FROM products WHERE ltrim(gtin,'0')=? OR gtin=? LIMIT 1",
            (ng, str(gtin).strip())
        ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Product not found")
    return row_to_product(row)

@app.get("/api/products/search")
def search(q: str = Query(min_length=1), limit: int = Query(default=50, ge=1, le=200)):
    qq = q.strip()
    conn = db()
    rows = conn.execute("""
        SELECT * FROM products
        WHERE liquor_code LIKE ?
           OR gtin LIKE ?
           OR brand_name LIKE ?
        ORDER BY
            CASE WHEN liquor_code = ? THEN 0 WHEN gtin = ? THEN 1 ELSE 2 END,
            brand_name
        LIMIT ?
    """, (f"%{qq}%", f"%{normalize_gtin(qq)}%", f"%{qq}%", qq, normalize_gtin(qq), limit)).fetchall()
    conn.close()
    return {"items": [row_to_product(r) for r in rows]}

@app.post("/api/products/link")
def link_mapping(item: MappingIn):
    gtin = normalize_gtin(item.gtin)
    code = item.liquor_code.strip()
    if not gtin or not code:
        raise HTTPException(400, "gtin and liquor_code are required")
    conn = db()
    cur = conn.execute("UPDATE products SET gtin=?, updated_at=? WHERE liquor_code=?",
                       (gtin, datetime.now(timezone.utc).isoformat(), code))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise HTTPException(404, "Liquor code not found")
    return {"ok": True, "gtin": gtin, "liquor_code": code}

@app.post("/api/sync/mlcc")
def sync_mlcc(background_tasks: BackgroundTasks):
    if sync_status["running"]:
        return JSONResponse({"ok": True, "message": "Sync already running", "sync": sync_status}, status_code=202)
    background_tasks.add_task(run_sync_job)
    return JSONResponse({"ok": True, "message": "MLCC sync started", "sync": sync_status}, status_code=202)

@app.get("/api/sync/status")
def sync_state():
    return sync_status
