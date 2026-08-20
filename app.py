import asyncio
import io
import json
import os
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler

DB_PATH = os.getenv("DB_PATH", "/data/mlcc_products.db")
SEED_PATH = Path(__file__).parent / "data" / "seed_products.json"
PRICEBOOK_URL = "https://customers.mlcc.michigan.gov/SoM_ProductRegistration/s/search-pricebook"

app = FastAPI(title="CC's LARA MLCC Product Service", version="1.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

sync_lock = threading.Lock()
sync_status = {"running":False,"last_started":None,"last_finished":None,"last_result":None,"last_error":None}

class MappingIn(BaseModel):
    gtin: str
    liquor_code: str

def db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn=db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS products (
        liquor_code TEXT PRIMARY KEY, gtin TEXT, brand_name TEXT, liquor_type TEXT,
        bottle_size TEXT, case_size TEXT, proof TEXT, ada_number TEXT,
        base_price REAL, licensee_price REAL, minimum_shelf_price REAL,
        source TEXT, updated_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_products_gtin ON products(gtin);
    CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand_name);
    CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
    """)
    conn.commit(); conn.close()

def normalize_gtin(value):
    if value is None: return ""
    s=re.sub(r"\D","",str(value))
    return s.lstrip("0") if s else ""

def upsert_product(p,source):
    code=str(p.get("liquor_code") or "").strip()
    if not code: return False
    gtin=normalize_gtin(p.get("gtin")); now=datetime.now(timezone.utc).isoformat()
    conn=db()
    conn.execute("""
    INSERT INTO products (liquor_code,gtin,brand_name,liquor_type,bottle_size,case_size,proof,ada_number,base_price,licensee_price,minimum_shelf_price,source,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(liquor_code) DO UPDATE SET
      gtin=CASE WHEN excluded.gtin!='' THEN excluded.gtin ELSE products.gtin END,
      brand_name=CASE WHEN excluded.brand_name!='' THEN excluded.brand_name ELSE products.brand_name END,
      liquor_type=CASE WHEN excluded.liquor_type!='' THEN excluded.liquor_type ELSE products.liquor_type END,
      bottle_size=CASE WHEN excluded.bottle_size!='' THEN excluded.bottle_size ELSE products.bottle_size END,
      case_size=COALESCE(excluded.case_size,products.case_size),
      proof=COALESCE(excluded.proof,products.proof),
      ada_number=CASE WHEN excluded.ada_number!='' THEN excluded.ada_number ELSE products.ada_number END,
      base_price=COALESCE(excluded.base_price,products.base_price),
      licensee_price=COALESCE(excluded.licensee_price,products.licensee_price),
      minimum_shelf_price=COALESCE(excluded.minimum_shelf_price,products.minimum_shelf_price),
      source=excluded.source,updated_at=excluded.updated_at
    """,(code,gtin,str(p.get("brand_name") or "").strip(),str(p.get("liquor_type") or "").strip(),
         str(p.get("bottle_size") or "").strip(),p.get("case_size"),p.get("proof"),
         str(p.get("ada_number") or "").strip(),p.get("base_price"),p.get("licensee_price"),
         p.get("minimum_shelf_price"),source,now))
    conn.commit(); conn.close(); return True

def load_seed():
    if not SEED_PATH.exists(): return
    for p in json.loads(SEED_PATH.read_text(encoding="utf-8")):
        upsert_product(p,p.get("source","seed"))

def set_meta(key,value):
    conn=db(); conn.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,str(value))); conn.commit(); conn.close()

def get_meta(key):
    conn=db(); row=conn.execute("SELECT value FROM metadata WHERE key=?",(key,)).fetchone(); conn.close(); return row["value"] if row else None

def row_to_product(r): return dict(r) if r else None


ADMIN_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#111111">
  <title>CC's MLCC Catalog</title>
  <style>
    :root{--bg:#f4f5f7;--card:#fff;--text:#111;--muted:#6b7280;--line:#e5e7eb;--accent:#b91c1c;--good:#15803d;--bad:#b91c1c;--blue:#1d4ed8}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
    .wrap{max-width:720px;margin:0 auto;padding:calc(18px + env(safe-area-inset-top)) 16px calc(28px + env(safe-area-inset-bottom))}
    .header{background:#111;color:#fff;border-radius:22px;padding:22px 20px;margin-bottom:16px}.header h1{margin:0 0 6px;font-size:26px}.header p{margin:0;color:#d1d5db;font-size:14px}
    .card{background:var(--card);border:1px solid var(--line);border-radius:20px;padding:18px;margin-bottom:14px}
    .label{color:var(--muted);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px}
    .status-line{display:flex;align-items:center;gap:8px;font-weight:700;font-size:17px;margin-bottom:14px}.dot{width:11px;height:11px;border-radius:50%;background:var(--good)}
    .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.metric{border:1px solid var(--line);border-radius:16px;padding:14px;background:#fafafa}
    .metric .big{font-size:26px;font-weight:800;line-height:1;margin-bottom:6px}.metric .small{color:var(--muted);font-size:12px;font-weight:600}
    .button{display:block;width:100%;min-height:58px;border:0;border-radius:17px;background:var(--accent);color:#fff;font-size:18px;font-weight:800;padding:16px 18px}
    .button:disabled{opacity:.45}.secondary{margin-top:10px;background:#fff;color:#111;border:1px solid var(--line)}
    .syncbox{border-radius:16px;border:1px solid var(--line);background:#fafafa;padding:14px;min-height:76px}.sync-title{font-weight:800;font-size:16px;margin-bottom:6px}
    .muted{color:var(--muted);font-size:13px;line-height:1.45}.good{color:var(--good)}.bad{color:var(--bad)}.blue{color:var(--blue)}
    .spinner{display:inline-block;width:18px;height:18px;border:3px solid #dbeafe;border-top-color:var(--blue);border-radius:50%;animation:spin 1s linear infinite;vertical-align:-3px;margin-right:7px}@keyframes spin{to{transform:rotate(360deg)}}
    .result-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:12px}.result-box{background:white;border:1px solid var(--line);border-radius:13px;padding:10px 8px;text-align:center}
    .result-box strong{display:block;font-size:20px;margin-bottom:3px}.result-box span{font-size:10px;color:var(--muted);font-weight:700}
    .search-row{display:flex;gap:8px;margin-top:10px}.search-row input{flex:1;min-width:0;min-height:48px;border:1px solid var(--line);border-radius:14px;padding:0 14px;font-size:16px}
    .search-row button{min-height:48px;border:0;border-radius:14px;padding:0 16px;font-size:15px;font-weight:800;color:#fff;background:#111}
    .product{margin-top:10px;padding:13px;border-radius:14px;border:1px solid var(--line);background:#fff;font-size:13px;line-height:1.55;overflow-wrap:anywhere}
  </style>
</head>
<body>
<div class="wrap">
  <div class="header"><h1>CC's MLCC Catalog</h1><p>Michigan liquor product database</p></div>
  <div class="card">
    <div class="label">Backend status</div>
    <div class="status-line"><span id="healthDot" class="dot"></span><span id="healthText">Checking...</span></div>
    <div class="grid">
      <div class="metric"><div class="big" id="products">—</div><div class="small">Products</div></div>
      <div class="metric"><div class="big" id="gtinLinked">—</div><div class="small">GTIN / UPC Linked</div></div>
    </div>
    <div class="muted" style="margin-top:12px">Last successful sync: <strong id="lastSync">Never</strong></div>
  </div>
  <div class="card">
    <div class="label">MLCC catalog synchronization</div>
    <button id="syncBtn" class="button" onclick="startSync()">Sync MLCC Catalog</button>
    <button class="button secondary" onclick="refreshAll()">Refresh Status</button>
    <div class="syncbox" style="margin-top:12px" id="syncBox"><div class="sync-title">Ready</div><div class="muted">Tap “Sync MLCC Catalog” to download the latest Michigan catalog data.</div></div>
  </div>
  <div class="card">
    <div class="label">Quick lookup test</div>
    <div class="muted">Test a Liquor Code or scanned GTIN / UPC after the catalog sync completes.</div>
    <div class="search-row"><input id="lookupValue" inputmode="numeric" autocomplete="off" placeholder="Liquor code or UPC"><button onclick="lookupProduct()">Lookup</button></div>
    <div id="lookupResult"></div>
  </div>
</div>
<script>
const syncBtn=document.getElementById("syncBtn"),syncBox=document.getElementById("syncBox");let syncPoll=null,sawRunning=false;
const fmt=n=>Number(n||0).toLocaleString();const safeDate=v=>v?new Date(v).toLocaleString():"Never";
function esc(s){return String(s??"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#039;")}
async function loadHealth(){try{const r=await fetch("/health",{cache:"no-store"}),d=await r.json();healthText.textContent=d.ok?"Online":"Problem detected";products.textContent=fmt(d.products);gtinLinked.textContent=fmt(d.gtin_linked);lastSync.textContent=safeDate(d.last_sync);healthDot.style.background=d.ok?"#15803d":"#b91c1c";if(d.sync&&d.sync.running)sawRunning=true}catch(e){healthText.textContent="Offline";healthDot.style.background="#b91c1c"}}
function showSyncStatus(s){if(s.running){sawRunning=true;syncBtn.disabled=true;syncBox.innerHTML=`<div class="sync-title blue"><span class="spinner"></span>Syncing MLCC catalog...</div><div class="muted">Started: <strong>${safeDate(s.last_started)}</strong></div>`;return}syncBtn.disabled=false;if(s.last_error){syncBox.innerHTML=`<div class="sync-title bad">Sync failed</div><div class="muted bad">${esc(s.last_error)}</div>`;return}if(s.last_result){const r=s.last_result;syncBox.innerHTML=`<div class="sync-title good">Sync complete</div><div class="muted">Finished: <strong>${safeDate(s.last_finished)}</strong></div><div class="result-grid"><div class="result-box"><strong>${fmt(r.products_seen)}</strong><span>FOUND</span></div><div class="result-box"><strong>${fmt(r.products_upserted)}</strong><span>SAVED</span></div><div class="result-box"><strong>${fmt(r.products_with_gtin)}</strong><span>WITH GTIN</span></div></div>`;return}syncBox.innerHTML=`<div class="sync-title">Ready</div><div class="muted">No catalog sync has completed yet.</div>`}
async function loadSyncStatus(){try{const r=await fetch("/api/sync/status",{cache:"no-store"}),s=await r.json();showSyncStatus(s);if(!s.running&&sawRunning){await loadHealth();if(syncPoll){clearInterval(syncPoll);syncPoll=null}}}catch(e){syncBox.innerHTML=`<div class="sync-title bad">Unable to read sync status</div>`}}
async function startSync(){syncBtn.disabled=true;sawRunning=false;syncBox.innerHTML=`<div class="sync-title blue"><span class="spinner"></span>Starting sync...</div><div class="muted">Connecting to the Michigan MLCC catalog.</div>`;try{const r=await fetch("/api/sync/mlcc",{method:"POST"}),d=await r.json();if(!r.ok&&r.status!==202)throw new Error(d.detail||"Unable to start sync");syncPoll=setInterval(loadSyncStatus,2000);setTimeout(loadSyncStatus,500)}catch(e){syncBtn.disabled=false;syncBox.innerHTML=`<div class="sync-title bad">Could not start sync</div><div class="muted bad">${esc(e.message)}</div>`}}
async function refreshAll(){await Promise.all([loadHealth(),loadSyncStatus()])}
async function lookupProduct(){const value=lookupValue.value.trim(),out=lookupResult;if(!value){out.innerHTML=`<div class="product bad">Enter a Liquor Code or UPC.</div>`;return}out.innerHTML=`<div class="product">Looking up...</div>`;const url=value.length<=7?`/api/products/lookup?liquor_code=${encodeURIComponent(value)}`:`/api/products/lookup?gtin=${encodeURIComponent(value)}`;try{const r=await fetch(url,{cache:"no-store"}),d=await r.json();if(!r.ok){out.innerHTML=`<div class="product bad">Product not found.</div>`;return}out.innerHTML=`<div class="product"><strong style="font-size:16px">${esc(d.brand_name||"Unnamed Product")}</strong><br>Liquor Code: <strong>${esc(d.liquor_code||"")}</strong><br>GTIN / UPC: <strong>${esc(d.gtin||"Not linked")}</strong><br>Size: ${esc(d.bottle_size||"—")}<br>Case Size: ${esc(d.case_size??"—")}<br>Type: ${esc(d.liquor_type||"—")}</div>`}catch(e){out.innerHTML=`<div class="product bad">Lookup failed: ${esc(e.message)}</div>`}}
refreshAll();setInterval(loadHealth,30000);
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def admin_root():
    return HTMLResponse(content=ADMIN_HTML)

@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return HTMLResponse(content=ADMIN_HTML)


async def scrape_mlcc():
    from playwright.async_api import async_playwright
    import openpyxl
    found={}
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True,args=["--no-sandbox","--disable-dev-shm-usage"])
        page=await browser.new_page(accept_downloads=True)
        await page.goto(PRICEBOOK_URL,wait_until="domcontentloaded",timeout=120000)
        await page.wait_for_timeout(6000)

        export=page.get_by_role("button",name=re.compile(r"Export All to Excel",re.I))
        if await export.count():
            try:
                async with page.expect_download(timeout=60000) as dl:
                    await export.first.click()
                download=await dl.value
                path=await download.path()
                if path:
                    wb=openpyxl.load_workbook(path,read_only=True,data_only=True); ws=wb.active
                    rows=list(ws.iter_rows(values_only=True))
                    if rows:
                        hdr=[str(x or "").strip() for x in rows[0]]; lower=[x.lower() for x in hdr]
                        def idx(*names):
                            for name in names:
                                for i,h in enumerate(lower):
                                    if name in h: return i
                            return None
                        i_code=idx("liquor code","liq code"); i_gtin=idx("gtin","upc","barcode"); i_brand=idx("brand name","product name","description")
                        i_size=idx("size"); i_type=idx("liquor type","type"); i_case=idx("case size","pack"); i_proof=idx("proof"); i_ada=idx("ada")
                        if i_code is not None:
                            for row in rows[1:]:
                                code=row[i_code] if i_code<len(row) else None
                                if not code: continue
                                item={"liquor_code":str(code).strip(),
                                      "gtin":str(row[i_gtin]).strip() if i_gtin is not None and i_gtin<len(row) and row[i_gtin] is not None else "",
                                      "brand_name":str(row[i_brand]).strip() if i_brand is not None and i_brand<len(row) and row[i_brand] is not None else "",
                                      "bottle_size":str(row[i_size]).strip() if i_size is not None and i_size<len(row) and row[i_size] is not None else "",
                                      "liquor_type":str(row[i_type]).strip() if i_type is not None and i_type<len(row) and row[i_type] is not None else "",
                                      "case_size":row[i_case] if i_case is not None and i_case<len(row) else None,
                                      "proof":row[i_proof] if i_proof is not None and i_proof<len(row) else None,
                                      "ada_number":str(row[i_ada]).strip() if i_ada is not None and i_ada<len(row) and row[i_ada] is not None else ""}
                                found[item["liquor_code"]]=item
            except Exception:
                pass

        has_gtin=any(normalize_gtin(x.get("gtin")) for x in found.values())
        if not found or not has_gtin:
            combo=page.get_by_text(re.compile(r"Select an Option|81-ALCOHOL",re.I))
            if await combo.count():
                await combo.first.click(); await page.wait_for_timeout(500)
            option_texts=await page.locator('[role="option"], lightning-base-combobox-item, .slds-listbox__option').all_inner_texts()
            options=[]
            for t in option_texts:
                t=" ".join(t.split())
                if t and t.lower() not in {"none","select an option"} and t not in options: options.append(t)
            if not options:
                body=await page.locator("body").inner_text()
                for m in re.findall(r"(?m)^\s*(\d{1,3}-[A-Z][A-Z0-9 '&()/.-]+)\s*$",body):
                    if m not in options: options.append(m)

            for opt in options:
                try:
                    current=page.get_by_text(re.compile(r"Select an Option|^\d{1,3}-",re.I))
                    if await current.count():
                        await current.first.click(); await page.wait_for_timeout(250)
                    choice=page.get_by_text(opt,exact=True)
                    if not await choice.count(): continue
                    await choice.first.click()
                    search=page.get_by_role("button",name=re.compile(r"^Search$",re.I))
                    if await search.count(): await search.first.click()
                    await page.wait_for_timeout(2500)
                    blocks=page.locator("article, tr, .slds-card, .slds-box, li")
                    for i in range(min(await blocks.count(),5000)):
                        txt=" ".join((await blocks.nth(i).inner_text()).split())
                        if not txt: continue
                        cm=re.search(r"(?:Liquor\s*Code|Liq(?:uor)?\s*Code)\s*[:#]?\s*(\d{3,7})",txt,re.I)
                        gm=re.search(r"(?:GTIN|UPC|Barcode)\s*[:#]?\s*([0-9]{8,14})",txt,re.I)
                        if not cm: continue
                        code=cm.group(1); gtin=gm.group(1) if gm else ""
                        bm=re.search(r"(?:Brand\s*Name|Product\s*Name|Description)\s*[:#]?\s*(.+?)(?=\s+(?:Liquor\s*Code|GTIN|UPC|Size|Proof|Pack|ADA)\b|$)",txt,re.I)
                        sm=re.search(r"(?:Bottle\s*Size|Size)\s*[:#]?\s*([0-9.]+\s*(?:ML|L))",txt,re.I)
                        found[code]={**found.get(code,{}),"liquor_code":code,"gtin":gtin or found.get(code,{}).get("gtin",""),
                                     "brand_name":bm.group(1).strip() if bm else found.get(code,{}).get("brand_name",""),
                                     "bottle_size":sm.group(1).strip() if sm else found.get(code,{}).get("bottle_size",""),
                                     "liquor_type":opt}
                except Exception:
                    continue
        await browser.close()

    updated=sum(1 for item in found.values() if upsert_product(item,"MLCC Search Pricebook"))
    return {"products_seen":len(found),"products_upserted":updated,"products_with_gtin":sum(1 for x in found.values() if normalize_gtin(x.get("gtin")))}

def run_sync_job():
    if not sync_lock.acquire(blocking=False): return
    try:
        sync_status["running"]=True; sync_status["last_started"]=datetime.now(timezone.utc).isoformat(); sync_status["last_error"]=None
        result=asyncio.run(scrape_mlcc())
        sync_status["last_result"]=result; sync_status["last_finished"]=datetime.now(timezone.utc).isoformat()
        set_meta("last_sync",sync_status["last_finished"]); set_meta("last_sync_result",json.dumps(result))
    except Exception as e:
        sync_status["last_error"]=f"{type(e).__name__}: {e}"; sync_status["last_finished"]=datetime.now(timezone.utc).isoformat()
    finally:
        sync_status["running"]=False; sync_lock.release()

@app.on_event("startup")
def startup():
    init_db(); load_seed()
    scheduler=BackgroundScheduler()
    scheduler.add_job(run_sync_job,"cron",hour=3,minute=0,id="nightly-mlcc-sync",replace_existing=True)
    scheduler.start(); app.state.scheduler=scheduler

@app.get("/health")
def health():
    conn=db(); total=conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]; gtin=conn.execute("SELECT COUNT(*) c FROM products WHERE gtin IS NOT NULL AND gtin != ''").fetchone()["c"]; conn.close()
    return {"ok":True,"products":total,"gtin_linked":gtin,"last_sync":get_meta("last_sync"),"sync":sync_status}

@app.get("/api/products/lookup")
def lookup(liquor_code:str|None=Query(default=None),gtin:str|None=Query(default=None)):
    if not liquor_code and not gtin: raise HTTPException(400,"Provide liquor_code or gtin")
    conn=db()
    if liquor_code:
        row=conn.execute("SELECT * FROM products WHERE liquor_code=?",(str(liquor_code).strip(),)).fetchone()
    else:
        ng=normalize_gtin(gtin); row=conn.execute("SELECT * FROM products WHERE ltrim(gtin,'0')=? OR gtin=? LIMIT 1",(ng,str(gtin).strip())).fetchone()
    conn.close()
    if not row: raise HTTPException(404,"Product not found")
    return row_to_product(row)

@app.get("/api/products/search")
def search(q:str=Query(min_length=1),limit:int=Query(default=50,ge=1,le=200)):
    qq=q.strip(); ng=normalize_gtin(qq); conn=db()
    rows=conn.execute("""SELECT * FROM products WHERE liquor_code LIKE ? OR gtin LIKE ? OR brand_name LIKE ?
                        ORDER BY CASE WHEN liquor_code=? THEN 0 WHEN gtin=? THEN 1 ELSE 2 END, brand_name LIMIT ?""",
                      (f"%{qq}%",f"%{ng}%",f"%{qq}%",qq,ng,limit)).fetchall()
    conn.close(); return {"items":[row_to_product(r) for r in rows]}

@app.post("/api/products/link")
def link_mapping(item:MappingIn):
    gtin=normalize_gtin(item.gtin); code=item.liquor_code.strip()
    if not gtin or not code: raise HTTPException(400,"gtin and liquor_code are required")
    conn=db(); cur=conn.execute("UPDATE products SET gtin=?, updated_at=? WHERE liquor_code=?",(gtin,datetime.now(timezone.utc).isoformat(),code)); conn.commit(); conn.close()
    if cur.rowcount==0: raise HTTPException(404,"Liquor code not found")
    return {"ok":True,"gtin":gtin,"liquor_code":code}

@app.post("/api/sync/mlcc")
def sync_mlcc(background_tasks:BackgroundTasks):
    if sync_status["running"]:
        return JSONResponse({"ok":True,"message":"Sync already running","sync":sync_status},status_code=202)
    background_tasks.add_task(run_sync_job)
    return JSONResponse({"ok":True,"message":"MLCC sync started","sync":sync_status},status_code=202)

@app.get("/api/sync/status")
def sync_state():
    return sync_status
