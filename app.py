import os, time, json, threading, math, random
from typing import Dict, List, Set
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# === Config ===
yf.set_tz_cache_location("custom/cache/location")
POLL_CYCLE_SEC = int(os.getenv("POLL_CYCLE_SEC", "120"))  # full-universe refresh target time
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))           # tickers per yfinance call
DATA_DELAY_NOTE = os.getenv("DATA_DELAY_NOTE", "Data may be delayed by provider.")
PORT = int(os.getenv("PORT", "8080"))

# === BigQuery Config ===
PROJECT_ID = os.getenv("PROJECT_ID", "cloud-project-476018")
DATASET_ID = os.getenv("DATASET_ID", "stocks_dataset")
TABLE_ID = os.getenv("TABLE_ID", "stock_prices")
ENABLE_BIGQUERY = os.getenv("ENABLE_BIGQUERY", "false").lower() == "true"

# Initialize BigQuery client if enabled
bq_client = None
if ENABLE_BIGQUERY:
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        print(f"BigQuery client initialized: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    except Exception as e:
        print(f"BigQuery initialization failed: {e}")
        ENABLE_BIGQUERY = False

# === Universe ===
with open("sp500.txt", "r", encoding="utf-8") as f:
    UNIVERSE: List[str] = [ln.strip().upper() for ln in f if ln.strip()]

# === State ===
app = FastAPI()
latest: Dict[str, dict] = {}               # symbol -> {symbol, price, ts}
subscribers: Set[threading.Event] = set()  # SSE nudges
lock = threading.Lock()

# === Batch plan ===
def chunk(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

BATCHES = list(chunk(UNIVERSE, BATCH_SIZE))
NUM_BATCHES = max(1, math.ceil(len(UNIVERSE) / BATCH_SIZE))
BATCH_INTERVAL = max(1.0, POLL_CYCLE_SEC / NUM_BATCHES)

# === BigQuery Storage ===
def store_to_bigquery(ticks: List[dict]):
    """Store price data to BigQuery"""
    if not ENABLE_BIGQUERY or not bq_client or not ticks:
        return

    try:
        # Convert to BigQuery format
        rows = []
        for t in ticks:
            rows.append({
                "symbol": t["symbol"],
                "price": float(t["price"]),
                "timestamp": datetime.fromtimestamp(t["ts"] / 1000).isoformat()
            })

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        errors = bq_client.insert_rows_json(table_ref, rows)

        if errors:
            print(f"BigQuery insert errors: {errors}")
        else:
            print(f"Inserted {len(rows)} rows to BigQuery")
    except Exception as e:
        print(f"BigQuery storage error: {e}")

# === Data fetch ===
def fetch_batch(symbols: List[str]) -> List[dict]:
    if not symbols:
        return []
    df = yf.download(
        tickers=" ".join(symbols),
        period="1d",
        interval="1m",
        group_by="ticker",
        threads=True,
        progress=False
    )
    now_ms = int(time.time() * 1000)
    ticks = []
    for s in symbols:
        try:
            close = float(pd.DataFrame(df[s]["Close"]).dropna().iloc[-1, 0])
            ticks.append({"symbol": s, "price": close, "ts": now_ms})
        except Exception:
            # symbol missing or no data in this interval; skip
            pass

    # Store to BigQuery if enabled
    if ticks:
        store_to_bigquery(ticks)

    return ticks

# === Viewer-driven poller ===
active_viewers = 0
poller_thread = None
stop_flag = threading.Event()

def poller():
    idx = 0
    backoff = 0.0
    BASE = BATCH_INTERVAL
    MAX_BACKOFF = 60.0
    last_push_ms = 0  # heartbeat timer

    while not stop_flag.is_set():
        syms = BATCHES[idx]
        try:
            ticks = fetch_batch(syms)
            changed = False
            now_ms = int(time.time() * 1000)

            with lock:
                for t in ticks:
                    prev = latest.get(t["symbol"])
                    # check if price changed (rounded to 2 decimal places)
                    price_changed = (not prev) or (round(prev["price"], 2) != round(t["price"], 2))

                    # always refresh cache so timestamp stays current
                    latest[t["symbol"]] = t
                    if price_changed:
                        changed = True

            # heartbeat: push at least once every 30 s even if nothing changed
            heartbeat = (now_ms - last_push_ms) > 30_000

            if changed or heartbeat:
                for ev in list(subscribers):
                    ev.set()
                last_push_ms = now_ms

            backoff = max(0.0, backoff * 0.5)
        except Exception:
            # exponential backoff on vendor/rate-limit errors
            backoff = min(MAX_BACKOFF, max(5.0, backoff * 1.7))

        time.sleep(BASE + backoff + random.uniform(0, 0.5))
        idx = (idx + 1) % NUM_BATCHES

def ensure_poller_running():
    global poller_thread
    if poller_thread is None or not poller_thread.is_alive():
        stop_flag.clear()
        poller_thread = threading.Thread(target=poller, daemon=True)
        poller_thread.start()

def maybe_stop_poller():
    if active_viewers == 0:
        stop_flag.set()

# === API Endpoints (put BEFORE static mount) ===
@app.get("/ping")
def ping():
    return "pong"

@app.get("/healthz")
def healthz():
    with lock:
        return {
            "status": "ok",
            "symbols": len(latest),
            "cycle_sec": POLL_CYCLE_SEC,
            "batch_size": BATCH_SIZE,
            "batches": NUM_BATCHES,
            "batch_interval_sec": BATCH_INTERVAL,
            "note": DATA_DELAY_NOTE,
            "active_viewers": active_viewers,
            "bigquery_enabled": ENABLE_BIGQUERY,
        }

@app.post("/api/store-current")
def store_current():
    """Manually store current in-memory data to BigQuery"""
    if not ENABLE_BIGQUERY:
        return JSONResponse({"error": "BigQuery not enabled. Set ENABLE_BIGQUERY=true"}, status_code=400)

    with lock:
        ticks = list(latest.values())

    if not ticks:
        return JSONResponse({"error": "No data available to store"}, status_code=400)

    store_to_bigquery(ticks)
    return {"status": "success", "stored": len(ticks)}

@app.get("/api/collect")
def collect_and_store():
    """Fetch fresh data and store to BigQuery (for Cloud Scheduler)"""
    if not ENABLE_BIGQUERY:
        return JSONResponse({"error": "BigQuery not enabled"}, status_code=400)

    # Rotate through all stocks - pick random batch each time
    import random
    batch_to_fetch = random.sample(UNIVERSE, min(50, len(UNIVERSE)))
    ticks = fetch_batch(batch_to_fetch)

    return {
        "status": "success",
        "fetched": len(ticks),
        "symbols_sampled": len(batch_to_fetch),
        "stored_to_bigquery": ENABLE_BIGQUERY
    }
    
@app.get("/status")
def status():
    with lock:
        return {
            "status": "ok",
            "symbols": len(latest),
            "cycle_sec": POLL_CYCLE_SEC,
            "batch_size": BATCH_SIZE,
            "batches": NUM_BATCHES,
            "batch_interval_sec": BATCH_INTERVAL,
            "note": DATA_DELAY_NOTE,
            "active_viewers": active_viewers,
        }

@app.get("/hz")
def hz():
    return status()

@app.get("/api/snapshot")
def snapshot(q: str = ""):
    want = [w.strip().upper() for w in q.split(",") if w.strip()] or UNIVERSE
    with lock:
        data = [latest[s] for s in want if s in latest]
    return JSONResponse({"data": data, "note": DATA_DELAY_NOTE})

@app.get("/sse")
def sse(q: str = ""):
    global active_viewers
    active_viewers += 1
    ensure_poller_running()

    want = [w.strip().upper() for w in q.split(",") if w.strip()] or UNIVERSE
    stop = threading.Event()
    subscribers.add(stop)

    def event_stream():
        try:
            with lock:
                init = [latest[s] for s in want if s in latest]
            yield f"data: {json.dumps({'type':'snapshot','data':init,'note':DATA_DELAY_NOTE})}\n\n"
            while not stop.is_set():
                stop.wait(timeout=20)
                with lock:
                    data = [latest[s] for s in want if s in latest]
                yield f"data: {json.dumps({'type':'update','data':data,'note':DATA_DELAY_NOTE})}\n\n"
                stop.clear()
        finally:
            subscribers.discard(stop)
            global active_viewers
            active_viewers = max(0, active_viewers - 1)
            maybe_stop_poller()

    return StreamingResponse(event_stream(), media_type="text/event-stream")

# === Static UI ===
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/index.html")

@app.get("/routes")
def routes():
    return [r.path for r in app.routes]
