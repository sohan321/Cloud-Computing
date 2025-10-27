import os, time, json, threading, math, random
from typing import Dict, List, Set
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import yfinance as yf
import pandas as pd

# === Config ===
yf.set_tz_cache_location("custom/cache/location")
POLL_CYCLE_SEC = int(os.getenv("POLL_CYCLE_SEC", "20"))  # full-universe refresh target time (10–20 typical)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))          # tickers per yfinance call
DATA_DELAY_NOTE = os.getenv("DATA_DELAY_NOTE", "Data may be delayed by provider.")
PORT = int(os.getenv("PORT", "8080"))

# === Universe ===
with open("sp500.txt", "r", encoding="utf-8") as f:
    UNIVERSE: List[str] = [ln.strip().upper() for ln in f if ln.strip()]

# === State ===
app = FastAPI()
latest: Dict[str, dict] = {}               # symbol -> {symbol, price, ts}
subscribers: Set[threading.Event] = set()  # SSE nudges
lock = threading.Lock()

# === Batch plan (refresh all symbols within POLL_CYCLE_SEC) ===
def chunk(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

BATCHES = list(chunk(UNIVERSE, BATCH_SIZE))
NUM_BATCHES = max(1, math.ceil(len(UNIVERSE) / BATCH_SIZE))
BATCH_INTERVAL = max(1.0, POLL_CYCLE_SEC / NUM_BATCHES)

# === Data fetching ===
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
            # sometimes a symbol is missing—skip; it will update next cycle
            pass
    return ticks

# === Poller thread ===
def poller():
    idx = 0
    backoff = 0.0              # extra sleep if vendor throttles
    BASE = BATCH_INTERVAL      # planned per-batch interval
    MAX_BACKOFF = 60.0         # cap extra delay to 60s

    while True:
        syms = BATCHES[idx]
        try:
            ticks = fetch_batch(syms)
            changed = False
            with lock:
                for t in ticks:
                    prev = latest.get(t["symbol"])
                    if (not prev) or (prev["price"] != t["price"]):
                        latest[t["symbol"]] = t
                        changed = True
            if changed:
                for ev in list(subscribers):
                    ev.set()
            # success: decay backoff
            backoff = max(0.0, backoff * 0.5)
        except Exception:
            # throttle/backoff on errors; keep last values
            backoff = min(MAX_BACKOFF, max(5.0, backoff * 1.7))

        # sleep planned interval + backoff + small jitter
        time.sleep(BASE + backoff + random.uniform(0, 0.5))
        idx = (idx + 1) % NUM_BATCHES

@app.on_event("startup")
def start():
    threading.Thread(target=poller, daemon=True).start()

# === API Endpoints ===

@app.get("/healthz")
def healthz():
    """Simple health and status check."""
    with lock:
        return {
            "status": "ok",
            "symbols": len(latest),
            "cycle_sec": POLL_CYCLE_SEC,
            "batch_size": BATCH_SIZE,
            "batches": NUM_BATCHES,
            "batch_interval_sec": BATCH_INTERVAL,
            "note": DATA_DELAY_NOTE,
        }

@app.get("/api/snapshot")
def snapshot(q: str = ""):
    """Returns current cached prices for the requested tickers."""
    want = [w.strip().upper() for w in q.split(",") if w.strip()] or UNIVERSE
    with lock:
        data = [latest[s] for s in want if s in latest]
    return JSONResponse({"data": data, "note": DATA_DELAY_NOTE})

@app.get("/sse")
def sse(q: str = ""):
    """Server-Sent Events stream for live updates."""
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

    return StreamingResponse(event_stream(), media_type="text/event-stream")

# === Static UI ===
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    """Serves the dashboard UI."""
    return FileResponse("static/index.html")
