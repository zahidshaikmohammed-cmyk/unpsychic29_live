import csv
import io
import os
import threading
import time as sleep_time
from datetime import datetime, time
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify

app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")
DHAN_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

STOCKS = ["KEI", "POLYMED", "NATIONALUM", "TRAVELFOOD"]
DECISION_CANDLES = {
    "KEI": "09:29",
    "POLYMED": "09:28",
    "NATIONALUM": "09:45",
    "TRAVELFOOD": "09:50",
}

MARKET_START = time(9, 15)
MARKET_END = time(15, 30)
POLL_SECONDS = 15
MASTER_TTL_SECONDS = 21600
HTTP_TIMEOUT_SECONDS = 20

# This service deliberately uses MEMORY ONLY.
# No database, SQLite file, JSON file, or persistent storage is used.
# A Render restart/spindown clears this memory; the next startup resyncs
# whatever Dhan currently makes available for the current session.
_state_lock = threading.RLock()
_state = {
    "session_date": None,
    "collector_started_at": None,
    "last_cycle_started_at": None,
    "last_cycle_finished_at": None,
    "last_successful_cycle_at": None,
    "cycle_count": 0,
    "successful_cycle_count": 0,
    "failed_cycle_count": 0,
    "collector_alive": False,
    "collector_error": None,
    "stocks": {
        symbol: {
            "security_id": None,
            "candles": {},
            "last_dhan_fetch_at": None,
            "last_dhan_success_at": None,
            "last_candle_time": None,
            "candle_count": 0,
            "last_error": None,
            "successful_fetches": 0,
            "failed_fetches": 0,
        }
        for symbol in STOCKS
    },
}

_session = {"master": None, "master_loaded_at": None}


def first_env(*names):
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return ""


def dhan_token():
    return first_env(
        "DHAN_ACCESS_TOKEN",
        "DHAN_ACCESS_TOKEN_JWT",
        "DHAN_API_ACCESS_TOKEN",
        "ACCESS_TOKEN",
    )


def now_ist():
    return datetime.now(IST)


def market_window(now):
    start = datetime.combine(now.date(), MARKET_START, tzinfo=IST)
    end = datetime.combine(now.date(), MARKET_END, tzinfo=IST)
    return start, end


def market_status(now):
    if now.weekday() >= 5:
        return "WEEKEND"
    if now.time() < MARKET_START:
        return "PREMARKET"
    if now.time() <= MARKET_END:
        return "OPEN"
    return "CLOSED"


def load_master():
    now = now_ist()
    loaded = _session.get("master")
    loaded_at = _session.get("master_loaded_at")
    if loaded is not None and loaded_at is not None:
        if (now - loaded_at).total_seconds() < MASTER_TTL_SECONDS:
            return loaded

    response = requests.get(DHAN_SCRIP_MASTER_URL, timeout=30)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    rows = {}
    for row in reader:
        if row.get("SEM_EXM_EXCH_ID") != "NSE":
            continue
        if row.get("SEM_INSTRUMENT_NAME") != "EQUITY":
            continue
        symbol = (row.get("SEM_TRADING_SYMBOL") or "").strip().upper()
        security_id = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
        if symbol and security_id:
            rows[symbol] = security_id

    missing = [symbol for symbol in STOCKS if symbol not in rows]
    if missing:
        raise RuntimeError("Dhan security master missing: " + ", ".join(missing))

    _session["master"] = rows
    _session["master_loaded_at"] = now
    return rows


def parse_epoch(value):
    return datetime.fromtimestamp(int(value), tz=IST)


def fetch_stock(symbol, security_id, now):
    session_start, session_end = market_window(now)
    effective_end = min(now, session_end)

    from_date = session_start.strftime("%Y-%m-%d %H:%M:%S")
    to_date = effective_end.strftime("%Y-%m-%d %H:%M:%S")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": dhan_token(),
    }
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "interval": "1",
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }

    response = requests.post(
        DHAN_INTRADAY_URL,
        headers=headers,
        json=payload,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Dhan HTTP {response.status_code}: {response.text[:300]}")

    body = response.json()
    timestamps = body.get("timestamp") or []
    opens = body.get("open") or []
    highs = body.get("high") or []
    lows = body.get("low") or []
    closes = body.get("close") or []
    volumes = body.get("volume") or []

    length = min(
        len(timestamps),
        len(opens),
        len(highs),
        len(lows),
        len(closes),
        len(volumes),
    )

    candles = []
    for i in range(length):
        dt = parse_epoch(timestamps[i])
        if dt < session_start or dt > effective_end:
            continue

        # Every candle exposed by this service is copied from Dhan exactly.
        # Nothing is calculated, interpolated, filled, or synthetically built.
        candles.append(
            {
                "timestamp": dt.strftime("%H:%M"),
                "timestamp_iso": dt.isoformat(),
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": volumes[i],
                "source": "DHAN",
            }
        )

    candles.sort(key=lambda x: x["timestamp_iso"])
    return candles


def reset_for_new_day(session_date):
    with _state_lock:
        if _state["session_date"] == session_date:
            return

        _state["session_date"] = session_date
        _state["cycle_count"] = 0
        _state["successful_cycle_count"] = 0
        _state["failed_cycle_count"] = 0
        _state["last_cycle_started_at"] = None
        _state["last_cycle_finished_at"] = None
        _state["last_successful_cycle_at"] = None
        _state["collector_error"] = None

        for symbol in STOCKS:
            _state["stocks"][symbol]["candles"] = {}
            _state["stocks"][symbol]["candle_count"] = 0
            _state["stocks"][symbol]["last_dhan_fetch_at"] = None
            _state["stocks"][symbol]["last_dhan_success_at"] = None
            _state["stocks"][symbol]["last_candle_time"] = None
            _state["stocks"][symbol]["last_error"] = None
            _state["stocks"][symbol]["successful_fetches"] = 0
            _state["stocks"][symbol]["failed_fetches"] = 0


def store_dhan_candles(symbol, candles, fetched_at):
    with _state_lock:
        stock = _state["stocks"][symbol]
        stock["last_dhan_fetch_at"] = fetched_at

        if not candles:
            stock["last_error"] = "Dhan returned no 1-minute candles for current session"
            stock["failed_fetches"] += 1
            return 0

        # Upsert by Dhan's own timestamp. This means an in-progress Dhan candle,
        # if Dhan exposes one, is refreshed from Dhan rather than synthesized.
        # Once the minute closes, the same timestamp is naturally replaced by
        # Dhan's final OHLCV values on the next poll.
        for candle in candles:
            stock["candles"][candle["timestamp_iso"]] = candle

        stock["candle_count"] = len(stock["candles"])
        stock["last_dhan_success_at"] = fetched_at
        stock["last_candle_time"] = max(stock["candles"].keys())
        stock["last_error"] = None
        stock["successful_fetches"] += 1
        return len(candles)


def collector_cycle():
    now = now_ist()
    session_date = now.strftime("%Y-%m-%d")
    reset_for_new_day(session_date)

    with _state_lock:
        _state["cycle_count"] += 1
        _state["last_cycle_started_at"] = now.isoformat()

    token = dhan_token()
    if not token:
        error = "Dhan access token environment variable not found"
        with _state_lock:
            _state["failed_cycle_count"] += 1
            _state["collector_error"] = error
            for symbol in STOCKS:
                _state["stocks"][symbol]["last_error"] = error
                _state["stocks"][symbol]["failed_fetches"] += 1
        return

    try:
        master = load_master()
    except Exception as exc:
        error = f"Security master error: {exc}"
        with _state_lock:
            _state["failed_cycle_count"] += 1
            _state["collector_error"] = error
            for symbol in STOCKS:
                _state["stocks"][symbol]["last_error"] = error
                _state["stocks"][symbol]["failed_fetches"] += 1
        return

    fetched_at = now_ist().isoformat()
    successes = 0

    for symbol in STOCKS:
        with _state_lock:
            _state["stocks"][symbol]["security_id"] = master[symbol]

        try:
            candles = fetch_stock(symbol, master[symbol], now_ist())
            if store_dhan_candles(symbol, candles, fetched_at) > 0:
                successes += 1
        except Exception as exc:
            with _state_lock:
                stock = _state["stocks"][symbol]
                stock["last_dhan_fetch_at"] = fetched_at
                stock["last_error"] = str(exc)
                stock["failed_fetches"] += 1

    finished_at = now_ist().isoformat()
    with _state_lock:
        _state["last_cycle_finished_at"] = finished_at
        if successes == len(STOCKS):
            _state["successful_cycle_count"] += 1
            _state["last_successful_cycle_at"] = finished_at
            _state["collector_error"] = None
        else:
            _state["failed_cycle_count"] += 1
            _state["collector_error"] = (
                f"Partial Dhan cycle: {successes}/{len(STOCKS)} stocks succeeded"
            )


def collector_loop():
    with _state_lock:
        _state["collector_alive"] = True
        _state["collector_started_at"] = now_ist().isoformat()

    while True:
        try:
            now = now_ist()
            status = market_status(now)

            if status == "OPEN":
                collector_cycle()
                # Wake every 15 seconds. The endpoint itself remains available
                # every second; this loop is only the Dhan acquisition engine.
                sleep_time.sleep(POLL_SECONDS)
            else:
                # Outside market hours no data is fabricated and no candle is
                # created. Keep the process alive and wait for the next session.
                sleep_time.sleep(5)
        except Exception as exc:
            with _state_lock:
                _state["collector_error"] = f"Collector loop error: {exc}"
                _state["failed_cycle_count"] += 1
            sleep_time.sleep(5)


def start_collector_once():
    # One background collector is intended for the Render service's single
    # Gunicorn worker. A module-level guard prevents duplicate threads if the
    # module is imported repeatedly in the same process.
    if getattr(app, "_collector_thread", None) is not None:
        return
    thread = threading.Thread(
        target=collector_loop,
        name="dhan-live-collector",
        daemon=True,
    )
    app._collector_thread = thread
    thread.start()


def snapshot_state():
    with _state_lock:
        snapshot = {
            "service": "UNPSYCHIC29_LIVE",
            "source": "DHAN",
            "timezone": "Asia/Kolkata",
            "market_window": "09:15-15:30 IST",
            "interval": "1m",
            "poll_seconds": POLL_SECONDS,
            "storage": "MEMORY_ONLY",
            "persistent_storage": False,
            "synthetic_candles": False,
            "synthetic_volume": False,
            "state": {
                "session_date": _state["session_date"],
                "collector_alive": _state["collector_alive"],
                "collector_started_at": _state["collector_started_at"],
                "last_cycle_started_at": _state["last_cycle_started_at"],
                "last_cycle_finished_at": _state["last_cycle_finished_at"],
                "last_successful_cycle_at": _state["last_successful_cycle_at"],
                "cycle_count": _state["cycle_count"],
                "successful_cycle_count": _state["successful_cycle_count"],
                "failed_cycle_count": _state["failed_cycle_count"],
                "collector_error": _state["collector_error"],
            },
            "stocks": {},
        }

        for symbol in STOCKS:
            stock = _state["stocks"][symbol]
            candles = [
                stock["candles"][key]
                for key in sorted(stock["candles"].keys())
            ]
            snapshot["stocks"][symbol] = {
                "security_id": stock["security_id"],
                "decision_candle": DECISION_CANDLES[symbol],
                "candle_count": len(candles),
                "last_candle_time": stock["last_candle_time"],
                "last_dhan_fetch_at": stock["last_dhan_fetch_at"],
                "last_dhan_success_at": stock["last_dhan_success_at"],
                "successful_fetches": stock["successful_fetches"],
                "failed_fetches": stock["failed_fetches"],
                "last_error": stock["last_error"],
                "data": candles,
            }

        return snapshot


@app.get("/")
def root():
    now = now_ist()
    return jsonify(
        {
            "service": "UNPSYCHIC29_LIVE",
            "status": "running",
            "market_status": market_status(now),
            "market_window": "09:15-15:30 IST",
            "storage": "MEMORY_ONLY",
            "synthetic_candles": False,
            "synthetic_volume": False,
            "endpoints": {
                "health": "/health",
                "status": "/status",
                "live_json": "/live.json",
                "live_text": "/live.txt",
            },
            "stocks": STOCKS,
        }
    )


@app.get("/health")
def health():
    # Cheap health endpoint for Render. It never triggers a Dhan fetch.
    with _state_lock:
        payload = {
            "status": "ok",
            "service": "UNPSYCHIC29_LIVE",
            "collector_alive": _state["collector_alive"],
            "session_date": _state["session_date"],
            "market_window": "09:15-15:30 IST",
            "last_successful_cycle_at": _state["last_successful_cycle_at"],
        }
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@app.get("/status")
def status():
    payload = snapshot_state()
    payload["generated_at"] = now_ist().isoformat()
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@app.get("/live.json")
def live_json():
    payload = snapshot_state()
    payload["generated_at"] = now_ist().isoformat()
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@app.get("/live.txt")
def live_txt():
    payload = snapshot_state()
    lines = [
        "SERVICE=UNPSYCHIC29_LIVE",
        "SOURCE=DHAN",
        "TIMEZONE=Asia/Kolkata",
        f"SESSION_DATE={payload['state']['session_date'] or ''}",
        f"MARKET_STATUS={market_status(now_ist())}",
        f"GENERATED_AT={payload['generated_at']}",
        "SESSION=09:15-15:30",
        "INTERVAL=1m",
        "STORAGE=MEMORY_ONLY",
        "PERSISTENT_STORAGE=false",
        "SYNTHETIC_CANDLES=false",
        "SYNTHETIC_VOLUME=false",
        f"COLLECTOR_ALIVE={payload['state']['collector_alive']}",
        f"LAST_SUCCESSFUL_CYCLE={payload['state']['last_successful_cycle_at'] or ''}",
        f"CYCLE_COUNT={payload['state']['cycle_count']}",
        "",
        "FORMAT=STOCK|TIME|OPEN|HIGH|LOW|CLOSE|VOLUME|SOURCE",
    ]

    for symbol in STOCKS:
        stock = payload["stocks"][symbol]
        lines.extend(
            [
                "",
                f"STOCK={symbol}",
                f"SECURITY_ID={stock['security_id'] or ''}",
                f"DECISION_CANDLE={stock['decision_candle']}",
                f"CANDLE_COUNT={stock['candle_count']}",
                f"LAST_CANDLE={stock['last_candle_time'] or ''}",
                f"LAST_DHAN_SUCCESS={stock['last_dhan_success_at'] or ''}",
            ]
        )
        if stock["last_error"]:
            lines.append(f"ERROR={stock['last_error']}")
        for candle in stock["data"]:
            lines.append(
                f"{symbol}|{candle['timestamp']}|{candle['open']}|{candle['high']}|"
                f"{candle['low']}|{candle['close']}|{candle['volume']}|DHAN"
            )

    response = Response("\n".join(lines) + "\n", mimetype="text/plain")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


# Start the acquisition engine as soon as the application process starts.
# Gunicorn should run this service with one worker so there is exactly one
# in-memory collector and exactly one set of Dhan requests.
start_collector_once()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
