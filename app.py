import csv
import io
import os
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, Response

app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")
DHAN_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

STOCKS = ["KEI", "POLYMED", "NATIONALUM", "TRAVELFOOD"]

# These are the V5 decision candles. The acquisition layer does not calculate
# V5 indicators; it only supplies the complete 1-minute OHLCV sequence.
DECISION_CANDLES = {
    "KEI": "09:29",
    "POLYMED": "09:28",
    "NATIONALUM": "09:45",
    "TRAVELFOOD": "09:50",
}

_session = {
    "master": None,
    "master_loaded_at": None,
}


def first_env(*names):
    for name in names:
        value = os.getenv(name)
        if value:
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
    start = datetime.combine(now.date(), time(9, 15), tzinfo=IST)
    end = datetime.combine(now.date(), time(15, 15), tzinfo=IST)
    return start, end


def load_master():
    now = now_ist()
    loaded = _session.get("master")
    loaded_at = _session.get("master_loaded_at")
    if loaded is not None and loaded_at is not None and (now - loaded_at).total_seconds() < 21600:
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

    # Dhan's intraday endpoint returns OHLC, volume and epoch timestamps.
    # We request the current session and then filter strictly to 09:15-15:15 IST.
    from_date = session_start.strftime("%Y-%m-%d %H:%M:%S")
    to_date = (session_end.date() + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

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

    response = requests.post(DHAN_INTRADAY_URL, headers=headers, json=payload, timeout=20)
    if response.status_code >= 400:
        raise RuntimeError(f"Dhan HTTP {response.status_code}: {response.text[:300]}")

    body = response.json()
    timestamps = body.get("timestamp") or []
    opens = body.get("open") or []
    highs = body.get("high") or []
    lows = body.get("low") or []
    closes = body.get("close") or []
    volumes = body.get("volume") or []

    length = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
    candles = []

    for i in range(length):
        dt = parse_epoch(timestamps[i])
        if dt < session_start or dt > effective_end:
            continue
        candle = {
            "timestamp": dt.strftime("%H:%M"),
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
        }
        if i < len(volumes):
            candle["volume"] = volumes[i]
        candles.append(candle)

    candles.sort(key=lambda x: x["timestamp"])
    return candles


def build_payload():
    now = now_ist()
    session_start, session_end = market_window(now)
    token = dhan_token()

    status = "WEEKEND" if now.weekday() >= 5 else "PREMARKET"
    if now.weekday() < 5 and session_start <= now <= session_end:
        status = "OPEN"
    elif now.weekday() < 5 and now > session_end:
        status = "CLOSED"

    result = {
        "service": "UNPSYCHIC29_LIVE",
        "source": "DHAN",
        "timezone": "Asia/Kolkata",
        "session_date": now.strftime("%Y-%m-%d"),
        "market_status": status,
        "generated_at": now.isoformat(),
        "session": {
            "start": "09:15",
            "end": "15:15",
            "interval": "1m",
        },
        "stocks": {},
    }

    if not token:
        result["status"] = "ERROR"
        result["error"] = "Dhan access token environment variable not found"
        return result

    try:
        master = load_master()
    except Exception as exc:
        result["status"] = "ERROR"
        result["error"] = f"Security master error: {exc}"
        return result

    result["status"] = "OK"

    for symbol in STOCKS:
        try:
            candles = fetch_stock(symbol, master[symbol], now)
            result["stocks"][symbol] = {
                "security_id": master[symbol],
                "decision_candle": DECISION_CANDLES[symbol],
                "candle_count": len(candles),
                "data": candles,
            }
        except Exception as exc:
            result["stocks"][symbol] = {
                "security_id": master.get(symbol),
                "decision_candle": DECISION_CANDLES[symbol],
                "candle_count": 0,
                "data": [],
                "error": str(exc),
            }

    return result


@app.get("/")
def root():
    return jsonify({
        "service": "UNPSYCHIC29_LIVE",
        "status": "running",
        "endpoint": "/live.json",
        "stocks": STOCKS,
        "market_window": "09:15-15:15 IST",
    })


@app.get("/health")
def health():
    return Response("OK\n", mimetype="text/plain")


@app.get("/live.json")
def live_json():
    payload = build_payload()
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
