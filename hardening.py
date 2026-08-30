"""UNPSYCHIC29 hardened Dhan 1-minute live acquisition layer.

One Gunicorn worker owns one collector thread. Candles are sourced only from
Dhan's /charts/intraday endpoint. No synthetic candles or volume are created.
Only completed one-minute candles are published.
"""
import base64
import csv
import hashlib
import hmac
import io
import os
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Response, jsonify
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

IST = ZoneInfo("Asia/Kolkata")
DHAN = "https://api.dhan.co/v2"
PROFILE_URL = DHAN + "/profile"
INTRADAY_URL = DHAN + "/charts/intraday"
MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"

STOCKS = ("KEI", "POLYMED", "NATIONALUM", "TRAVELFOOD")
DECISION = {"KEI": "09:29", "POLYMED": "09:28", "NATIONALUM": "09:45", "TRAVELFOOD": "09:50"}
START = dtime(9, 15)
END = dtime(15, 30)
POLL_SECONDS = 15
PREMARKET_SECONDS = 30
TIMEOUT = 12
STALE_SECONDS = 90
MASTER_TTL = 6 * 60 * 60
PROFILE_TTL = 5 * 60
TOKEN_RETRY_SECONDS = 125

_state = None
_state_lock = None
_http_local = threading.local()
_auth_lock = threading.Lock()
_token = None
_token_source = None
_client_id = None
_token_expiry = None
_profile = None
_profile_at = None
_master = None
_master_at = None
_last_token_attempt = 0.0
_last_token_error = None
_stop = threading.Event()
_installed = False


def env(*names):
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return ""


def now():
    return datetime.now(IST)


def market_status(t=None):
    t = t or now()
    if t.weekday() >= 5:
        return "WEEKEND"
    if t.time() < START:
        return "PREMARKET"
    if t.time() <= END:
        return "OPEN"
    return "CLOSED"


def http():
    session = getattr(_http_local, "session", None)
    if session is None:
        retry = Retry(total=3, connect=3, read=3, status=3, backoff_factor=0.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset(("GET", "POST")),
                      respect_retry_after_header=True, raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
        session = requests.Session()
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "UNPSYCHIC29-LIVE-HARDENED/3.0"})
        _http_local.session = session
    return session


def parse_dt(value):
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=IST)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(IST) if parsed.tzinfo else parsed.replace(tzinfo=IST)
    except ValueError:
        return None


def totp(secret):
    clean = secret.replace(" ", "").replace("-", "")
    clean += "=" * ((8 - len(clean) % 8) % 8)
    key = base64.b32decode(clean, casefold=True)
    counter = struct.pack(">Q", int(time.time()) // 30)
    digest = hmac.new(key, counter, hashlib.sha1).digest()
    offset = digest[-1] & 15
    return f"{(struct.unpack('>I', digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000:06d}"


def supplied_token():
    return env("DHAN_ACCESS_TOKEN", "DHAN_ACCESS_TOKEN_JWT", "DHAN_API_ACCESS_TOKEN", "ACCESS_TOKEN")


def get_token(force=False):
    global _token, _token_source, _client_id, _token_expiry
    global _last_token_attempt, _last_token_error
    with _auth_lock:
        supplied = supplied_token()
        cid = env("DHAN_CLIENT_ID", "CLIENT_ID", "DHAN_CLIENTID")
        if supplied and not force:
            _token, _token_source, _client_id = supplied, "ENV", cid or _client_id
            return _token
        if _token and not force:
            return _token
        pin = env("DHAN_PIN", "DHAN_API_PIN")
        secret = env("DHAN_TOTP_SECRET", "TOTP_SECRET", "DHAN_TOTP")
        if not (cid and pin and secret):
            _last_token_error = "Dhan authentication variables incomplete"
            return _token or supplied
        if _last_token_attempt and time.monotonic() - _last_token_attempt < TOKEN_RETRY_SECONDS:
            return _token
        _last_token_attempt = time.monotonic()
        try:
            response = http().post(TOKEN_URL, params={"dhanClientId": cid, "pin": pin, "totp": totp(secret)}, timeout=TIMEOUT)
            if response.status_code >= 400:
                raise RuntimeError(f"Dhan token generation HTTP {response.status_code}: {response.text[:250]}")
            body = response.json()
            generated = body.get("accessToken")
            if not generated:
                raise RuntimeError("Dhan token generation returned no accessToken")
            _token = generated
            _token_source = "TOTP_AUTO"
            _client_id = body.get("dhanClientId") or cid
            _token_expiry = parse_dt(body.get("expiryTime"))
            _last_token_error = None
            return _token
        except Exception as exc:
            _last_token_error = str(exc)
            return _token or supplied


def auth_headers():
    token = get_token()
    if not token:
        raise RuntimeError(_last_token_error or "Dhan access token unavailable")
    headers = {"Accept": "application/json", "Content-Type": "application/json", "access-token": token}
    cid = env("DHAN_CLIENT_ID", "CLIENT_ID", "DHAN_CLIENTID") or _client_id
    if cid:
        headers["client-id"] = cid
    return headers


def profile(force=False):
    global _profile, _profile_at, _token, _token_expiry
    if _profile_at and not force and (now() - _profile_at).total_seconds() < PROFILE_TTL:
        return _profile
    response = http().get(PROFILE_URL, headers=auth_headers(), timeout=TIMEOUT)
    if response.status_code in (401, 403):
        with _auth_lock:
            _token = None
            _profile_at = None
        fresh = get_token(force=True)
        if not fresh:
            raise RuntimeError(_last_token_error or "Dhan re-authentication failed")
        response = http().get(PROFILE_URL, headers=auth_headers(), timeout=TIMEOUT)
    if response.status_code >= 400:
        raise RuntimeError(f"Dhan profile HTTP {response.status_code}: {response.text[:300]}")
    body = response.json()
    if not isinstance(body, dict):
        raise RuntimeError("Dhan profile returned invalid JSON object")
    _profile = body
    _profile_at = now()
    _token_expiry = parse_dt(body.get("tokenValidity")) or _token_expiry
    if body.get("dataPlan") not in (None, "Active"):
        raise RuntimeError(f"Dhan Data API plan is {body.get('dataPlan')!r}")
    return _profile


def security_master(force=False):
    global _master, _master_at
    if _master and _master_at and not force and (now() - _master_at).total_seconds() < MASTER_TTL:
        return _master
    response = http().get(MASTER_URL, timeout=30)
    response.raise_for_status()
    rows = {}
    for row in csv.DictReader(io.StringIO(response.text)):
        if row.get("SEM_EXM_EXCH_ID") != "NSE" or row.get("SEM_INSTRUMENT_NAME") != "EQUITY":
            continue
        symbol = (row.get("SEM_TRADING_SYMBOL") or "").strip().upper()
        security_id = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
        if symbol and security_id:
            rows[symbol] = security_id
    missing = [symbol for symbol in STOCKS if symbol not in rows]
    if missing:
        raise RuntimeError("Dhan security master missing: " + ", ".join(missing))
    _master, _master_at = rows, now()
    return rows


def closed_cutoff(t):
    minute = t.replace(second=0, microsecond=0)
    return minute - timedelta(minutes=1) if t.second == 0 and t.microsecond == 0 else minute


def fetch_stock(symbol, security_id, t):
    session_start = datetime.combine(t.date(), START, tzinfo=IST)
    session_end = datetime.combine(t.date(), END, tzinfo=IST)
    cutoff = min(closed_cutoff(t), session_end - timedelta(minutes=1))
    if cutoff < session_start:
        return [], None
    payload = {
        "securityId": str(security_id), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY",
        "interval": "1", "oi": False,
        "fromDate": session_start.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate": (cutoff + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
    }
    response = http().post(INTRADAY_URL, headers=auth_headers(), json=payload, timeout=TIMEOUT)
    if response.status_code in (401, 403):
        with _auth_lock:
            global _token, _profile_at
            _token = None
            _profile_at = None
        fresh = get_token(force=True)
        if not fresh:
            raise RuntimeError(_last_token_error or "Dhan re-authentication failed")
        response = http().post(INTRADAY_URL, headers=auth_headers(), json=payload, timeout=TIMEOUT)
    if response.status_code >= 400:
        raise RuntimeError(f"Dhan HTTP {response.status_code}: {response.text[:350]}")
    body = response.json()
    arrays = [body.get(k) or [] for k in ("timestamp", "open", "high", "low", "close", "volume")]
    count = min((len(values) for values in arrays), default=0)
    candles = []
    for i in range(count):
        try:
            dt = datetime.fromtimestamp(int(arrays[0][i]), tz=IST)
            o, h, l, c, v = arrays[1][i], arrays[2][i], arrays[3][i], arrays[4][i], arrays[5][i]
            of, hf, lf, cf, vf = map(float, (o, h, l, c, v))
        except (TypeError, ValueError, OverflowError):
            continue
        if dt < session_start or dt > cutoff or dt.second or dt.microsecond:
            continue
        if min(of, hf, lf, cf) <= 0 or hf < max(of, cf) or lf > min(of, cf) or hf < lf or vf < 0:
            continue
        candles.append({"timestamp": dt.strftime("%H:%M"), "timestamp_iso": dt.isoformat(), "open": o,
                        "high": h, "low": l, "close": c, "volume": v, "source": "DHAN", "synthetic": False})
    candles.sort(key=lambda item: item["timestamp_iso"])
    if not candles:
        raise RuntimeError("Dhan returned no valid completed 1-minute candles")
    return candles, hashlib.sha256(response.content).hexdigest()


def reset_day(day):
    with _state_lock:
        if _state["session_date"] == day:
            return
        _state["session_date"] = day
        _state["cycle_count"] = _state["successful_cycle_count"] = _state["failed_cycle_count"] = 0
        _state["last_cycle_started_at"] = _state["last_cycle_finished_at"] = _state["last_successful_cycle_at"] = None
        _state["collector_error"] = None
        for symbol in STOCKS:
            _state["stocks"][symbol].update({"security_id": None, "candles": {}, "last_dhan_fetch_at": None,
                "last_dhan_success_at": None, "last_candle_time": None, "candle_count": 0, "last_error": None,
                "successful_fetches": 0, "failed_fetches": 0, "last_response_sha256": None})


def fetch_one(symbol, master, timestamp):
    sid = master[symbol]
    candles, digest = fetch_stock(symbol, sid, timestamp)
    return symbol, sid, candles, digest


def cycle():
    t = now()
    reset_day(t.strftime("%Y-%m-%d"))
    with _state_lock:
        _state["cycle_count"] += 1
        _state["last_cycle_started_at"] = t.isoformat()
    try:
        profile(False)
        master = security_master(False)
        successes, failures = 0, []
        with ThreadPoolExecutor(max_workers=len(STOCKS), thread_name_prefix="dhan") as executor:
            futures = {executor.submit(fetch_one, symbol, master, now()): symbol for symbol in STOCKS}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    _, sid, candles, digest = future.result()
                    with _state_lock:
                        stock = _state["stocks"][symbol]
                        stock["security_id"] = sid
                        for candle in candles:
                            stock["candles"][candle["timestamp_iso"]] = candle
                        stock["candle_count"] = len(stock["candles"])
                        stock["last_dhan_fetch_at"] = now().isoformat()
                        if candles:
                            stock["last_candle_time"] = max(stock["candles"])
                            stock["last_dhan_success_at"] = stock["last_dhan_fetch_at"]
                            stock["last_response_sha256"] = digest
                        stock["successful_fetches"] += 1
                        stock["last_error"] = None
                    successes += 1
                except Exception as exc:
                    with _state_lock:
                        stock = _state["stocks"][symbol]
                        stock["security_id"] = master.get(symbol)
                        stock["last_dhan_fetch_at"] = now().isoformat()
                        stock["last_error"] = str(exc)
                        stock["failed_fetches"] += 1
                    failures.append(f"{symbol}: {exc}")
        finished = now().isoformat()
        with _state_lock:
            _state["last_cycle_finished_at"] = finished
            if successes == len(STOCKS):
                _state["successful_cycle_count"] += 1
                _state["last_successful_cycle_at"] = finished
                _state["collector_error"] = None
            else:
                _state["failed_cycle_count"] += 1
                _state["collector_error"] = f"Partial Dhan acquisition {successes}/{len(STOCKS)}: " + " | ".join(failures[:4])
    except Exception as exc:
        with _state_lock:
            _state["failed_cycle_count"] += 1
            _state["collector_error"] = f"Dhan preflight/acquisition: {exc}"
            _state["last_cycle_finished_at"] = now().isoformat()


def collector_loop():
    with _state_lock:
        _state["collector_alive"] = True
        _state["collector_started_at"] = now().isoformat()
    next_tick = time.monotonic()
    try:
        while not _stop.is_set():
            status = market_status()
            if status == "OPEN":
                cycle()
                next_tick += POLL_SECONDS
                if next_tick < time.monotonic():
                    next_tick = time.monotonic() + POLL_SECONDS
                _stop.wait(max(0.0, next_tick - time.monotonic()))
            elif status == "PREMARKET":
                try:
                    profile(False)
                    security_master(False)
                    with _state_lock:
                        _state["collector_error"] = None
                except Exception as exc:
                    with _state_lock:
                        _state["collector_error"] = f"PREMARKET_AUTH: {exc}"
                _stop.wait(PREMARKET_SECONDS)
            else:
                _stop.wait(PREMARKET_SECONDS)
                next_tick = time.monotonic()
    finally:
        with _state_lock:
            _state["collector_alive"] = False


def snapshot(include_data=False):
    t = now()
    market = market_status(t)
    with _state_lock:
        result = {"service": "UNPSYCHIC29_LIVE", "version": "3.0-HARDENED", "source": "DHAN",
                  "timezone": "Asia/Kolkata", "market_window": "09:15-15:30 IST", "market_status": market,
                  "interval": "1m", "poll_seconds": POLL_SECONDS, "storage": "MEMORY_ONLY",
                  "persistent_storage": False, "synthetic_candles": False, "synthetic_volume": False,
                  "generated_at": t.isoformat(), "runtime": {k: _state.get(k) for k in (
                      "session_date", "collector_alive", "collector_started_at", "last_cycle_started_at",
                      "last_cycle_finished_at", "last_successful_cycle_at", "cycle_count",
                      "successful_cycle_count", "failed_cycle_count", "collector_error")},
                  "dhan_auth": {"token_configured": bool(_token or supplied_token()), "token_source": _token_source,
                      "dhan_client_id": _client_id, "token_expiry": _token_expiry.isoformat() if _token_expiry else None,
                      "token_generation_last_error": _last_token_error, "last_profile_check_at": _profile_at.isoformat() if _profile_at else None,
                      "data_plan": (_profile or {}).get("dataPlan"), "data_validity": (_profile or {}).get("dataValidity")}, "stocks": {}}
        for symbol in STOCKS:
            stock = _state["stocks"][symbol]
            keys = sorted(stock["candles"])
            latest = keys[-1] if keys else None
            age = (t - datetime.fromisoformat(latest)).total_seconds() if latest else None
            if market == "OPEN":
                expected = max(0, int((t - datetime.combine(t.date(), START, tzinfo=IST)).total_seconds() // 60))
                missing = max(0, expected - len(keys))
                if stock["last_error"]:
                    status = "ERROR"
                elif not latest:
                    status = "NO_DATA"
                elif age > STALE_SECONDS:
                    status = "STALE_DHAN_DATA"
                elif missing > 0:
                    status = "CANDLE_GAP"
                else:
                    status = "OK"
            elif market == "PREMARKET":
                status = "READY" if _profile and not _last_token_error else "AUTH_NOT_READY"
                expected = missing = 0
            else:
                status = "READY" if _profile and not _last_token_error else "NO_DATA"
                expected = missing = 0
            item = {"status": status, "security_id": stock["security_id"], "decision_candle": DECISION[symbol],
                    "candle_count": len(keys), "expected_closed_candles": expected, "missing_closed_candles": missing,
                    "last_candle_time": stock["last_candle_time"], "last_dhan_fetch_at": stock["last_dhan_fetch_at"],
                    "last_dhan_success_at": stock["last_dhan_success_at"], "candle_age_seconds": round(age, 1) if age is not None else None,
                    "successful_fetches": stock["successful_fetches"], "failed_fetches": stock["failed_fetches"],
                    "last_error": stock["last_error"], "last_response_sha256": stock.get("last_response_sha256")}
            if include_data:
                item["data"] = [stock["candles"][key] for key in keys]
            result["stocks"][symbol] = item
    if market == "OPEN":
        result["overall_status"] = "OK" if result["runtime"]["collector_alive"] and all(v["status"] == "OK" for v in result["stocks"].values()) else "DEGRADED"
    else:
        result["overall_status"] = "READY" if result["dhan_auth"]["data_plan"] == "Active" and result["runtime"]["collector_alive"] and not _last_token_error else "DEGRADED"
    return result


def health():
    snap = snapshot(False)
    payload = {"status": "ok" if snap["runtime"]["collector_alive"] else "degraded", "service": "UNPSYCHIC29_LIVE",
               "process_alive": True, "collector_alive": snap["runtime"]["collector_alive"], "market_status": snap["market_status"],
               "last_successful_cycle_at": snap["runtime"]["last_successful_cycle_at"], "collector_error": snap["runtime"]["collector_error"]}
    return jsonify(payload), (200 if payload["collector_alive"] else 503)


def status():
    snap = snapshot(False)
    return jsonify(snap), (200 if snap["overall_status"] in ("OK", "READY") else 503)


def live_json():
    snap = snapshot(True)
    return jsonify(snap), (200 if snap["overall_status"] in ("OK", "READY") else 503)


def live_txt():
    snap = snapshot(True)
    lines = ["SERVICE=UNPSYCHIC29_LIVE", f"VERSION={snap['version']}", "SOURCE=DHAN", "TIMEZONE=Asia/Kolkata",
             f"SESSION_DATE={snap['runtime']['session_date'] or ''}", f"MARKET_STATUS={snap['market_status']}",
             f"GENERATED_AT={snap['generated_at']}", "SESSION=09:15-15:30", "INTERVAL=1m", "STORAGE=MEMORY_ONLY",
             "PERSISTENT_STORAGE=false", "SYNTHETIC_CANDLES=false", "SYNTHETIC_VOLUME=false",
             f"COLLECTOR_ALIVE={snap['runtime']['collector_alive']}", f"OVERALL_STATUS={snap['overall_status']}",
             f"LAST_SUCCESSFUL_CYCLE={snap['runtime']['last_successful_cycle_at'] or ''}", f"CYCLE_COUNT={snap['runtime']['cycle_count']}", "",
             "FORMAT=STOCK|TIME|OPEN|HIGH|LOW|CLOSE|VOLUME|SOURCE"]
    for symbol in STOCKS:
        stock = snap["stocks"][symbol]
        lines += ["", f"STOCK={symbol}", f"SECURITY_ID={stock['security_id'] or ''}", f"DECISION_CANDLE={stock['decision_candle']}",
                   f"STATUS={stock['status']}", f"CANDLE_COUNT={stock['candle_count']}", f"EXPECTED_CLOSED_CANDLES={stock['expected_closed_candles']}",
                   f"MISSING_CLOSED_CANDLES={stock['missing_closed_candles']}", f"LAST_CANDLE={stock['last_candle_time'] or ''}",
                   f"LAST_DHAN_SUCCESS={stock['last_dhan_success_at'] or ''}"]
        if stock["last_error"]:
            lines.append(f"ERROR={stock['last_error']}")
        for candle in stock["data"]:
            lines.append(f"{symbol}|{candle['timestamp']}|{candle['open']}|{candle['high']}|{candle['low']}|{candle['close']}|{candle['volume']}|DHAN")
    return Response("\n".join(lines) + "\n", mimetype="text/plain")


def dhan_check_hard():
    result = {"service": "UNPSYCHIC29_LIVE", "source": "DHAN", "checked_at": now().isoformat(),
              "market_status": market_status(), "overall": "FAILED", "auth": {}, "security_master": {}, "stocks": {}}
    try:
        p = profile(True)
        m = security_master(True)
        result["auth"] = {"ok": True, "token_configured": bool(_token or supplied_token()), "token_source": _token_source,
                           "data_plan": p.get("dataPlan"), "data_validity": p.get("dataValidity"), "token_validity": p.get("tokenValidity")}
        result["security_master"] = {symbol: m[symbol] for symbol in STOCKS}
        if result["market_status"] != "OPEN":
            result["overall"] = "READY" if p.get("dataPlan") == "Active" else "FAILED"
            return jsonify(result), (200 if result["overall"] == "READY" else 503)
        successes = 0
        with ThreadPoolExecutor(max_workers=len(STOCKS), thread_name_prefix="probe") as executor:
            futures = {executor.submit(fetch_one, symbol, m, now()): symbol for symbol in STOCKS}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    _, sid, candles, digest = future.result()
                    latest = candles[-1] if candles else None
                    result["stocks"][symbol] = {"ok": bool(latest), "security_id": sid, "candle_count_returned": len(candles),
                                                 "latest_closed_candle": latest, "response_sha256": digest, "synthetic": False}
                    if latest:
                        successes += 1
                except Exception as exc:
                    result["stocks"][symbol] = {"ok": False, "error": str(exc)}
        if successes == 0 and (now() - datetime.combine(now().date(), START, tzinfo=IST)).total_seconds() < 60:
            result["overall"] = "READY"
        else:
            result["overall"] = "PASS" if successes == len(STOCKS) else "FAILED"
        return jsonify(result), (200 if result["overall"] in ("PASS", "READY") else 503)
    except Exception as exc:
        result["error"] = str(exc)
        return jsonify(result), 503


def install(application):
    global _state, _state_lock, _installed
    if _installed:
        return
    flask_app = application.app if hasattr(application, "app") else application
    if not hasattr(flask_app, "view_functions"):
        raise RuntimeError("Gunicorn integration received no Flask app object")
    _state = flask_app._state
    _state_lock = flask_app._state_lock
    _stop.clear()
    flask_app.view_functions["health"] = health
    flask_app.view_functions["status"] = status
    flask_app.view_functions["live_json"] = live_json
    flask_app.view_functions["live_txt"] = live_txt
    if "dhan_check_hard" in flask_app.view_functions:
        flask_app.view_functions["dhan_check_hard"] = dhan_check_hard
    else:
        flask_app.add_url_rule("/dhan-check-hard", "dhan_check_hard", dhan_check_hard, methods=["GET"])
    threading.Thread(target=collector_loop, name="dhan-collector", daemon=True).start()
    _installed = True
