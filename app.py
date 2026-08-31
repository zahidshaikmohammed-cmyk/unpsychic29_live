import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify

app = Flask(__name__)
IST = ZoneInfo("Asia/Kolkata")

UPSTREAM_LIVE_URL = os.getenv(
    "UPSTREAM_LIVE_URL",
    "https://psy29-live-data-hardening.onrender.com/data",
).strip()
HTTP_TIMEOUT_SECONDS = int(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "20"))


def now_ist():
    return datetime.now(IST)


def upstream_url():
    separator = "&" if "?" in UPSTREAM_LIVE_URL else "?"
    return f"{UPSTREAM_LIVE_URL}{separator}_cb={int(now_ist().timestamp() * 1000)}"


def fetch_upstream():
    response = requests.get(
        upstream_url(),
        headers={
            "Accept": "application/json",
            "Cache-Control": "no-cache, no-store",
            "Pragma": "no-cache",
        },
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response


def proxy_json():
    try:
        response = fetch_upstream()
        return response.json(), 200, None
    except requests.RequestException as exc:
        return None, 503, f"UPSTREAM_FETCH_ERROR: {exc}"
    except ValueError as exc:
        return None, 502, f"UPSTREAM_JSON_ERROR: {exc}"


@app.after_request
def no_cache(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/")
def root():
    return jsonify({
        "service": "UNPSYCHIC29_LIVE",
        "status": "running",
        "source": "PSY29_LIVE_DATA_HARDENING",
        "upstream": UPSTREAM_LIVE_URL,
        "timezone": "Asia/Kolkata",
        "endpoints": {
            "health": "/health",
            "status": "/status",
            "live_json": "/live.json",
            "live_text": "/live.txt",
            "data": "/data",
        },
    })


@app.get("/health")
def health():
    try:
        response = fetch_upstream()
        return jsonify({
            "status": "ok",
            "service": "UNPSYCHIC29_LIVE",
            "upstream_status": response.status_code,
            "upstream": UPSTREAM_LIVE_URL,
            "checked_at": now_ist().isoformat(),
        })
    except Exception as exc:
        return jsonify({
            "status": "degraded",
            "service": "UNPSYCHIC29_LIVE",
            "upstream": UPSTREAM_LIVE_URL,
            "error": str(exc),
            "checked_at": now_ist().isoformat(),
        }), 503


@app.get("/data")
def data():
    payload, status_code, error = proxy_json()
    if error:
        return jsonify({
            "service": "UNPSYCHIC29_LIVE",
            "source": "PSY29_LIVE_DATA_HARDENING",
            "generated_at": now_ist().isoformat(),
            "overall_status": "DEGRADED",
            "error": error,
        }), status_code
    return jsonify(payload), status_code


@app.get("/live.json")
def live_json():
    return data()


@app.get("/status")
def status():
    return data()


@app.get("/live.txt")
def live_txt():
    payload, status_code, error = proxy_json()
    if error:
        return Response(
            "SERVICE=UNPSYCHIC29_LIVE\n"
            "OVERALL_STATUS=DEGRADED\n"
            f"ERROR={error}\n",
            status=status_code,
            mimetype="text/plain",
        )
    import json
    return Response(json.dumps(payload, separators=(",", ":")), mimetype="text/plain")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
