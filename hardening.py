"""UNPSYCHIC29 live-market hardening layer.
Loaded by Gunicorn post_worker_init so the existing app can be hardened
without changing the public endpoint contract.
"""
import base64
import csv
import hashlib
import hmac
import io
import struct
import threading
import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
from flask import Response, jsonify
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

IST = ZoneInfo("Asia/Kolkata")
DHAN = "https://api.dhan.co/v2"
PROFILE = DHAN + "/profile"
INTRADAY = DHAN + "/charts/intraday"
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"
GENERATE = "https://auth.dhan.co/app/generateAccessToken"
POLL = 15
TIMEOUT = 12
STALE = 150
STOCKS = ("KEI", "POLYMED", "NATIONALUM", "TRAVELFOOD")
START = dtime(9, 15)
END = dtime(15, 30)
_lock = threading.RLock()
_stop = threading.Event()
_installed = False
_http = None
_token = None
_token_source = None
_client_id = None
_expiry = None
_master = None
_master_at = None
_profile_at = None
_profile = None


def env(*names):
    import os
    for n in names:
        v = os.getenv(n)
        if v and v.strip():
            return v.strip()
    return ""


def now():
    return datetime.now(IST)


def http():
    global _http
    if _http is None:
        r = Retry(total=3, connect=3, read=3, status=3, backoff_factor=.6,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(("GET", "POST")),
                  respect_retry_after_header=True, raise_on_status=False)
        a = HTTPAdapter(max_retries=r, pool_connections=8, pool_maxsize=8)
        _http = requests.Session()
        _http.mount("https://", a)
        _http.mount("http://", a)
        _http.headers.update({"User-Agent": "UNPSYCHIC29-LIVE-HARDENED/1.0"})
    return _http


def parse_dt(v):
    if not v:
        return None
    s = str(v).strip()
    for f in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, f).replace(tzinfo=IST)
        except ValueError:
            pass
    try:
        x = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return x.astimezone(IST) if x.tzinfo else x.replace(tzinfo=IST)
    except ValueError:
        return None


def totp(secret):
    s = secret.replace(" ", "").replace("-", "")
    s += "=" * ((8-len(s)%8)%8)
    key = base64.b32decode(s, casefold=True)
    msg = struct.pack(">Q", int(time.time())//30)
    d = hmac.new(key, msg, hashlib.sha1).digest()
    o = d[-1] & 15
    return f"{(struct.unpack('>I', d[o:o+4])[0] & 0x7fffffff)%1000000:06d}"


def get_token(force=False):
    global _token, _token_source, _client_id, _expiry
    if _token and not force:
        return _token
    t = env("DHAN_ACCESS_TOKEN", "DHAN_ACCESS_TOKEN_JWT", "DHAN_API_ACCESS_TOKEN", "ACCESS_TOKEN")
    if t and not force:
        _token, _token_source = t, "ENV"
        _client_id = env("DHAN_CLIENT_ID", "CLIENT_ID", "DHAN_CLIENTID") or _client_id
        return _token
    cid = env("DHAN_CLIENT_ID", "CLIENT_ID", "DHAN_CLIENTID")
    pin = env("DHAN_PIN", "DHAN_API_PIN")
    secret = env("DHAN_TOTP_SECRET", "TOTP_SECRET", "DHAN_TOTP")
    if not (cid and pin and secret):
        return _token or t
    r = http().post(GENERATE, params={"dhanClientId": cid, "pin": pin, "totp": totp(secret)}, timeout=TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Dhan token generation HTTP {r.status_code}: {r.text[:250]}")
    b = r.json(); token = b.get("accessToken")
    if not token:
        raise RuntimeError("Dhan token generation returned no accessToken")
    _token, _token_source, _client_id = token, "TOTP_AUTO", b.get("dhanClientId") or cid
    _expiry = parse_dt(b.get("expiryTime"))
    return _token


def headers():
    t = get_token()
    if not t:
        raise RuntimeError("Dhan access token missing from Render runtime environment")
    h = {"Accept":"application/json", "Content-Type":"application/json", "access-token":t}
    cid = env("DHAN_CLIENT_ID", "CLIENT_ID", "DHAN_CLIENTID") or _client_id
    if cid: h["client-id"] = cid
    return h


def profile(force=False):
    global _profile_at, _profile, _expiry
    if _profile_at and not force and (now()-_profile_at).total_seconds()<300:
        return True, _profile
    r = http().get(PROFILE, headers=headers(), timeout=TIMEOUT)
    if r.status_code in (401,403):
        if env("DHAN_CLIENT_ID", "CLIENT_ID") and env("DHAN_TOTP_SECRET", "TOTP_SECRET", "DHAN_TOTP"):
            get_token(True); r = http().get(PROFILE, headers=headers(), timeout=TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Dhan profile HTTP {r.status_code}: {r.text[:300]}")
    _profile = r.json(); _profile_at = now(); _expiry = parse_dt(_profile.get("tokenValidity"))
    return True, _profile


def master(force=False):
    global _master, _master_at
    if _master and _master_at and (now()-_master_at).total_seconds()<21600 and not force:
        return _master
    r = http().get(MASTER, timeout=30); r.raise_for_status()
    rows = {}
    for row in csv.DictReader(io.StringIO(r.text)):
        if row.get("SEM_EXM_EXCH_ID") == "NSE" and row.get("SEM_INSTRUMENT_NAME") == "EQUITY":
            s=(row.get("SEM_TRADING_SYMBOL") or "").strip().upper(); sid=(row.get("SEM_SMST_SECURITY_ID") or "").strip()
            if s and sid: rows[s]=sid
    miss=[s for s in STOCKS if s not in rows]
    if miss: raise RuntimeError("Dhan security master missing: "+", ".join(miss))
    _master, _master_at = rows, now(); return rows


def fetch(symbol, sid, t):
    start=datetime.combine(t.date(), START, tzinfo=IST); end=datetime.combine(t.date(), END, tzinfo=IST)
    effective=min(t,end)
    payload={"securityId":str(sid),"exchangeSegment":"NSE_EQ","instrument":"EQUITY","interval":"1","oi":False,
             "fromDate":start.strftime("%Y-%m-%d %H:%M:%S"),"toDate":effective.strftime("%Y-%m-%d %H:%M:%S")}
    r=http().post(INTRADAY, headers=headers(), json=payload, timeout=TIMEOUT)
    if r.status_code in (401,403) and env("DHAN_CLIENT_ID","CLIENT_ID") and env("DHAN_TOTP_SECRET","TOTP_SECRET","DHAN_TOTP"):
        get_token(True); r=http().post(INTRADAY,headers=headers(),json=payload,timeout=TIMEOUT)
    if r.status_code>=400: raise RuntimeError(f"Dhan HTTP {r.status_code}: {r.text[:350]}")
    b=r.json(); keys=("timestamp","open","high","low","close","volume"); a=[b.get(k) or [] for k in keys]
    n=min(len(x) for x in a) if a else 0; out=[]
    for i in range(n):
        dt=datetime.fromtimestamp(int(a[0][i]),tz=IST)
        if dt<start or dt>effective or dt.second or dt.microsecond: continue
        try:
            o,h,l,c,v=map(float,(a[1][i],a[2][i],a[3][i],a[4][i],a[5][i]))
        except (TypeError,ValueError): continue
        if min(o,h,l,c)<=0 or h<max(o,c) or l>min(o,c) or h<l or v<0: continue
        out.append({"timestamp":dt.strftime("%H:%M"),"timestamp_iso":dt.isoformat(),"open":a[1][i],"high":a[2][i],"low":a[3][i],"close":a[4][i],"volume":a[5][i],"source":"DHAN","synthetic":False})
    if not out: raise RuntimeError("Dhan returned no valid 1-minute candles")
    return out, hashlib.sha256(r.content).hexdigest()


def cycle(app):
    s=app._state; t=now(); day=t.strftime("%Y-%m-%d")
    with app._state_lock:
        if s.get("session_date")!=day:
            s["session_date"]=day
            s["cycle_count"]=s["successful_cycle_count"]=s["failed_cycle_count"]=0
            for x in STOCKS:
                s["stocks"][x]["candles"]={}; s["stocks"][x]["candle_count"]=0
                s["stocks"][x]["successful_fetches"]=s["stocks"][x]["failed_fetches"]=0
                s["stocks"][x]["last_error"]=None
        s["cycle_count"]+=1; s["last_cycle_started_at"]=t.isoformat()
    try:
        profile(True); m=master(); success=0
        for sym in STOCKS:
            try:
                sid=m[sym]; cs,sha=fetch(sym,sid,now())
                with app._state_lock:
                    st=s["stocks"][sym]; st["security_id"]=sid
                    for c in cs: st["candles"][c["timestamp_iso"]]=c
                    st["candle_count"]=len(st["candles"]); st["last_candle_time"]=max(st["candles"])
                    st["last_dhan_fetch_at"]=st["last_dhan_success_at"]=now().isoformat(); st["successful_fetches"]+=1; st["last_error"]=None; st["last_response_sha256"]=sha
                success+=1
            except Exception as e:
                with app._state_lock:
                    st=s["stocks"][sym]; st["security_id"]=m.get(sym); st["last_dhan_fetch_at"]=now().isoformat(); st["last_error"]=str(e); st["failed_fetches"]+=1
        with app._state_lock:
            s["last_cycle_finished_at"]=now().isoformat()
            if success==len(STOCKS): s["successful_cycle_count"]+=1; s["last_successful_cycle_at"]=s["last_cycle_finished_at"]; s["collector_error"]=None
            else: s["failed_cycle_count"]+=1; s["collector_error"]=f"Partial Dhan acquisition: {success}/{len(STOCKS)}"
        return success==len(STOCKS)
    except Exception as e:
        with app._state_lock: s["failed_cycle_count"]+=1; s["collector_error"]=f"Dhan preflight/acquisition: {e}"; s["last_cycle_finished_at"]=now().isoformat()
        return False


def snapshot(app, data=False):
    t=now(); s=app._state
    with app._state_lock:
        out={"service":"UNPSYCHIC29_LIVE","version":"2.0-HARDENED","source":"DHAN","timezone":"Asia/Kolkata","market_window":"09:15-15:30 IST","market_status":"WEEKEND" if t.weekday()>=5 else ("PREMARKET" if t.time()<START else ("OPEN" if t.time()<=END else "CLOSED")),"interval":"1m","poll_seconds":POLL,"storage":"MEMORY_ONLY","persistent_storage":False,"synthetic_candles":False,"synthetic_volume":False,"runtime":{k:s.get(k) for k in ("session_date","collector_alive","collector_started_at","last_cycle_started_at","last_cycle_finished_at","last_successful_cycle_at","cycle_count","successful_cycle_count","failed_cycle_count","collector_error")},"dhan_auth":{"token_configured":bool(get_token()),"token_source":_token_source,"dhan_client_id":_client_id,"token_expiry":_expiry.isoformat() if _expiry else None,"last_profile_check_at":_profile_at.isoformat() if _profile_at else None,"profile_error":s.get("profile_error"),"data_plan":(_profile or {}).get("dataPlan"),"data_validity":(_profile or {}).get("dataValidity")},"stocks":{}}
        for sym in STOCKS:
            st=s["stocks"][sym]; keys=sorted(st["candles"]); latest=keys[-1] if keys else None; age=(t-datetime.fromisoformat(latest)).total_seconds() if latest else None
            expected=max(0,int((t-datetime.combine(t.date(),START,tzinfo=IST)).total_seconds()//60)) if out["market_status"]=="OPEN" else 0
            missing=max(0,expected-len(keys))
            status="OK" if out["market_status"]!="OPEN" and latest else ("ERROR" if st["last_error"] else ("NO_DATA" if not latest else ("STALE_DHAN_DATA" if age>STALE else ("CANDLE_GAP" if missing>1 else "OK"))))
            item={"status":status,"security_id":st["security_id"],"candle_count":len(keys),"expected_closed_candles":expected,"missing_closed_candles":missing,"last_candle_time":st["last_candle_time"],"last_dhan_fetch_at":st["last_dhan_fetch_at"],"last_dhan_success_at":st["last_dhan_success_at"],"candle_age_seconds":round(age,1) if age is not None else None,"successful_fetches":st["successful_fetches"],"failed_fetches":st["failed_fetches"],"last_error":st["last_error"],"last_response_sha256":st.get("last_response_sha256")}
            if data: item["data"]=[st["candles"][k] for k in keys]
            out["stocks"][sym]=item
    out["overall_status"]="OK" if out["runtime"]["collector_alive"] and not (out["market_status"]=="OPEN" and any(x["status"]!="OK" for x in out["stocks"].values())) else "DEGRADED"
    return out


def health(app):
    with app._state_lock:
        return jsonify({"status":"ok","service":"UNPSYCHIC29_LIVE","process_alive":True,"collector_alive":bool(app._state.get("collector_alive")),"market_status":snapshot(app)["market_status"],"last_successful_cycle_at":app._state.get("last_successful_cycle_at"),"collector_error":app._state.get("collector_error")}),200


def status(app):
    x=snapshot(app,False); return jsonify(x), (200 if x["overall_status"]=="OK" else 503)


def probe(app):
    r={"service":"UNPSYCHIC29_LIVE","source":"DHAN","checked_at":now().isoformat()}
    try:
        _,p=profile(True); m=master(True); r["auth"]={"ok":True,"token_configured":bool(get_token()),"data_plan":p.get("dataPlan"),"data_validity":p.get("dataValidity"),"token_validity":p.get("tokenValidity")}; r["security_master"]={x:m[x] for x in STOCKS}; r["stocks"]={}
        for x in STOCKS:
            cs,sha=fetch(x,m[x],now()); r["stocks"][x]={"ok":True,"source":"DHAN","synthetic":False,"candle_count":len(cs),"latest_candle":cs[-1],"response_sha256":sha}
        r["overall"]="OK"; return jsonify(r),200
    except Exception as e:
        r["overall"]="FAILED"; r["error"]=str(e); return jsonify(r),503


def text(app):
    x=snapshot(app,True); L=["UNPSYCHIC29_LIVE","VERSION=2.0-HARDENED","SOURCE=DHAN","INTERVAL=1m","STORAGE=MEMORY_ONLY","SYNTHETIC_CANDLES=FALSE","SYNTHETIC_VOLUME=FALSE",f"MARKET_STATUS={x['market_status']}",f"COLLECTOR_ALIVE={x['runtime']['collector_alive']}",f"LAST_SUCCESSFUL_CYCLE={x['runtime']['last_successful_cycle_at']}",f"COLLECTOR_ERROR={x['runtime']['collector_error']}",""]
    for sym in STOCKS:
        z=x["stocks"][sym]; L += [f"[{sym}]",f"SECURITY_ID={z['security_id']}",f"STATUS={z['status']}",f"CANDLE_COUNT={z['candle_count']}",f"EXPECTED_CLOSED_CANDLES={z['expected_closed_candles']}",f"MISSING_CLOSED_CANDLES={z['missing_closed_candles']}",f"LAST_CANDLE={z['last_candle_time']}",f"LAST_DHAN_SUCCESS={z['last_dhan_success_at']}",f"SUCCESSFUL_FETCHES={z['successful_fetches']}",f"FAILED_FETCHES={z['failed_fetches']}",f"LAST_ERROR={z['last_error']}" ]
        for c in z["data"]: L.append(f"{c['timestamp']} | O={c['open']} H={c['high']} L={c['low']} C={c['close']} V={c['volume']} | SOURCE=DHAN | SYNTHETIC=FALSE")
        L.append("")
    return Response("\n".join(L)+"\n",mimetype="text/plain")


def worker(app):
    with app._state_lock: app._state["collector_alive"]=True; app._state["collector_started_at"]=now().isoformat()
    while not _stop.is_set():
        try:
            t=now(); open_now=t.weekday()<5 and START<=t.time()<=END
            if open_now: cycle(app); _stop.wait(POLL)
            elif t.weekday()<5 and t.time()<START:
                try: profile(True); master()
                except Exception as e:
                    with app._state_lock: app._state["collector_error"]=f"Premarket check: {e}"
                _stop.wait(15)
            else: _stop.wait(30)
        except Exception as e:
            with app._state_lock: app._state["collector_error"]=f"Collector supervisor: {e}"
            _stop.wait(5)
    with app._state_lock: app._state["collector_alive"]=False


def install(appmod):
    global _installed
    if _installed: return
    _installed=True
    appmod.collector_cycle=lambda: cycle(appmod)
    appmod.snapshot_state=lambda include_data=True: snapshot(appmod,include_data)
    appmod.app.view_functions["health"]=lambda: health(appmod)
    appmod.app.view_functions["status"]=lambda: status(appmod)
    appmod.app.add_url_rule("/dhan-check-hard", "dhan_check_hard", lambda: probe(appmod))
    appmod.app.add_url_rule("/live-hard.txt", "live_hard_txt", lambda: text(appmod))
    th=getattr(appmod.app,"_collector_thread",None)
    if not th or not th.is_alive():
        th=threading.Thread(target=worker,args=(appmod,),name="dhan-live-hardened",daemon=False); appmod.app._collector_thread=th; th.start()
