"""UNPSYCHIC29 hardened live Dhan acquisition worker.
Single collector, real Dhan 1-minute OHLCV only, no synthetic data.
Authentication is cached and TOTP generation is rate-limited.
"""
import base64,csv,hashlib,hmac,io,os,struct,threading,time
from datetime import datetime,time as dtime
from zoneinfo import ZoneInfo
import requests
from flask import Response,jsonify
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

IST=ZoneInfo("Asia/Kolkata")
DHAN="https://api.dhan.co/v2"; PROFILE=DHAN+"/profile"; INTRADAY=DHAN+"/charts/intraday"
MASTER="https://images.dhan.co/api-data/api-scrip-master.csv"; GENERATE="https://auth.dhan.co/app/generateAccessToken"
STOCKS=("KEI","POLYMED","NATIONALUM","TRAVELFOOD")
DECISION={"KEI":"09:29","POLYMED":"09:28","NATIONALUM":"09:45","TRAVELFOOD":"09:50"}
START=dtime(9,15); END=dtime(15,30); POLL=15; TIMEOUT=12; STALE=150; TOKEN_RETRY_SECONDS=125
_http=None; _token=None; _token_source=None; _client_id=None; _expiry=None; _last_token_attempt=0.0; _last_token_error=None
_profile=None; _profile_at=None; _master=None; _master_at=None; _install_lock=threading.Lock(); _installed=False; _stop=threading.Event()

def env(*names):
    for n in names:
        v=os.getenv(n)
        if v and v.strip(): return v.strip()
    return ""

def now(): return datetime.now(IST)

def market_status(t=None):
    t=t or now()
    if t.weekday()>=5: return "WEEKEND"
    if t.time()<START: return "PREMARKET"
    if t.time()<=END: return "OPEN"
    return "CLOSED"

def http():
    global _http
    if _http is None:
        retry=Retry(total=3,connect=3,read=3,status=3,backoff_factor=.6,status_forcelist=(429,500,502,503,504),allowed_methods=frozenset(("GET","POST")),respect_retry_after_header=True,raise_on_status=False)
        adapter=HTTPAdapter(max_retries=retry,pool_connections=8,pool_maxsize=8)
        _http=requests.Session(); _http.mount("https://",adapter); _http.mount("http://",adapter); _http.headers.update({"User-Agent":"UNPSYCHIC29-LIVE-HARDENED/2.1"})
    return _http

def parse_dt(v):
    if not v:return None
    s=str(v).strip()
    for f in ("%d/%m/%Y %H:%M","%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S"):
        try:return datetime.strptime(s,f).replace(tzinfo=IST)
        except ValueError:pass
    try:
        x=datetime.fromisoformat(s.replace("Z","+00:00")); return x.astimezone(IST) if x.tzinfo else x.replace(tzinfo=IST)
    except ValueError:return None

def totp(secret):
    s=secret.replace(" ","").replace("-",""); s+="="*((8-len(s)%8)%8); key=base64.b32decode(s,casefold=True)
    msg=struct.pack(">Q",int(time.time())//30); d=hmac.new(key,msg,hashlib.sha1).digest(); o=d[-1]&15
    return f"{(struct.unpack('>I',d[o:o+4])[0]&0x7fffffff)%1000000:06d}"

def token(force=False):
    global _token,_token_source,_client_id,_expiry,_last_token_attempt,_last_token_error
    with _install_lock:
        if _token and not force:return _token
        supplied=env("DHAN_ACCESS_TOKEN","DHAN_ACCESS_TOKEN_JWT","DHAN_API_ACCESS_TOKEN","ACCESS_TOKEN")
        cid=env("DHAN_CLIENT_ID","CLIENT_ID","DHAN_CLIENTID")
        if supplied and not force:
            _token,_token_source,_client_id=supplied,"ENV",cid or _client_id; return _token
        pin=env("DHAN_PIN","DHAN_API_PIN"); secret=env("DHAN_TOTP_SECRET","TOTP_SECRET","DHAN_TOTP")
        if not(cid and pin and secret):_last_token_error="Dhan authentication variables incomplete"; return _token or supplied
        if _last_token_attempt and time.monotonic()-_last_token_attempt<TOKEN_RETRY_SECONDS:return _token
        _last_token_attempt=time.monotonic()
        try:
            r=http().post(GENERATE,params={"dhanClientId":cid,"pin":pin,"totp":totp(secret)},timeout=TIMEOUT)
            if r.status_code>=400:raise RuntimeError(f"Dhan token generation HTTP {r.status_code}: {r.text[:250]}")
            b=r.json(); generated=b.get("accessToken")
            if not generated:raise RuntimeError("Dhan token generation returned no accessToken")
            _token=generated; _token_source="TOTP_AUTO"; _client_id=b.get("dhanClientId") or cid; _expiry=parse_dt(b.get("expiryTime")); _last_token_error=None
            return _token
        except Exception as exc:_last_token_error=str(exc); return _token or supplied

def headers():
    t=token()
    if not t:raise RuntimeError(_last_token_error or "Dhan access token unavailable")
    h={"Accept":"application/json","Content-Type":"application/json","access-token":t}; cid=env("DHAN_CLIENT_ID","CLIENT_ID","DHAN_CLIENTID") or _client_id
    if cid:h["client-id"]=cid
    return h

def profile(force=False):
    global _profile,_profile_at,_expiry,_token
    if _profile_at and not force and (now()-_profile_at).total_seconds()<300:return _profile
    r=http().get(PROFILE,headers=headers(),timeout=TIMEOUT)
    if r.status_code in (401,403):
        _token=None; fresh=token(force=True)
        if not fresh:raise RuntimeError(_last_token_error or "Dhan re-authentication failed")
        r=http().get(PROFILE,headers=headers(),timeout=TIMEOUT)
    if r.status_code>=400:raise RuntimeError(f"Dhan profile HTTP {r.status_code}: {r.text[:300]}")
    _profile=r.json(); _profile_at=now(); _expiry=parse_dt(_profile.get("tokenValidity")) or _expiry; return _profile

def master(force=False):
    global _master,_master_at
    if _master and _master_at and not force and (now()-_master_at).total_seconds()<21600:return _master
    r=http().get(MASTER,timeout=30); r.raise_for_status(); rows={}
    for row in csv.DictReader(io.StringIO(r.text)):
        if row.get("SEM_EXM_EXCH_ID")!="NSE" or row.get("SEM_INSTRUMENT_NAME")!="EQUITY":continue
        s=(row.get("SEM_TRADING_SYMBOL") or "").strip().upper(); sid=(row.get("SEM_SMST_SECURITY_ID") or "").strip()
        if s and sid:rows[s]=sid
    missing=[s for s in STOCKS if s not in rows]
    if missing:raise RuntimeError("Dhan security master missing: "+", ".join(missing))
    _master,_master_at=rows,now(); return rows

def fetch(symbol,sid,t):
    global _token
    start=datetime.combine(t.date(),START,tzinfo=IST); end=datetime.combine(t.date(),END,tzinfo=IST); effective=min(t,end)
    if effective<start:raise RuntimeError("Premarket: candle acquisition not started")
    payload={"securityId":str(sid),"exchangeSegment":"NSE_EQ","instrument":"EQUITY","interval":"1","oi":False,"fromDate":start.strftime("%Y-%m-%d %H:%M:%S"),"toDate":effective.strftime("%Y-%m-%d %H:%M:%S")}
    r=http().post(INTRADAY,headers=headers(),json=payload,timeout=TIMEOUT)
    if r.status_code in (401,403):
        _token=None; fresh=token(force=True)
        if not fresh:raise RuntimeError(_last_token_error or "Dhan re-authentication failed")
        r=http().post(INTRADAY,headers=headers(),json=payload,timeout=TIMEOUT)
    if r.status_code>=400:raise RuntimeError(f"Dhan HTTP {r.status_code}: {r.text[:350]}")
    b=r.json(); arrays=[b.get(k) or [] for k in ("timestamp","open","high","low","close","volume")]; n=min(len(x) for x in arrays) if arrays else 0; out=[]
    for i in range(n):
        try:
            dt=datetime.fromtimestamp(int(arrays[0][i]),tz=IST); o,h,l,c,v=arrays[1][i],arrays[2][i],arrays[3][i],arrays[4][i],arrays[5][i]; of,hf,lf,cf,vf=map(float,(o,h,l,c,v))
        except (TypeError,ValueError,OverflowError):continue
        if dt<start or dt>effective or dt.second or dt.microsecond:continue
        if min(of,hf,lf,cf)<=0 or hf<max(of,cf) or lf>min(of,cf) or hf<lf or vf<0:continue
        out.append({"timestamp":dt.strftime("%H:%M"),"timestamp_iso":dt.isoformat(),"open":o,"high":h,"low":l,"close":c,"volume":v,"source":"DHAN","synthetic":False})
    if not out:raise RuntimeError("Dhan returned no valid 1-minute candles")
    return out,hashlib.sha256(r.content).hexdigest()

def reset_day(app,day):
    s=app._state
    with app._state_lock:
        if s.get("session_date")==day:return
        s["session_date"]=day; s["cycle_count"]=s["successful_cycle_count"]=s["failed_cycle_count"]=0; s["last_cycle_started_at"]=s["last_cycle_finished_at"]=s["last_successful_cycle_at"]=None; s["collector_error"]=None
        for symbol in STOCKS:s["stocks"][symbol].update({"security_id":None,"candles":{},"last_dhan_fetch_at":None,"last_dhan_success_at":None,"last_candle_time":None,"candle_count":0,"last_error":None,"successful_fetches":0,"failed_fetches":0})

def cycle(app):
    t=now(); reset_day(app,t.strftime("%Y-%m-%d"))
    with app._state_lock:app._state["cycle_count"]+=1; app._state["last_cycle_started_at"]=t.isoformat()
    try:
        profile(False); m=master(False); success=0
        for symbol in STOCKS:
            sid=m[symbol]
            try:
                candles,digest=fetch(symbol,sid,now())
                with app._state_lock:
                    st=app._state["stocks"][symbol]; st["security_id"]=sid
                    for c in candles:st["candles"][c["timestamp_iso"]]=c
                    st["candle_count"]=len(st["candles"]); st["last_candle_time"]=max(st["candles"]); st["last_dhan_fetch_at"]=st["last_dhan_success_at"]=now().isoformat(); st["successful_fetches"]+=1; st["last_error"]=None; st["last_response_sha256"]=digest
                success+=1
            except Exception as exc:
                with app._state_lock:
                    st=app._state["stocks"][symbol]; st["security_id"]=sid; st["last_dhan_fetch_at"]=now().isoformat(); st["last_error"]=str(exc); st["failed_fetches"]+=1
        with app._state_lock:
            s=app._state; s["last_cycle_finished_at"]=now().isoformat()
            if success==len(STOCKS):s["successful_cycle_count"]+=1;s["last_successful_cycle_at"]=s["last_cycle_finished_at"];s["collector_error"]=None
            else:s["failed_cycle_count"]+=1;s["collector_error"]=f"Partial Dhan acquisition: {success}/{len(STOCKS)}"
    except Exception as exc:
        with app._state_lock:app._state["failed_cycle_count"]+=1;app._state["collector_error"]=f"Dhan preflight/acquisition: {exc}";app._state["last_cycle_finished_at"]=now().isoformat()

def collector_loop(app):
    with app._state_lock:app._state["collector_alive"]=True;app._state["collector_started_at"]=now().isoformat()
    while not _stop.is_set():
        st=market_status()
        if st=="OPEN":cycle(app);_stop.wait(POLL)
        elif st=="PREMARKET":
            try:profile(False);master(False); 
            except Exception as exc:
                with app._state_lock:app._state["collector_error"]=f"PREMARKET_AUTH: {exc}"
            else:
                with app._state_lock:app._state["collector_error"]=None
            _stop.wait(30)
        else:_stop.wait(30)
    with app._state_lock:app._state["collector_alive"]=False

def snapshot(app,include_data=False):
    t=now(); market=market_status(t); s=app._state
    with app._state_lock:
        x={"service":"UNPSYCHIC29_LIVE","version":"2.1-HARDENED","source":"DHAN","timezone":"Asia/Kolkata","market_window":"09:15-15:30 IST","market_status":market,"interval":"1m","poll_seconds":POLL,"storage":"MEMORY_ONLY","persistent_storage":False,"synthetic_candles":False,"synthetic_volume":False,"runtime":{k:s.get(k) for k in ("session_date","collector_alive","collector_started_at","last_cycle_started_at","last_cycle_finished_at","last_successful_cycle_at","cycle_count","successful_cycle_count","failed_cycle_count","collector_error")},"dhan_auth":{"token_configured":bool(_token or env("DHAN_ACCESS_TOKEN","DHAN_ACCESS_TOKEN_JWT","DHAN_API_ACCESS_TOKEN","ACCESS_TOKEN")),"token_source":_token_source,"dhan_client_id":_client_id,"token_expiry":_expiry.isoformat() if _expiry else None,"token_generation_last_error":_last_token_error,"last_profile_check_at":_profile_at.isoformat() if _profile_at else None,"data_plan":(_profile or {}).get("dataPlan"),"data_validity":(_profile or {}).get("dataValidity")},"stocks":{}}
        for symbol in STOCKS:
            st=s["stocks"][symbol];keys=sorted(st["candles"]);latest=keys[-1] if keys else None;age=(t-datetime.fromisoformat(latest)).total_seconds() if latest else None
            if market=="OPEN":
                expected=max(0,int((t-datetime.combine(t.date(),START,tzinfo=IST)).total_seconds()//60));missing=max(0,expected-len(keys));state="ERROR" if st["last_error"] else ("NO_DATA" if not latest else ("STALE_DHAN_DATA" if age>STALE else ("CANDLE_GAP" if missing>1 else "OK")))
            else:expected=missing=0;state="READY" if market=="PREMARKET" and _profile and not _last_token_error else ("NO_DATA" if not latest else "OK")
            item={"status":state,"security_id":st["security_id"],"decision_candle":DECISION[symbol],"candle_count":len(keys),"expected_closed_candles":expected,"missing_closed_candles":missing,"last_candle_time":st["last_candle_time"],"last_dhan_fetch_at":st["last_dhan_fetch_at"],"last_dhan_success_at":st["last_dhan_success_at"],"candle_age_seconds":round(age,1) if age is not None else None,"successful_fetches":st["successful_fetches"],"failed_fetches":st["failed_fetches"],"last_error":st["last_error"]}
            if include_data:item["data"]=[st["candles"][k] for k in keys]
            x["stocks"][symbol]=item
    if market=="OPEN":x["overall_status"]="OK" if s["collector_alive"] and not any(v["status"]!="OK" for v in x["stocks"].values()) else "DEGRADED"
    elif market=="PREMARKET":x["overall_status"]="READY" if s["collector_alive"] and _profile and not _last_token_error else "DEGRADED"
    else:x["overall_status"]="OK"
    return x

def no_cache(response):
    response.headers["Cache-Control"]="no-store, no-cache, must-revalidate, max-age=0";response.headers["Pragma"]="no-cache";return response

def health(app):
    x=snapshot(app,False);return no_cache(jsonify({"status":"ok","service":"UNPSYCHIC29_LIVE","process_alive":True,"collector_alive":x["runtime"]["collector_alive"],"market_status":x["market_status"],"overall_status":x["overall_status"],"last_successful_cycle_at":x["runtime"]["last_successful_cycle_at"],"collector_error":x["runtime"]["collector_error"]}))

def status(app):
    x=snapshot(app,False);response=jsonify(x);response=no_cache(response);return response,(200 if x["overall_status"] in ("OK","READY") else 503)

def live_json(app):
    x=snapshot(app,True);x["generated_at"]=now().isoformat();return no_cache(jsonify(x))

def live_txt(app):
    x=snapshot(app,True);lines=["SERVICE=UNPSYCHIC29_LIVE","VERSION=2.1-HARDENED","SOURCE=DHAN","TIMEZONE=Asia/Kolkata",f"SESSION_DATE={x['runtime']['session_date'] or ''}",f"MARKET_STATUS={x['market_status']}",f"GENERATED_AT={now().isoformat()}","SESSION=09:15-15:30","INTERVAL=1m","STORAGE=MEMORY_ONLY","PERSISTENT_STORAGE=false","SYNTHETIC_CANDLES=false","SYNTHETIC_VOLUME=false",f"COLLECTOR_ALIVE={x['runtime']['collector_alive']}",f"OVERALL_STATUS={x['overall_status']}",f"LAST_SUCCESSFUL_CYCLE={x['runtime']['last_successful_cycle_at'] or ''}",f"CYCLE_COUNT={x['runtime']['cycle_count']}","","FORMAT=STOCK|TIME|OPEN|HIGH|LOW|CLOSE|VOLUME|SOURCE"]
    for symbol in STOCKS:
        st=x["stocks"][symbol];lines += ["",f"STOCK={symbol}",f"SECURITY_ID={st['security_id'] or ''}",f"DECISION_CANDLE={st['decision_candle']}",f"CANDLE_COUNT={st['candle_count']}",f"LAST_CANDLE={st['last_candle_time'] or ''}",f"LAST_DHAN_SUCCESS={st['last_dhan_success_at'] or ''}"]
        if st["last_error"]:lines.append(f"ERROR={st['last_error']}")
        for c in st["data"]:lines.append(f"{symbol}|{c['timestamp']}|{c['open']}|{c['high']}|{c['low']}|{c['close']}|{c['volume']}|DHAN")
    return no_cache(Response("\n".join(lines)+"\n",mimetype="text/plain"))

def probe(app):
    t=now();market=market_status(t);result={"service":"UNPSYCHIC29_LIVE","source":"DHAN","checked_at":t.isoformat(),"market_status":market}
    try:
        p=profile(True);m=master(True);result["auth"]={"ok":True,"token_configured":bool(_token or env("DHAN_ACCESS_TOKEN","DHAN_ACCESS_TOKEN_JWT","DHAN_API_ACCESS_TOKEN","ACCESS_TOKEN")),"token_source":_token_source,"data_plan":p.get("dataPlan"),"data_validity":p.get("dataValidity"),"token_validity":p.get("tokenValidity")};result["security_master"]={s:m[s] for s in STOCKS}
        if market!="OPEN":result["overall"]="READY" if market=="PREMARKET" else "CLOSED";result["message"]="Authentication and security master verified; candle acquisition is intentionally not probed outside market hours.";return no_cache(jsonify(result))
        result["stocks"]={}
        for symbol in STOCKS:
            candles,digest=fetch(symbol,m[symbol],t);result["stocks"][symbol]={"ok":True,"candle_count":len(candles),"last_candle":candles[-1]["timestamp"],"response_sha256":digest}
        result["overall"]="OK"
    except Exception as exc:result["overall"]="FAILED";result["error"]=str(exc)
    return no_cache(jsonify(result))

def install(app):
    global _installed
    with _install_lock:
        if _installed:return
        _installed=True
        app.view_functions["health"]=lambda:health(app);app.view_functions["status"]=lambda:status(app);app.view_functions["live_json"]=lambda:live_json(app);app.view_functions["live_txt"]=lambda:live_txt(app)
        app.add_url_rule("/dhan-check-hard",endpoint="dhan_check_hard_hardened",view_func=lambda:probe(app),methods=["GET"])
        threading.Thread(target=collector_loop,args=(app,),name="dhan-hardened-collector",daemon=True).start()
