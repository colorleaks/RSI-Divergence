"""
RSI Divergence Scanner — 4H Timeframe
Detects 4 divergence scenarios on Binance USDT perpetuals.

Scenario 1 — BULLISH CLASSIC:    Price lower lows + RSI higher lows (downtrend)
Scenario 2 — BULLISH CONSOL:     Price flat + RSI higher highs (consolidation)
Scenario 3 — BEARISH CLASSIC:    Price higher highs + RSI lower highs (uptrend)
Scenario 4 — BEARISH CONSOL:     Price flat + RSI lower lows (consolidation)

Scoring (0-100):
  Touch points         0-20 pts
  RSI level at pivot   0-20 pts  (weighted: oldest 60% + newest 40%)
  RSI difference       0-15 pts
  Price move           0-15 pts
  Volume at pivot      0-15 pts
  Recency              0-10 pts
  Daily trend align     0-5 pts
  Consolidation bonus  +10 pts (capped at 100)

Run:  python rsi_div_scanner.py
Open: http://localhost:5014
"""

from flask import Flask, jsonify, request, Response
import requests, threading, time, os, math

app  = Flask(__name__)
BASE = "https://fapi.binance.com"

import urllib3
urllib3.disable_warnings()
SESSION = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=40, pool_maxsize=40, max_retries=1)
SESSION.mount("https://", adapter)

# ── Telegram ───────────────────────────────────────────────────────────────
tg_cfg = {"enabled": False, "token": "", "chat_id": ""}

def tg_send(text):
    if not tg_cfg["enabled"]: return
    def _send():
        try:
            url = f"https://api.telegram.org/bot{tg_cfg['token']}/sendMessage"
            SESSION.post(url, json={"chat_id": tg_cfg["chat_id"],
                "text": text, "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            print(f"[TG] {e}")
    threading.Thread(target=_send, daemon=True).start()

@app.route("/api/telegram/config", methods=["GET"])
def tg_get(): return jsonify(tg_cfg)

@app.route("/api/telegram/config", methods=["POST"])
def tg_set():
    d = request.get_json() or {}
    if "enabled"  in d: tg_cfg["enabled"]  = bool(d["enabled"])
    if "token"    in d: tg_cfg["token"]    = str(d["token"]).strip()
    if "chat_id"  in d: tg_cfg["chat_id"]  = str(d["chat_id"]).strip()
    return jsonify({"status": "ok"})

@app.route("/api/telegram/test", methods=["POST"])
def tg_test():
    try:
        url = f"https://api.telegram.org/bot{tg_cfg['token']}/sendMessage"
        r = SESSION.post(url, json={"chat_id": tg_cfg["chat_id"],
            "text": "✅ <b>RSI Divergence Scanner connected!</b>",
            "parse_mode": "HTML"}, timeout=8)
        if r.status_code == 200:
            return jsonify({"status": "ok", "msg": "Test sent!"})
        return jsonify({"status": "error",
                        "msg": r.json().get("description","Failed")})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})

# ── Settings ───────────────────────────────────────────────────────────────
settings = {
    "rsi_period":         14,    # RSI period
    "fractal_periods":    2,     # bars each side for pivot detection
    "lookback_bars":      60,    # bars to look back
    "min_pivot_dist":     8,     # min bars between pivots
    "max_pivot_dist":     30,    # max bars between pivots
    "recency_bars":       10,    # second pivot within last N bars
    "min_rsi_diff":       3.0,   # min RSI points difference between pivots
    "consol_range_pct":   8.0,   # max price range % for consolidation
    "consol_slope_pct":   3.0,   # max price slope % for consolidation
    "min_score":          40,    # min score to show
    "alert_score":        70,    # min score to alert
    "alert_max_bars_ago": 3,     # only alert if bars_ago <= this
    "min_vol_24m":        2.0,   # min 24H volume in millions USDT
    # Scenario toggles
    "sc1_enabled": True,   # bullish classic
    "sc2_enabled": True,   # bullish consolidation
    "sc3_enabled": True,   # bearish classic
    "sc4_enabled": True,   # bearish consolidation
}

@app.route("/api/settings", methods=["GET"])
def settings_get(): return jsonify(settings)

@app.route("/api/settings", methods=["POST"])
def settings_set():
    d = request.get_json() or {}
    int_keys   = ["rsi_period","fractal_periods","lookback_bars",
                  "min_pivot_dist","max_pivot_dist","recency_bars",
                  "min_score","alert_score","alert_max_bars_ago"]
    float_keys = ["min_rsi_diff","consol_range_pct","consol_slope_pct","min_vol_24m"]
    bool_keys  = ["sc1_enabled","sc2_enabled","sc3_enabled","sc4_enabled"]
    for k in int_keys:
        if k in d: settings[k] = max(1, int(d[k]))
    for k in float_keys:
        if k in d: settings[k] = max(0.0, float(d[k]))
    for k in bool_keys:
        if k in d: settings[k] = bool(d[k])
    return jsonify({"status": "ok", **settings})

# ── State ──────────────────────────────────────────────────────────────────
cache = {
    "signals": [], "last_scan": None, "scanning": False,
    "progress": "", "progress_pct": 0, "error": None,
    "scan_count": 0, "next_scan_at": 0,
}
scan_lock = threading.Lock()

# ── Timing — 4H boundary ──────────────────────────────────────────────────
INTERVAL_MS = 4 * 60 * 60 * 1000

def next_4h_ms():
    now_ms = int(time.time() * 1000)
    return ((now_ms // INTERVAL_MS) + 1) * INTERVAL_MS

def secs_to_next_4h():
    return max(0, (next_4h_ms() - int(time.time() * 1000)) / 1000)

# ── HTTP ───────────────────────────────────────────────────────────────────
def get_json(endpoint, params=None, timeout=10):
    try:
        r = SESSION.get(BASE + endpoint, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except:
        return None

# ── Math ───────────────────────────────────────────────────────────────────
def last_closed_pos(klines):
    now_ms = int(time.time() * 1000)
    last_ct = int(klines[-1][6])
    return len(klines) - 1 if last_ct < now_ms else len(klines) - 2

def calc_rsi(closes, period=14):
    """Full RSI series using Wilder smoothing."""
    n = len(closes)
    if n < period + 1:
        return [None] * n
    gains = [max(closes[i]-closes[i-1], 0) for i in range(1, n)]
    losses= [max(closes[i-1]-closes[i], 0) for i in range(1, n)]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    def rsi_val(ag, al):
        if al == 0: return 100.0
        return 100.0 - 100.0/(1.0 + ag/al)
    out = [None] * period
    out.append(rsi_val(avg_g, avg_l))
    for i in range(period, len(gains)):
        avg_g = (avg_g*(period-1) + gains[i]) / period
        avg_l = (avg_l*(period-1) + losses[i]) / period
        out.append(rsi_val(avg_g, avg_l))
    return out

def find_pivots(values, fp, mode="low"):
    """Pivot lows or highs with fp bars each side."""
    pivots = []
    n = len(values)
    for i in range(fp, n - fp):
        if values[i] is None: continue
        window = [values[i+j] for j in range(-fp, fp+1)
                  if 0 <= i+j < n and values[i+j] is not None]
        if len(window) < fp*2+1: continue
        if mode == "low"  and values[i] == min(window):
            pivots.append((i, values[i]))
        if mode == "high" and values[i] == max(window):
            pivots.append((i, values[i]))
    return pivots

def linear_slope_pct(values):
    """
    Linear regression slope as % change from start to end.
    Returns (start_val, end_val, slope_pct, r2)
    """
    n = len(values)
    if n < 2: return (values[0], values[-1], 0.0, 0.0)
    xs = list(range(n))
    xm = sum(xs)/n; ym = sum(values)/n
    ss_xx = sum((x-xm)**2 for x in xs)
    ss_xy = sum((xs[i]-xm)*(values[i]-ym) for i in range(n))
    slope  = ss_xy/ss_xx if ss_xx > 0 else 0
    interc = ym - slope*xm
    y_pred = [slope*x+interc for x in xs]
    ss_res = sum((values[i]-y_pred[i])**2 for i in range(n))
    ss_tot = sum((v-ym)**2 for v in values) or 1e-10
    r2 = max(0.0, 1.0 - ss_res/ss_tot)
    start_v = interc
    end_v   = slope*(n-1)+interc
    slope_pct = (end_v-start_v)/abs(start_v)*100 if start_v != 0 else 0
    return (start_v, end_v, slope_pct, r2)

def ema_val(closes, period):
    if len(closes) < period: return None
    k = 2.0/(period+1)
    v = sum(closes[:period])/period
    for c in closes[period:]:
        v = c*k + v*(1-k)
    return v

def daily_ema50_slope(k_daily):
    if not k_daily or len(k_daily) < 60: return 'flat'
    c_pos = last_closed_pos(k_daily)
    if c_pos < 55: return 'flat'
    cl = [float(k[4]) for k in k_daily[:c_pos+1]]
    e_now  = ema_val(cl,      50)
    e_prev = ema_val(cl[:-5], 50)
    if not e_now or not e_prev: return 'flat'
    chg = (e_now-e_prev)/e_prev*100
    if chg >  0.1: return 'up'
    if chg < -0.1: return 'down'
    return 'flat'

# ── Scoring ────────────────────────────────────────────────────────────────
def score_touch_points(n_touches):
    if n_touches >= 4: return 20
    if n_touches == 3: return 15
    if n_touches == 2: return  8
    return 0

def score_rsi_level(rsi_oldest, rsi_newest, direction):
    """
    Weighted RSI level score.
    direction = 'bull' or 'bear'
    """
    weighted = rsi_oldest * 0.6 + rsi_newest * 0.4
    if direction == 'bull':
        if weighted < 30:  return 20
        if weighted < 40:  return 15
        if weighted < 50:  return 10
        if weighted < 60:  return  5
        return 0
    else:  # bear
        if weighted > 70:  return 20
        if weighted > 60:  return 15
        if weighted > 50:  return 10
        if weighted > 40:  return  5
        return 0

def score_rsi_diff(rsi_vals):
    """
    RSI consistency across all pivots.
    For bullish: how consistently RSI rose across ALL pivots
    For bearish: how consistently RSI fell across ALL pivots
    Uses total difference from first to last pivot.
    """
    if len(rsi_vals) < 2: return 0
    total_diff = abs(rsi_vals[-1] - rsi_vals[0])
    if total_diff > 15: return 15
    if total_diff > 10: return 12
    if total_diff >  5: return  8
    if total_diff >  3: return  4
    return 0

def score_price_move(p_vals, direction):
    """
    How much price moved between pivots.
    direction = 'bull' (price falling) or 'bear' (price rising)
    """
    if len(p_vals) < 2: return 0
    total_move = abs(p_vals[-1] - p_vals[0]) / abs(p_vals[0]) * 100
    if total_move > 10: return 15
    if total_move >  5: return 10
    if total_move >  2: return  5
    return 0

def score_volume(pivot_vol, local_avg_vol):
    ratio = pivot_vol / local_avg_vol if local_avg_vol > 0 else 0
    if ratio > 2.0: return 15
    if ratio > 1.5: return 12
    if ratio > 1.0: return  8
    if ratio > 0.8: return  4
    return 0

def score_recency(bars_ago):
    if bars_ago <= 2: return 10
    if bars_ago <= 5: return  7
    if bars_ago <= 8: return  4
    if bars_ago <= 10: return 2
    return 0

def score_trend_align(div_type, slope):
    """
    div_type: 'bullish' or 'bearish'
    slope: 'up', 'down', 'flat'
    """
    if div_type == 'bullish':
        if slope in ('up','flat'): return 5
        return 0
    else:
        if slope in ('down','flat'): return 5
        return 0

# ── Price mode detection ───────────────────────────────────────────────────
def detect_price_mode(closes, highs, lows, n_bars=20):
    """
    Returns: 'downtrend', 'uptrend', 'consolidation', 'borderline'
    """
    if len(closes) < n_bars: return 'borderline'
    recent_c = closes[-n_bars:]
    recent_h = highs[-n_bars:]
    recent_l = lows[-n_bars:]

    # Price range
    rng_h = max(recent_h); rng_l = min(recent_l)
    range_pct = (rng_h - rng_l) / rng_l * 100 if rng_l > 0 else 999

    # Linear slope
    _, _, slope_pct, _ = linear_slope_pct(recent_c)

    cr = settings["consol_range_pct"]
    cs = settings["consol_slope_pct"]

    if range_pct < cr and abs(slope_pct) < cs:
        return 'consolidation'
    if slope_pct < -cs:
        return 'downtrend'
    if slope_pct > cs:
        return 'uptrend'
    return 'borderline'

# ── Core analysis ──────────────────────────────────────────────────────────
def analyze(sym, klines, ticker, k_daily=None):
    if not klines or len(klines) < 80:
        return []

    c_pos = last_closed_pos(klines)
    bars  = klines[:c_pos+1]
    n     = len(bars)
    if n < 80: return []

    closes = [float(k[4]) for k in bars]
    highs  = [float(k[2]) for k in bars]
    lows   = [float(k[3]) for k in bars]
    vols   = [float(k[5]) for k in bars]
    price  = closes[-1]

    rsi_series = calc_rsi(closes, settings["rsi_period"])

    fp       = settings["fractal_periods"]
    lookback = settings["lookback_bars"]
    min_dist = settings["min_pivot_dist"]
    max_dist = settings["max_pivot_dist"]
    recency  = settings["recency_bars"]
    cutoff   = n - lookback

    # Daily slope
    slope = daily_ema50_slope(k_daily)

    # Price mode
    mode = detect_price_mode(closes, highs, lows, 20)

    # Pivots on price
    price_lows   = [(i,v) for i,v in find_pivots(lows,  fp,"low")  if i >= cutoff]
    price_highs  = [(i,v) for i,v in find_pivots(highs, fp,"high") if i >= cutoff]

    # Pivots on RSI
    rsi_lows     = [(i,v) for i,v in find_pivots(rsi_series, fp,"low")  if i >= cutoff]
    rsi_highs    = [(i,v) for i,v in find_pivots(rsi_series, fp,"high") if i >= cutoff]

    results = []

    # ─────────────────────────────────────────────────────────────────────
    # SCENARIO 1 — BULLISH CLASSIC
    # Price making lower lows, RSI making higher lows
    # ─────────────────────────────────────────────────────────────────────
    if settings["sc1_enabled"] and mode == 'downtrend':
        # Find chains of price lower lows where RSI matches higher lows
        # Build groups of consecutive pivot lows that form the divergence
        chains = []
        for start in range(len(price_lows)):
            chain_p = [price_lows[start]]
            chain_r = []
            # Find matching RSI low near this price pivot
            best_r = min(rsi_lows, key=lambda x: abs(x[0]-price_lows[start][0]),
                         default=None)
            if best_r and abs(best_r[0]-price_lows[start][0]) <= fp:
                chain_r = [best_r]
            else:
                continue

            for nxt in range(start+1, len(price_lows)):
                pi, pv = price_lows[nxt]
                prev_pi, prev_pv = chain_p[-1]
                if pi - prev_pi < min_dist: continue
                if pi - prev_pi > max_dist: break
                if pv >= prev_pv: continue  # must be lower low
                # Find RSI low near this price pivot
                rr = min(rsi_lows, key=lambda x: abs(x[0]-pi), default=None)
                if not rr or abs(rr[0]-pi) > fp: continue
                prev_ri, prev_rv = chain_r[-1]
                if rr[0] <= chain_r[-1][0]: continue  # RSI idx must advance
                if rr[1] <= prev_rv: continue  # must be higher RSI low
                chain_p.append(price_lows[nxt])
                chain_r.append(rr)

            if len(chain_p) >= 2:
                chains.append((chain_p, chain_r))

        for chain_p, chain_r in chains:
            # Most recent pivot
            last_p_idx, last_p_val = chain_p[-1]
            bars_ago = n - 1 - last_p_idx
            if bars_ago > recency: continue

            # RSI check
            rsi_oldest = chain_r[0][1]
            rsi_newest = chain_r[-1][1]
            rsi_diff   = rsi_newest - rsi_oldest
            if rsi_diff < settings["min_rsi_diff"]: continue
            # RSI extreme gate: at least the oldest pivot must be below 60
            if rsi_oldest >= 60: continue

            # Volume at most recent price pivot
            p2i   = last_p_idx
            sv    = max(0, p2i-10); ev = min(len(vols), p2i+10)
            avg_v = sum(vols[sv:ev])/max(len(vols[sv:ev]),1)
            piv_v = vols[p2i] if p2i < len(vols) else 0

            # Price move
            price_vals = [v for _,v in chain_p]

            # Scoring
            n_touches = len(chain_p)
            s_touch   = score_touch_points(n_touches)
            s_rsi_lvl = score_rsi_level(rsi_oldest, rsi_newest, 'bull')
            s_rsi_dif = score_rsi_diff([v for _,v in chain_r])
            s_price   = score_price_move(price_vals, 'bull')
            s_vol     = score_volume(piv_v, avg_v)
            s_rec     = score_recency(bars_ago)
            s_trend   = score_trend_align('bullish', slope)
            total     = min(100, s_touch+s_rsi_lvl+s_rsi_dif+s_price+s_vol+s_rec+s_trend)

            if total < settings["min_score"]: continue

            results.append({
                "symbol":    sym, "base": sym.replace("USDT",""),
                "scenario":  1, "label": "BULLISH CLASSIC",
                "direction": "bullish", "mode": mode,
                "price":     round(price,8),
                "chg_24h":   round(float(ticker.get("priceChangePercent",0)),2),
                "vol_24m":   round(float(ticker.get("quoteVolume",0))/1e6,1),
                "score":     total,
                "n_touches": n_touches,
                "bars_ago":  bars_ago,
                "rsi_oldest":round(rsi_oldest,1),
                "rsi_newest":round(rsi_newest,1),
                "rsi_diff":  round(rsi_diff,1),
                "price_oldest": round(chain_p[0][1],8),
                "price_newest": round(last_p_val,8),
                "slope":     slope,
                "pts": {"touch":s_touch,"rsi_lvl":s_rsi_lvl,"rsi_dif":s_rsi_dif,
                        "price":s_price,"vol":s_vol,"rec":s_rec,"trend":s_trend},
                "pivot_idxs": [i for i,_ in chain_p],
                "rsi_idxs":   [i for i,_ in chain_r],
                "rsi_vals":   [v for _,v in chain_r],
            })

    # ─────────────────────────────────────────────────────────────────────
    # SCENARIO 2 — BULLISH CONSOLIDATION
    # Price flat, RSI making higher highs
    # ─────────────────────────────────────────────────────────────────────
    if settings["sc2_enabled"] and mode == 'consolidation':
        # Find consecutive RSI highs that are rising while price is flat
        chains = []
        for start in range(len(rsi_highs)):
            chain_r = [rsi_highs[start]]
            for nxt in range(start+1, len(rsi_highs)):
                ri, rv = rsi_highs[nxt]
                prev_ri, prev_rv = chain_r[-1]
                if ri - prev_ri < min_dist: continue
                if ri - prev_ri > max_dist: break
                if rv <= prev_rv: continue  # must be higher high
                chain_r.append(rsi_highs[nxt])
            if len(chain_r) >= 2:
                chains.append(chain_r)

        for chain_r in chains:
            last_r_idx, last_r_val = chain_r[-1]
            bars_ago = n - 1 - last_r_idx
            if bars_ago > recency: continue

            rsi_oldest = chain_r[0][1]
            rsi_newest = last_r_val
            rsi_diff   = rsi_newest - rsi_oldest
            if rsi_diff < settings["min_rsi_diff"]: continue

            p2i   = last_r_idx
            sv    = max(0,p2i-10); ev = min(len(vols),p2i+10)
            avg_v = sum(vols[sv:ev])/max(len(vols[sv:ev]),1)
            piv_v = vols[p2i] if p2i < len(vols) else 0

            n_touches = len(chain_r)
            s_touch   = score_touch_points(n_touches)
            s_rsi_lvl = score_rsi_level(rsi_oldest, rsi_newest, 'bull')
            s_rsi_dif = score_rsi_diff([v for _,v in chain_r])
            s_price   = 0  # no price move requirement for consolidation
            s_vol     = score_volume(piv_v, avg_v)
            s_rec     = score_recency(bars_ago)
            s_trend   = score_trend_align('bullish', slope)
            consol_bonus = 10  # consolidation bonus
            total     = min(100, s_touch+s_rsi_lvl+s_rsi_dif+s_price+s_vol+s_rec+s_trend+consol_bonus)

            if total < settings["min_score"]: continue

            results.append({
                "symbol":    sym, "base": sym.replace("USDT",""),
                "scenario":  2, "label": "BULLISH CONSOL",
                "direction": "bullish", "mode": mode,
                "price":     round(price,8),
                "chg_24h":   round(float(ticker.get("priceChangePercent",0)),2),
                "vol_24m":   round(float(ticker.get("quoteVolume",0))/1e6,1),
                "score":     total,
                "n_touches": n_touches,
                "bars_ago":  bars_ago,
                "rsi_oldest":round(rsi_oldest,1),
                "rsi_newest":round(rsi_newest,1),
                "rsi_diff":  round(rsi_diff,1),
                "price_oldest": round(closes[chain_r[0][0]],8),
                "price_newest": round(closes[last_r_idx],8),
                "slope":     slope,
                "pts": {"touch":s_touch,"rsi_lvl":s_rsi_lvl,"rsi_dif":s_rsi_dif,
                        "price":s_price,"vol":s_vol,"rec":s_rec,"trend":s_trend,
                        "consol":consol_bonus},
                "pivot_idxs": [i for i,_ in chain_r],
                "rsi_idxs":   [i for i,_ in chain_r],
                "rsi_vals":   [v for _,v in chain_r],
            })

    # ─────────────────────────────────────────────────────────────────────
    # SCENARIO 3 — BEARISH CLASSIC
    # Price making higher highs, RSI making lower highs
    # ─────────────────────────────────────────────────────────────────────
    if settings["sc3_enabled"] and mode == 'uptrend':
        chains = []
        for start in range(len(price_highs)):
            chain_p = [price_highs[start]]
            best_r  = min(rsi_highs, key=lambda x: abs(x[0]-price_highs[start][0]),
                          default=None)
            if best_r and abs(best_r[0]-price_highs[start][0]) <= fp:
                chain_r = [best_r]
            else:
                continue

            for nxt in range(start+1, len(price_highs)):
                pi, pv = price_highs[nxt]
                prev_pi, prev_pv = chain_p[-1]
                if pi - prev_pi < min_dist: continue
                if pi - prev_pi > max_dist: break
                if pv <= prev_pv: continue  # must be higher high
                rr = min(rsi_highs, key=lambda x: abs(x[0]-pi), default=None)
                if not rr or abs(rr[0]-pi) > fp: continue
                prev_ri, prev_rv = chain_r[-1]
                if rr[0] <= chain_r[-1][0]: continue  # RSI idx must advance
                if rr[1] >= prev_rv: continue  # must be lower RSI high
                chain_p.append(price_highs[nxt])
                chain_r.append(rr)

            if len(chain_p) >= 2:
                chains.append((chain_p, chain_r))

        for chain_p, chain_r in chains:
            last_p_idx, last_p_val = chain_p[-1]
            bars_ago = n - 1 - last_p_idx
            if bars_ago > recency: continue

            rsi_oldest = chain_r[0][1]
            rsi_newest = chain_r[-1][1]
            rsi_diff   = rsi_oldest - rsi_newest  # falling RSI
            if rsi_diff < settings["min_rsi_diff"]: continue
            # RSI extreme gate: at least the oldest pivot must be above 40
            if rsi_oldest <= 40: continue

            p2i   = last_p_idx
            sv    = max(0,p2i-10); ev = min(len(vols),p2i+10)
            avg_v = sum(vols[sv:ev])/max(len(vols[sv:ev]),1)
            piv_v = vols[p2i] if p2i < len(vols) else 0

            price_vals = [v for _,v in chain_p]
            n_touches = len(chain_p)
            s_touch   = score_touch_points(n_touches)
            s_rsi_lvl = score_rsi_level(rsi_oldest, rsi_newest, 'bear')
            s_rsi_dif = score_rsi_diff([v for _,v in chain_r])
            s_price   = score_price_move(price_vals, 'bear')
            s_vol     = score_volume(piv_v, avg_v)
            s_rec     = score_recency(bars_ago)
            s_trend   = score_trend_align('bearish', slope)
            total     = min(100, s_touch+s_rsi_lvl+s_rsi_dif+s_price+s_vol+s_rec+s_trend)

            if total < settings["min_score"]: continue

            results.append({
                "symbol":    sym, "base": sym.replace("USDT",""),
                "scenario":  3, "label": "BEARISH CLASSIC",
                "direction": "bearish", "mode": mode,
                "price":     round(price,8),
                "chg_24h":   round(float(ticker.get("priceChangePercent",0)),2),
                "vol_24m":   round(float(ticker.get("quoteVolume",0))/1e6,1),
                "score":     total,
                "n_touches": n_touches,
                "bars_ago":  bars_ago,
                "rsi_oldest":round(rsi_oldest,1),
                "rsi_newest":round(rsi_newest,1),
                "rsi_diff":  round(rsi_diff,1),
                "price_oldest": round(chain_p[0][1],8),
                "price_newest": round(last_p_val,8),
                "slope":     slope,
                "pts": {"touch":s_touch,"rsi_lvl":s_rsi_lvl,"rsi_dif":s_rsi_dif,
                        "price":s_price,"vol":s_vol,"rec":s_rec,"trend":s_trend},
                "pivot_idxs": [i for i,_ in chain_p],
                "rsi_idxs":   [i for i,_ in chain_r],
                "rsi_vals":   [v for _,v in chain_r],
            })

    # ─────────────────────────────────────────────────────────────────────
    # SCENARIO 4 — BEARISH CONSOLIDATION
    # Price flat, RSI making lower lows
    # ─────────────────────────────────────────────────────────────────────
    if settings["sc4_enabled"] and mode == 'consolidation':
        chains = []
        for start in range(len(rsi_lows)):
            chain_r = [rsi_lows[start]]
            for nxt in range(start+1, len(rsi_lows)):
                ri, rv = rsi_lows[nxt]
                prev_ri, prev_rv = chain_r[-1]
                if ri - prev_ri < min_dist: continue
                if ri - prev_ri > max_dist: break
                if rv >= prev_rv: continue  # must be lower low
                chain_r.append(rsi_lows[nxt])
            if len(chain_r) >= 2:
                chains.append(chain_r)

        for chain_r in chains:
            last_r_idx, last_r_val = chain_r[-1]
            bars_ago = n - 1 - last_r_idx
            if bars_ago > recency: continue

            rsi_oldest = chain_r[0][1]
            rsi_newest = last_r_val
            rsi_diff   = rsi_oldest - rsi_newest
            if rsi_diff < settings["min_rsi_diff"]: continue

            p2i   = last_r_idx
            sv    = max(0,p2i-10); ev = min(len(vols),p2i+10)
            avg_v = sum(vols[sv:ev])/max(len(vols[sv:ev]),1)
            piv_v = vols[p2i] if p2i < len(vols) else 0

            n_touches = len(chain_r)
            s_touch   = score_touch_points(n_touches)
            s_rsi_lvl = score_rsi_level(rsi_oldest, rsi_newest, 'bear')
            s_rsi_dif = score_rsi_diff([v for _,v in chain_r])
            s_price   = 0
            s_vol     = score_volume(piv_v, avg_v)
            s_rec     = score_recency(bars_ago)
            s_trend   = score_trend_align('bearish', slope)
            consol_bonus = 10
            total     = min(100, s_touch+s_rsi_lvl+s_rsi_dif+s_price+s_vol+s_rec+s_trend+consol_bonus)

            if total < settings["min_score"]: continue

            results.append({
                "symbol":    sym, "base": sym.replace("USDT",""),
                "scenario":  4, "label": "BEARISH CONSOL",
                "direction": "bearish", "mode": mode,
                "price":     round(price,8),
                "chg_24h":   round(float(ticker.get("priceChangePercent",0)),2),
                "vol_24m":   round(float(ticker.get("quoteVolume",0))/1e6,1),
                "score":     total,
                "n_touches": n_touches,
                "bars_ago":  bars_ago,
                "rsi_oldest":round(rsi_oldest,1),
                "rsi_newest":round(rsi_newest,1),
                "rsi_diff":  round(rsi_diff,1),
                "price_oldest": round(closes[chain_r[0][0]],8),
                "price_newest": round(closes[last_r_idx],8),
                "slope":     slope,
                "pts": {"touch":s_touch,"rsi_lvl":s_rsi_lvl,"rsi_dif":s_rsi_dif,
                        "price":s_price,"vol":s_vol,"rec":s_rec,"trend":s_trend,
                        "consol":consol_bonus},
                "pivot_idxs": [i for i,_ in chain_r],
                "rsi_idxs":   [i for i,_ in chain_r],
                "rsi_vals":   [v for _,v in chain_r],
            })

    # Deduplicate per symbol per direction — keep highest score
    seen = {}
    for r in results:
        key = f"{r['symbol']}_{r['direction']}"
        if key not in seen or r['score'] > seen[key]['score']:
            seen[key] = r
    return list(seen.values())

# ── Chart data API ─────────────────────────────────────────────────────────
@app.route("/api/chart/<symbol>")
def api_chart(symbol):
    try:
        klines = get_json("/fapi/v1/klines",
                          {"symbol": symbol, "interval": "4h", "limit": 200},
                          timeout=12)
        if not klines: return jsonify({"error":"No data"}),404
        c_pos = last_closed_pos(klines)
        bars  = klines[:c_pos+1]
        ohlcv = [{"t":int(b[0])//1000,"o":float(b[1]),"h":float(b[2]),
                  "l":float(b[3]),"c":float(b[4]),"v":float(b[5])} for b in bars]
        closes = [b["c"] for b in ohlcv]
        rsi_s  = calc_rsi(closes, settings["rsi_period"])
        rsi_data = [{"t":ohlcv[i]["t"],"v":round(rsi_s[i],2)}
                    for i in range(len(ohlcv)) if rsi_s[i] is not None]
        sigs = [s for s in cache["signals"] if s["symbol"]==symbol]
        return jsonify({"symbol":symbol,"ohlcv":ohlcv,
                        "rsi":rsi_data,"signals":sigs})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error":str(e)}),500

# ── Scan ───────────────────────────────────────────────────────────────────
def do_scan():
    if not scan_lock.acquire(blocking=False): return
    cache["scanning"]     = True
    cache["error"]        = None
    cache["progress"]     = "Fetching tickers..."
    cache["progress_pct"] = 0
    try:
        tickers = get_json("/fapi/v1/ticker/24hr") or []
        min_vol = settings["min_vol_24m"] * 1_000_000
        usdt = sorted(
            [t for t in tickers if t["symbol"].endswith("USDT")
             and float(t.get("quoteVolume",0)) >= min_vol],
            key=lambda x: float(x.get("quoteVolume",0)), reverse=True)[:300]

        total=len(usdt); done=[0]; found=[]; lock=threading.Lock()
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process(t):
            sym = t["symbol"]
            try:
                from concurrent.futures import ThreadPoolExecutor as TPE
                with TPE(max_workers=2) as ex:
                    f4h = ex.submit(get_json,"/fapi/v1/klines",
                                    {"symbol":sym,"interval":"4h","limit":200})
                    f1d = ex.submit(get_json,"/fapi/v1/klines",
                                    {"symbol":sym,"interval":"1d","limit":60})
                    k4h=f4h.result(); k1d=f1d.result()
                return analyze(sym, k4h, t, k1d)
            except Exception as e:
                print(f"[SCAN] {sym}: {e}")
                return []
            finally:
                with lock:
                    done[0]+=1
                    cache["progress"]=f"Scanning [{done[0]}/{total}]..."
                    cache["progress_pct"]=int(done[0]/total*95)

        with ThreadPoolExecutor(max_workers=15) as ex:
            futures = {ex.submit(process,t):t for t in usdt}
            for f in as_completed(futures,timeout=180):
                try:
                    res=f.result(timeout=15)
                    if res:
                        with lock: found.extend(res)
                except Exception as e:
                    print(f"[FUTURES] {e}")

        found.sort(key=lambda x: -x["score"])

        # Telegram alerts
        max_bars = settings["alert_max_bars_ago"]
        fresh    = [s for s in found if s["bars_ago"] <= max_bars
                    and s["score"] >= settings["alert_score"]]
        if tg_cfg["enabled"] and fresh:
            scan_time = time.strftime("%Y-%m-%d %H:%M")
            parts = [
                f"📈 <b>RSI Divergence Scan — {scan_time}</b>",
                f"🟢 Bullish: {sum(1 for s in fresh if s['direction']=='bullish')}  "
                f"🔴 Bearish: {sum(1 for s in fresh if s['direction']=='bearish')}",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ]
            for s in fresh[:15]:
                ico = "🟢" if s["direction"]=="bullish" else "🔴"
                stars = "🟢🟢🟢" if s["score"]>=80 else "🟢🟢" if s["score"]>=60 else "🟡"
                parts.append(
                    f"{ico} <b>{s['base']}/USDT</b> — {s['label']}\n"
                    f"   Score: <b>{s['score']}/100</b> {stars}  |  "
                    f"Touches: {s['n_touches']}  |  {s['bars_ago']} bars ago\n"
                    f"   RSI: {s['rsi_oldest']} → {s['rsi_newest']} "
                    f"(+{s['rsi_diff']} pts)  |  Slope: {s['slope']}"
                )
            parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            tg_send("\n".join(parts))

        cache["signals"]      = found
        cache["last_scan"]    = time.strftime("%Y-%m-%d %H:%M:%S")
        cache["scan_count"]   = cache.get("scan_count",0)+1
        cache["progress"]     = f"Done — {len(found)} divergences found"
        cache["progress_pct"] = 100
        cache["next_scan_at"] = next_4h_ms()/1000
    except Exception as e:
        import traceback
        cache["error"]=str(e); cache["progress"]=f"Error: {str(e)[:80]}"
        print(traceback.format_exc())
    finally:
        cache["scanning"]=False; scan_lock.release()

def sync_scanner():
    time.sleep(5)
    while True:
        try:
            secs=secs_to_next_4h(); time.sleep(secs+0.5)
            if not cache["scanning"]:
                cache["next_scan_at"]=next_4h_ms()/1000
                threading.Thread(target=do_scan,daemon=True).start()
            else:
                time.sleep(60)
        except Exception as e:
            print(f"[SYNC] {e}"); time.sleep(60)

cache["next_scan_at"]=next_4h_ms()/1000
threading.Thread(target=sync_scanner,daemon=True).start()

@app.route("/")
def index(): return Response(HTML, mimetype="text/html")

@app.route("/health")
def health(): return jsonify({"status":"ok","signals":len(cache["signals"])})

@app.route("/api/scan", methods=["POST"])
def api_scan():
    if cache["scanning"]: return jsonify({"status":"already_scanning"})
    threading.Thread(target=do_scan,daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/api/status")
def api_status():
    now=time.time(); sigs=cache["signals"]
    return jsonify({
        "scanning":cache["scanning"],"progress":cache["progress"],
        "progress_pct":cache["progress_pct"],"last_scan":cache["last_scan"],
        "error":cache["error"],"scan_count":cache.get("scan_count",0),
        "next_scan_in":max(0,cache.get("next_scan_at",0)-now),
        "signals":sigs,"settings":settings,
        "stats":{
            "total":len(sigs),
            "bullish":sum(1 for s in sigs if s["direction"]=="bullish"),
            "bearish":sum(1 for s in sigs if s["direction"]=="bearish"),
            "sc1":sum(1 for s in sigs if s["scenario"]==1),
            "sc2":sum(1 for s in sigs if s["scenario"]==2),
            "sc3":sum(1 for s in sigs if s["scenario"]==3),
            "sc4":sum(1 for s in sigs if s["scenario"]==4),
        }
    })

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>RSI Divergence Scanner</title>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700;900&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#05070d;--bg2:#08090f;--bg3:#0c0e18;--border:#13182a;
  --green:#00e5a0;--red:#ff4060;--gold:#f0b429;--blue:#4d9fff;--purple:#a78bfa;
  --text:#e2e8f8;--muted:#3d4f72;}
*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);color:var(--text);font-family:'DM Mono',monospace;min-height:100vh;}
body::before{content:'';position:fixed;inset:0;pointer-events:none;
  background:radial-gradient(ellipse at 20% 20%,rgba(0,229,160,0.03) 0%,transparent 50%),
             radial-gradient(ellipse at 80% 80%,rgba(167,139,250,0.03) 0%,transparent 50%);}
header{position:sticky;top:0;z-index:100;padding:12px 24px;
  border-bottom:1px solid var(--border);background:rgba(5,7,13,.97);
  backdrop-filter:blur(16px);display:flex;align-items:center;
  justify-content:space-between;flex-wrap:wrap;gap:8px;}
.logo{font-family:'Outfit',sans-serif;font-weight:900;font-size:18px;
  display:flex;align-items:center;gap:8px;}
.logo-icon{width:28px;height:28px;border-radius:6px;
  background:linear-gradient(135deg,var(--purple),var(--blue));
  display:flex;align-items:center;justify-content:center;font-size:14px;}
.logo-sub{font-size:9px;color:var(--muted);letter-spacing:2px;}
.hright{display:flex;align-items:center;gap:7px;flex-wrap:wrap;}
.pill{display:flex;align-items:center;gap:5px;font-size:10px;color:var(--muted);
  background:var(--bg3);border:1px solid var(--border);padding:5px 10px;border-radius:20px;}
.dot{width:6px;height:6px;border-radius:50%;animation:pulse 2s infinite;}
.dot.green{background:var(--green);box-shadow:0 0 5px var(--green);}
.dot.grey{background:var(--muted);}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
.btn{font-family:'DM Mono',monospace;font-size:10px;font-weight:500;
  letter-spacing:1.2px;text-transform:uppercase;padding:6px 13px;
  border-radius:5px;border:none;cursor:pointer;transition:all .2s;}
.btn-primary{background:linear-gradient(135deg,var(--purple),var(--blue));color:#fff;font-weight:700;}
.btn-primary:hover{opacity:.85;}
.btn-primary:disabled{opacity:.4;cursor:not-allowed;}
.btn-cfg{background:transparent;color:var(--gold);border:1px solid rgba(240,180,41,0.4);}
.btn-cfg:hover{background:rgba(240,180,41,0.08);}
.btn-tg{background:transparent;color:var(--blue);border:1px solid rgba(77,159,255,0.4);}
.btn-tg:hover{background:rgba(77,159,255,0.08);}
.clock-wrap{display:flex;align-items:center;gap:7px;background:var(--bg3);
  border:1px solid var(--border);padding:6px 12px;border-radius:6px;}
.clock-lbl{font-size:9px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;}
.clock-val{font-family:'Outfit',sans-serif;font-weight:700;font-size:14px;
  color:var(--purple);min-width:50px;text-align:center;}
#prog{display:none;height:2px;background:var(--border);}
#prog-fill{height:100%;background:linear-gradient(90deg,var(--purple),var(--blue));
  transition:width .3s;width:0%;}
#prog-fill.ind{width:30%;animation:ind 1s ease-in-out infinite;}
@keyframes ind{0%{transform:translateX(-200%)}100%{transform:translateX(500%)}}
#prog-lbl{padding:3px 24px;font-size:9px;color:var(--muted);display:none;}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(90px,1fr));
  gap:1px;background:var(--border);border-bottom:1px solid var(--border);}
.stat{background:var(--bg2);padding:10px 14px;}
.stat-lbl{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:3px;}
.stat-val{font-family:'Outfit',sans-serif;font-size:18px;font-weight:700;}
.cg{color:var(--green)}.cr{color:var(--red)}.cb{color:var(--blue)}
.cgold{color:var(--gold)}.cpur{color:var(--purple)}.cmuted{color:var(--muted);font-size:12px!important}
.filters{padding:8px 24px;display:flex;gap:8px;flex-wrap:wrap;
  align-items:center;border-bottom:1px solid var(--border);background:var(--bg2);}
.fl{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;}
.fg{display:flex;gap:3px;flex-wrap:wrap;}
.fc{font-family:'DM Mono',monospace;font-size:9px;background:transparent;
  border:1px solid var(--border);color:var(--muted);padding:3px 9px;
  border-radius:3px;cursor:pointer;transition:all .15s;text-transform:uppercase;}
.fc:hover{color:var(--text);}
.fc.on{border-color:var(--purple);color:var(--purple);background:rgba(167,139,250,0.06);}
.fc.on-g{border-color:var(--green);color:var(--green);background:rgba(0,229,160,0.06);}
.fc.on-r{border-color:var(--red);color:var(--red);background:rgba(255,64,96,0.06);}
.tw{padding:12px 24px;overflow-x:auto;}
table{width:100%;border-collapse:collapse;min-width:1000px;}
thead tr{border-bottom:1px solid var(--border);}
th{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;
  padding:6px 8px;text-align:left;white-space:nowrap;}
tbody tr{border-bottom:1px solid rgba(19,24,42,0.5);transition:background .1s;cursor:pointer;}
tbody tr:hover{background:rgba(167,139,250,0.03);}
td{padding:7px 8px;font-size:11px;white-space:nowrap;}
.pair{font-family:'Outfit',sans-serif;font-weight:700;font-size:14px;}
.pair-sub{font-size:8px;color:var(--muted);margin-top:1px;}
.badge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:4px;
  font-size:9px;font-weight:700;}
.b-bull{background:rgba(0,229,160,0.1);color:var(--green);border:1px solid rgba(0,229,160,0.25);}
.b-bear{background:rgba(255,64,96,0.1);color:var(--red);border:1px solid rgba(255,64,96,0.25);}
.b-sc{background:rgba(167,139,250,0.1);color:var(--purple);border:1px solid rgba(167,139,250,0.25);}
.score-wrap{display:flex;align-items:center;gap:5px;}
.score-bg{width:50px;height:5px;background:var(--border);border-radius:3px;overflow:hidden;}
.score-fg{height:100%;border-radius:3px;}
.score-num{font-family:'Outfit',sans-serif;font-weight:900;font-size:14px;}
.pts-breakdown{display:flex;flex-wrap:wrap;gap:2px;max-width:200px;}
.pt{font-size:8px;padding:1px 4px;border-radius:2px;background:var(--bg3);color:var(--muted);}
.pt.has{background:rgba(167,139,250,0.1);color:var(--purple);}
.empty{text-align:center;padding:60px;color:var(--muted);}
.empty-ico{font-size:36px;margin-bottom:12px;opacity:.2;}
footer{padding:8px 24px;border-top:1px solid var(--border);font-size:9px;
  color:var(--muted);display:flex;justify-content:space-between;flex-wrap:wrap;gap:4px;}
.panel-overlay{display:none;position:fixed;inset:0;z-index:9500;
  background:rgba(0,0,0,0.9);backdrop-filter:blur(6px);
  align-items:flex-start;justify-content:center;overflow-y:auto;padding:20px;}
.panel-box{background:var(--bg2);border:1px solid var(--border);border-radius:10px;
  width:92vw;max-width:560px;overflow:hidden;margin:auto;}
.panel-hdr{display:flex;align-items:center;justify-content:space-between;
  padding:14px 18px;border-bottom:1px solid var(--border);background:var(--bg2);
  position:sticky;top:0;z-index:1;}
.panel-body{padding:18px;}
.p-section{margin-bottom:18px;}
.p-title{font-size:9px;color:var(--muted);text-transform:uppercase;
  letter-spacing:2px;margin-bottom:10px;padding-bottom:6px;
  border-bottom:1px solid var(--border);}
.p-row{display:flex;align-items:center;justify-content:space-between;
  margin-bottom:10px;gap:12px;}
.p-lbl{font-size:10px;color:var(--text);}
.p-sub{font-size:9px;color:var(--muted);margin-top:2px;}
.p-inp{background:var(--bg3);border:1px solid var(--border);border-radius:5px;
  padding:6px 10px;color:var(--gold);font-family:'DM Mono',monospace;
  font-size:11px;font-weight:700;outline:none;text-align:right;width:80px;}
.p-inp:focus{border-color:var(--gold);}
.p-save{width:100%;font-family:'DM Mono',monospace;font-size:10px;font-weight:700;
  letter-spacing:1px;padding:9px;border-radius:5px;border:none;cursor:pointer;
  background:linear-gradient(135deg,var(--purple),var(--blue));color:#fff;margin-top:8px;}
.tog{width:44px;height:24px;border-radius:12px;background:var(--border);
  cursor:pointer;position:relative;transition:.2s;flex-shrink:0;}
.tog-thumb{position:absolute;top:3px;left:3px;width:18px;height:18px;
  background:#fff;border-radius:50%;transition:.2s;}
.tg-inp{width:100%;background:var(--bg3);border:1px solid var(--border);
  border-radius:5px;padding:8px 10px;color:var(--text);
  font-family:'DM Mono',monospace;font-size:10px;outline:none;}
.tg-inp:focus{border-color:var(--blue);}
.tg-btns{display:flex;gap:8px;margin-top:4px;}
.tg-save{flex:1;font-family:'DM Mono',monospace;font-size:10px;font-weight:700;
  padding:8px;border-radius:5px;border:none;cursor:pointer;
  background:linear-gradient(135deg,var(--green),var(--blue));color:#000;}
.tg-test{font-family:'DM Mono',monospace;font-size:10px;padding:8px 14px;
  border-radius:5px;cursor:pointer;background:transparent;
  color:var(--blue);border:1px solid var(--blue);}
.close-btn{font-family:'DM Mono',monospace;font-size:9px;background:var(--bg3);
  color:var(--muted);border:1px solid var(--border);padding:5px 12px;
  border-radius:4px;cursor:pointer;}
.close-btn:hover{color:var(--text);}
.chart-overlay{display:none;position:fixed;inset:0;z-index:9900;
  background:rgba(0,0,0,0.92);backdrop-filter:blur(8px);
  align-items:center;justify-content:center;padding:16px;}
.chart-box{background:var(--bg2);border:1px solid var(--border);border-radius:10px;
  width:96vw;max-width:1100px;max-height:92vh;overflow:hidden;display:flex;flex-direction:column;}
.chart-hdr{display:flex;align-items:center;justify-content:space-between;
  padding:12px 18px;border-bottom:1px solid var(--border);flex-shrink:0;}
.chart-title{font-family:'Outfit',sans-serif;font-weight:900;font-size:20px;}
.chart-meta{display:flex;gap:10px;align-items:center;margin-top:3px;flex-wrap:wrap;}
.chart-meta span{font-size:9px;color:var(--muted);}
.chart-body{flex:1;overflow-y:auto;}
.chart-pane{padding:12px 18px;}
.pane-lbl{font-size:8px;color:var(--muted);text-transform:uppercase;
  letter-spacing:2px;margin-bottom:6px;}
::-webkit-scrollbar{width:3px;height:3px;}
::-webkit-scrollbar-track{background:var(--bg);}
::-webkit-scrollbar-thumb{background:var(--border);}
</style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">&#8593;</div>
    RSI Divergence Scanner
    <span class="logo-sub">4H &middot; BINANCE PERPS</span>
  </div>
  <div class="hright">
    <div class="pill"><div class="dot grey" id="dot"></div><span id="st-lbl">READY</span></div>
    <div class="clock-wrap">
      <span class="clock-lbl">NEXT 4H</span>
      <span class="clock-val" id="clock-val">—</span>
    </div>
    <button class="btn btn-cfg" onclick="openSettings()">&#9881; SETTINGS</button>
    <button class="btn btn-tg"  onclick="openTg()">&#128172; TG ALERTS</button>
    <button class="btn btn-primary" id="scan-btn" onclick="manualScan()">&#9889; SCAN NOW</button>
  </div>
</header>
<div id="prog"><div id="prog-fill"></div></div>
<div id="prog-lbl"></div>
<div class="stats">
  <div class="stat"><div class="stat-lbl">Total</div><div class="stat-val cgold" id="s-total">—</div></div>
  <div class="stat"><div class="stat-lbl">&#129001; Bullish</div><div class="stat-val cg" id="s-bull">—</div></div>
  <div class="stat"><div class="stat-lbl">&#128308; Bearish</div><div class="stat-val cr" id="s-bear">—</div></div>
  <div class="stat"><div class="stat-lbl">SC1 Bull Classic</div><div class="stat-val cpur" id="s-sc1">—</div></div>
  <div class="stat"><div class="stat-lbl">SC2 Bull Consol</div><div class="stat-val cpur" id="s-sc2">—</div></div>
  <div class="stat"><div class="stat-lbl">SC3 Bear Classic</div><div class="stat-val cpur" id="s-sc3">—</div></div>
  <div class="stat"><div class="stat-lbl">SC4 Bear Consol</div><div class="stat-val cpur" id="s-sc4">—</div></div>
  <div class="stat"><div class="stat-lbl">Scan #</div><div class="stat-val cmuted" id="s-count">—</div></div>
  <div class="stat"><div class="stat-lbl">Last Scan</div><div class="stat-val cmuted" id="s-time">—</div></div>
</div>
<div class="filters">
  <span class="fl">Type:</span>
  <div class="fg">
    <button class="fc on" id="f-all"  onclick="setFilt('type','all',this)">ALL</button>
    <button class="fc"    id="f-bull" onclick="setFilt('type','bullish',this)">&#129001; BULLISH</button>
    <button class="fc"    id="f-bear" onclick="setFilt('type','bearish',this)">&#128308; BEARISH</button>
  </div>
  <span class="fl">Scenario:</span>
  <div class="fg">
    <button class="fc on" id="f-sc-all" onclick="setFilt('sc','all',this)">ALL</button>
    <button class="fc"    id="f-sc1"    onclick="setFilt('sc','1',this)">SC1</button>
    <button class="fc"    id="f-sc2"    onclick="setFilt('sc','2',this)">SC2</button>
    <button class="fc"    id="f-sc3"    onclick="setFilt('sc','3',this)">SC3</button>
    <button class="fc"    id="f-sc4"    onclick="setFilt('sc','4',this)">SC4</button>
  </div>
  <span class="fl">Recency:</span>
  <div class="fg">
    <button class="fc on" id="r-all"   onclick="setFilt('age','all',this)">ALL</button>
    <button class="fc"    id="r-fresh" onclick="setFilt('age','fresh',this)">&#128293; 0-3</button>
    <button class="fc"    id="r-rec"   onclick="setFilt('age','recent',this)">&#128336; 4-8</button>
    <button class="fc"    id="r-old"   onclick="setFilt('age','older',this)">9+</button>
  </div>
</div>
<div class="tw">
  <table>
    <thead><tr>
      <th>PAIR</th><th>SCENARIO</th><th>SCORE</th><th>TOUCHES</th>
      <th>BARS AGO</th><th>RSI OLDEST</th><th>RSI NEWEST</th>
      <th>RSI DIFF</th><th>PRICE MOVE</th><th>SLOPE</th>
      <th>SCORE BREAKDOWN</th><th>24H CHG</th>
    </tr></thead>
    <tbody id="tb">
      <tr><td colspan="12">
        <div class="empty">
          <div class="empty-ico">&#8593;</div>
          <div>Click SCAN NOW to detect RSI divergences</div>
          <div style="margin-top:8px;font-size:10px;color:var(--muted);">
            4H &middot; RSI 14 &middot; 4 scenarios &middot; Weighted scoring
          </div>
        </div>
      </td></tr>
    </tbody>
  </table>
</div>
<footer>
  <span>RSI Divergence Scanner &middot; 4H &middot; 4 Scenarios &middot; Weighted Score 0-100 &middot; Fires at 4H close</span>
  <span id="ft"></span>
</footer>

<!-- Settings panel -->
<div id="set-panel" class="panel-overlay">
  <div class="panel-box">
    <div class="panel-hdr">
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:18px;">&#9881;</span>
        <span style="font-family:Outfit,sans-serif;font-weight:700;font-size:16px;">Settings</span>
      </div>
      <button class="close-btn" onclick="document.getElementById('set-panel').style.display='none'">X CLOSE</button>
    </div>
    <div class="panel-body">
      <div class="p-section">
        <div class="p-title">RSI Settings</div>
        <div class="p-row">
          <div><div class="p-lbl">RSI Period</div><div class="p-sub">Standard is 14</div></div>
          <input class="p-inp" id="s-rsi" type="number" min="2" max="50" value="14">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Pivot Detection</div>
        <div class="p-row">
          <div><div class="p-lbl">Fractal Periods</div><div class="p-sub">Bars each side for pivot</div></div>
          <input class="p-inp" id="s-fp" type="number" min="1" max="5" value="2">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Lookback Bars</div><div class="p-sub">Total bars to search</div></div>
          <input class="p-inp" id="s-lb" type="number" min="20" max="150" value="60">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Min Pivot Distance</div><div class="p-sub">Min bars between pivots</div></div>
          <input class="p-inp" id="s-mn" type="number" min="2" max="30" value="8">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Max Pivot Distance</div><div class="p-sub">Max bars between pivots</div></div>
          <input class="p-inp" id="s-mx" type="number" min="10" max="60" value="30">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Quality Filters</div>
        <div class="p-row">
          <div><div class="p-lbl">Min RSI Difference</div><div class="p-sub">Min RSI points between pivots</div></div>
          <input class="p-inp" id="s-rd" type="number" min="1" max="30" step="0.5" value="3">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Recency Bars</div><div class="p-sub">Second pivot within last N bars</div></div>
          <input class="p-inp" id="s-rec" type="number" min="1" max="20" value="10">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Consolidation Detection</div>
        <div class="p-row">
          <div><div class="p-lbl">Max Range % (consolidation)</div><div class="p-sub">Price range < X% = consolidation</div></div>
          <input class="p-inp" id="s-cr" type="number" min="2" max="20" step="0.5" value="8">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Max Slope % (consolidation)</div><div class="p-sub">Price slope < X% = consolidation</div></div>
          <input class="p-inp" id="s-cs" type="number" min="1" max="10" step="0.5" value="3">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Score Thresholds</div>
        <div class="p-row">
          <div><div class="p-lbl">Min Score to Show</div></div>
          <input class="p-inp" id="s-ms" type="number" min="10" max="90" step="5" value="40">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Alert Score (Telegram)</div></div>
          <input class="p-inp" id="s-as" type="number" min="10" max="100" step="5" value="70">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Min 24H Volume (M)</div></div>
          <input class="p-inp" id="s-mv" type="number" min="0" step="1" value="2">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Scenario Toggles</div>
        <div class="p-row">
          <div><div class="p-lbl">SC1 — Bullish Classic</div></div>
          <div class="tog" id="tog-sc1" onclick="toggleSc('sc1')"><div class="tog-thumb" id="th-sc1"></div></div>
        </div>
        <div class="p-row">
          <div><div class="p-lbl">SC2 — Bullish Consolidation</div></div>
          <div class="tog" id="tog-sc2" onclick="toggleSc('sc2')"><div class="tog-thumb" id="th-sc2"></div></div>
        </div>
        <div class="p-row">
          <div><div class="p-lbl">SC3 — Bearish Classic</div></div>
          <div class="tog" id="tog-sc3" onclick="toggleSc('sc3')"><div class="tog-thumb" id="th-sc3"></div></div>
        </div>
        <div class="p-row">
          <div><div class="p-lbl">SC4 — Bearish Consolidation</div></div>
          <div class="tog" id="tog-sc4" onclick="toggleSc('sc4')"><div class="tog-thumb" id="th-sc4"></div></div>
        </div>
      </div>
      <div id="set-status" style="font-size:10px;color:var(--muted);min-height:14px;margin-bottom:8px;"></div>
      <button class="p-save" onclick="saveSettings()">SAVE SETTINGS</button>
    </div>
  </div>
</div>

<!-- TG panel -->
<div id="tg-panel" class="panel-overlay">
  <div class="panel-box" style="max-width:440px;">
    <div class="panel-hdr">
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:18px;">&#128172;</span>
        <span style="font-family:Outfit,sans-serif;font-weight:700;font-size:16px;">Telegram Alerts</span>
      </div>
      <button class="close-btn" onclick="document.getElementById('tg-panel').style.display='none'">X CLOSE</button>
    </div>
    <div class="panel-body">
      <div style="display:flex;align-items:center;justify-content:space-between;
        background:var(--bg3);border:1px solid var(--border);border-radius:6px;
        padding:12px 14px;margin-bottom:12px;">
        <div>
          <div style="font-size:11px;font-weight:600;letter-spacing:1px;">ALERTS ENABLED</div>
          <div style="font-size:9px;color:var(--muted);margin-top:2px;">Fires at 4H scan</div>
        </div>
        <div class="tog" id="tg-tog" onclick="tgToggle()"><div class="tog-thumb" id="tg-thumb"></div></div>
      </div>
      <div style="margin-bottom:10px;">
        <div style="font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:5px;">Bot Token</div>
        <input class="tg-inp" id="tg-token" type="text" placeholder="1234567890:AAAA...">
      </div>
      <div style="margin-bottom:10px;">
        <div style="font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:5px;">Chat ID</div>
        <input class="tg-inp" id="tg-chatid" type="text" placeholder="1234567890">
      </div>
      <div style="margin-bottom:10px;">
        <div style="font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:5px;">Alert Only When Bars Ago &le;</div>
        <div style="display:flex;align-items:center;gap:10px;">
          <input class="tg-inp" id="tg-maxbars" type="number" min="0" max="10" value="3"
            style="width:70px;text-align:center;color:var(--gold);font-weight:700;">
          <span style="font-size:9px;color:var(--muted)">default 3 (last 4 candles)</span>
        </div>
      </div>
      <div id="tg-status" style="font-size:10px;color:var(--muted);min-height:16px;margin-bottom:12px;"></div>
      <div class="tg-btns">
        <button class="tg-save" onclick="tgSave()">SAVE</button>
        <button class="tg-test" onclick="tgTest()">TEST</button>
      </div>
    </div>
  </div>
</div>

<!-- Chart modal -->
<div id="chart-overlay" class="chart-overlay">
  <div class="chart-box">
    <div class="chart-hdr">
      <div>
        <div class="chart-title" id="chart-sym">—</div>
        <div class="chart-meta">
          <span id="chart-sc-lbl">—</span>
          <span id="chart-score-lbl">—</span>
          <span id="chart-touch-lbl">—</span>
          <span style="color:var(--muted)">4H &middot; RSI 14</span>
        </div>
      </div>
      <button class="close-btn" onclick="closeChart()">&#10005; CLOSE</button>
    </div>
    <div class="chart-body">
      <div id="chart-loading" style="text-align:center;padding:60px;color:var(--muted);">Loading...</div>
      <div id="chart-content" style="display:none">
        <div class="chart-pane">
          <div class="pane-lbl">Price &middot; 4H Candles</div>
          <div id="chart-main" style="height:300px;"></div>
        </div>
        <div class="chart-pane" style="border-top:1px solid var(--border)">
          <div class="pane-lbl">RSI 14 &middot; Divergence Lines</div>
          <div id="chart-rsi" style="height:180px;"></div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
var signals=[], filt={type:'all',sc:'all',age:'all'};
var scanPoll=null, isScanning=false;
var mainChart=null, rsiChart=null;
var scToggles={sc1:true,sc2:true,sc3:true,sc4:true};

function updateClock(nextIn){
  var r=Math.max(0,Math.round(nextIn));
  var h=Math.floor(r/3600),m=Math.floor((r%3600)/60);
  document.getElementById('clock-val').textContent=h+'h '+(m<10?'0':'')+m+'m';
}
setInterval(function(){
  if(!isScanning) fetch('/api/status')
    .then(function(r){return r.json();})
    .then(function(d){updateClock(d.next_scan_in||0);})
    .catch(function(){});
},5000);

function manualScan(){
  if(isScanning) return;
  isScanning=true;
  document.getElementById('scan-btn').disabled=true;
  document.getElementById('scan-btn').textContent='SCANNING...';
  document.getElementById('prog').style.display='block';
  document.getElementById('prog-lbl').style.display='block';
  document.getElementById('prog-fill').style.width='0%';
  document.getElementById('prog-fill').classList.add('ind');
  fetch('/api/scan',{method:'POST'})
    .then(function(r){return r.json();})
    .then(function(){if(!scanPoll) scanPoll=setInterval(poll,1500);})
    .catch(function(){stopPoll();});
}
function stopPoll(){
  clearInterval(scanPoll);scanPoll=null;isScanning=false;
  document.getElementById('scan-btn').disabled=false;
  document.getElementById('scan-btn').textContent='SCAN NOW';
  setTimeout(function(){
    document.getElementById('prog').style.display='none';
    document.getElementById('prog-lbl').style.display='none';
    document.getElementById('prog-fill').classList.remove('ind');
  },2000);
}
function poll(){
  fetch('/api/status').then(function(r){return r.json();}).then(function(d){
    document.getElementById('prog-fill').classList.remove('ind');
    document.getElementById('prog-fill').style.width=(d.progress_pct||0)+'%';
    document.getElementById('prog-lbl').textContent=d.progress||'';
    document.getElementById('dot').className='dot '+(d.scanning?'green':'grey');
    document.getElementById('st-lbl').textContent=d.scanning?'SCANNING':'LIVE';
    updateClock(d.next_scan_in||0);
    updateStats(d.stats,d.last_scan,d.scan_count);
    if(!d.scanning&&d.last_scan){signals=d.signals||[];render();stopPoll();}
    if(d.error&&!d.scanning) stopPoll();
  }).catch(function(){});
}
setInterval(function(){
  fetch('/api/status').then(function(r){return r.json();}).then(function(d){
    updateClock(d.next_scan_in||0);
    updateStats(d.stats,d.last_scan,d.scan_count);
    if(d.scanning&&!scanPoll){
      isScanning=true;
      document.getElementById('prog').style.display='block';
      document.getElementById('prog-lbl').style.display='block';
      document.getElementById('prog-fill').classList.add('ind');
      document.getElementById('scan-btn').disabled=true;
      document.getElementById('scan-btn').textContent='SCANNING...';
      scanPoll=setInterval(poll,1500);
    }
  }).catch(function(){});
},5000);

function updateStats(st,ls,cnt){
  if(!st) return;
  document.getElementById('s-total').textContent=st.total||0;
  document.getElementById('s-bull').textContent=st.bullish||0;
  document.getElementById('s-bear').textContent=st.bearish||0;
  document.getElementById('s-sc1').textContent=st.sc1||0;
  document.getElementById('s-sc2').textContent=st.sc2||0;
  document.getElementById('s-sc3').textContent=st.sc3||0;
  document.getElementById('s-sc4').textContent=st.sc4||0;
  document.getElementById('s-count').textContent=cnt?'#'+cnt:'—';
  document.getElementById('s-time').textContent=ls?ls.slice(11):'—';
}

function fp(p){
  if(p==null) return '—';
  var a=Math.abs(p);
  if(a<0.0001) return p.toFixed(6);
  if(a<0.01)   return p.toFixed(5);
  if(a<1)      return p.toFixed(4);
  if(a<10)     return p.toFixed(3);
  if(a<1000)   return p.toFixed(2);
  return p.toFixed(0);
}

function setFilt(dim,val,el){
  filt[dim]=val;
  var groups={type:['f-all','f-bull','f-bear'],
              sc:['f-sc-all','f-sc1','f-sc2','f-sc3','f-sc4'],
              age:['r-all','r-fresh','r-rec','r-old']};
  groups[dim].forEach(function(id){document.getElementById(id).className='fc';});
  var cls=val==='bullish'?'on-g':val==='bearish'?'on-r':'on';
  el.className='fc '+cls;
  render();
}

function render(){
  var data=signals.slice();
  if(filt.type==='bullish') data=data.filter(function(s){return s.direction==='bullish';});
  if(filt.type==='bearish') data=data.filter(function(s){return s.direction==='bearish';});
  if(filt.sc!=='all') data=data.filter(function(s){return s.scenario===parseInt(filt.sc);});
  if(filt.age==='fresh')  data=data.filter(function(s){return s.bars_ago<=3;});
  if(filt.age==='recent') data=data.filter(function(s){return s.bars_ago>=4&&s.bars_ago<=8;});
  if(filt.age==='older')  data=data.filter(function(s){return s.bars_ago>=9;});

  var tb=document.getElementById('tb');
  if(!data.length){
    tb.innerHTML='<tr><td colspan="12"><div class="empty">'
      +'<div class="empty-ico">&#8593;</div><div>No divergences found</div></div></td></tr>';
    return;
  }

  tb.innerHTML=data.slice(0,100).map(function(s){
    var isBull=s.direction==='bullish';
    var dirBadge=isBull
      ?'<span class="badge b-bull">&#129001; '+s.label+'</span>'
      :'<span class="badge b-bear">&#128308; '+s.label+'</span>';
    var scoreCol=s.score>=80?'var(--green)':s.score>=60?'var(--blue)':'var(--muted)';
    var stars=s.score>=80?'&#129001;&#129001;&#129001;':s.score>=60?'&#129001;&#129001;':'&#129001;';
    var scoreBar='<div class="score-wrap">'
      +'<div class="score-bg"><div class="score-fg" style="width:'+s.score+'%;background:'+scoreCol+'"></div></div>'
      +'<span class="score-num" style="color:'+scoreCol+'">'+s.score+'</span>'
      +'</div>';
    var barsCol=s.bars_ago<=3?'var(--green)':s.bars_ago<=8?'var(--gold)':'var(--muted)';
    var barsIco=s.bars_ago<=3?'&#128293; ':'';
    var dCol=s.chg_24h>=0?'var(--green)':'var(--red)';
    var slopeCol=s.slope==='up'?'var(--green)':s.slope==='down'?'var(--red)':'var(--muted)';
    var slopeIco=s.slope==='up'?'&#8593;':s.slope==='down'?'&#8595;':'&#8212;';
    var rsiDiffCol=isBull?'var(--green)':'var(--red)';
    var priceMoveStr='—';
    if(s.price_oldest&&s.price_newest){
      var pm=Math.abs((s.price_newest-s.price_oldest)/s.price_oldest*100);
      priceMoveStr=(isBull?'-':'+')+(pm).toFixed(1)+'%';
    }
    // Score breakdown
    var pts=s.pts||{};
    var pkeys=[['touch',pts.touch],['rsi_lvl',pts.rsi_lvl],['rsi_dif',pts.rsi_dif],
               ['price',pts.price],['vol',pts.vol],['rec',pts.rec],['trend',pts.trend],
               ['consol',pts.consol]];
    var breakdown=pkeys.filter(function(p){return p[1];}).map(function(p){
      return '<span class="pt has">'+p[0]+':'+p[1]+'</span>';
    }).join('');

    return '<tr data-sym="'+s.symbol+'" onclick="openChart(this.dataset.sym)">'
      +'<td><div class="pair">'+s.base+'/USDT</div>'
        +'<div class="pair-sub">$'+s.vol_24m+'M &middot; '+s.mode+'</div></td>'
      +'<td>'+dirBadge+'</td>'
      +'<td>'+scoreBar+'</td>'
      +'<td style="color:var(--purple);font-weight:700;font-family:Outfit,sans-serif">'+s.n_touches+'</td>'
      +'<td style="color:'+barsCol+';font-weight:700">'+barsIco+s.bars_ago+'</td>'
      +'<td style="color:var(--muted)">'+s.rsi_oldest+'</td>'
      +'<td style="color:'+rsiDiffCol+'">'+s.rsi_newest+'</td>'
      +'<td style="color:'+rsiDiffCol+';font-weight:700">'+(isBull?'+':'-')+s.rsi_diff+'</td>'
      +'<td style="color:'+(isBull?'var(--red)':'var(--green)')+'">'+priceMoveStr+'</td>'
      +'<td style="color:'+slopeCol+'">'+slopeIco+' '+s.slope+'</td>'
      +'<td><div class="pts-breakdown">'+breakdown+'</div></td>'
      +'<td style="color:'+dCol+'">'+(s.chg_24h>=0?'+':'')+s.chg_24h+'%</td>'
      +'</tr>';
  }).join('');
}

// ── Chart ──────────────────────────────────────────────────────────────────
function openChart(symbol){
  document.getElementById('chart-overlay').style.display='flex';
  document.getElementById('chart-loading').style.display='block';
  document.getElementById('chart-content').style.display='none';
  document.getElementById('chart-sym').textContent=symbol.replace('USDT','/USDT');

  var sigs=signals.filter(function(s){return s.symbol===symbol;});
  var sig=sigs[0]||{};
  document.getElementById('chart-sc-lbl').textContent=sig.label||'';
  document.getElementById('chart-score-lbl').textContent=sig.score?'Score: '+sig.score+'/100':'';
  document.getElementById('chart-touch-lbl').textContent=sig.n_touches?'Touches: '+sig.n_touches:'';

  if(mainChart){mainChart.remove();mainChart=null;}
  if(rsiChart){rsiChart.remove();rsiChart=null;}

  fetch('/api/chart/'+symbol)
    .then(function(r){return r.json();})
    .then(function(d){
      if(d.error){document.getElementById('chart-loading').textContent='Error: '+d.error;return;}
      document.getElementById('chart-loading').style.display='none';
      document.getElementById('chart-content').style.display='block';
      // Small delay to allow DOM to render before measuring widths
      setTimeout(function(){ drawCharts(d, sigs); }, 50);
    })
    .catch(function(e){document.getElementById('chart-loading').textContent='Failed: '+e;});
}

function cOpts(elId, h){
  var el=document.getElementById(elId);
  var parent=el?el.parentElement:null;
  var w=parent&&parent.clientWidth>0?parent.clientWidth-36:800;
  return {width:w,height:h,
    layout:{background:{color:'transparent'},textColor:'#3d4f72'},
    grid:{vertLines:{color:'rgba(19,24,42,0.8)'},horzLines:{color:'rgba(19,24,42,0.8)'}},
    crosshair:{mode:1},
    rightPriceScale:{borderColor:'#13182a'},
    timeScale:{borderColor:'#13182a',timeVisible:true}};
}

function drawCharts(d, sigs){
  // Get width from chart-box which is always visible
  var box=document.querySelector('.chart-box');
  var w=box?(box.clientWidth-36):800;

  // ── Price chart ─────────────────────────────────────────────────────────
  mainChart=LightweightCharts.createChart(document.getElementById('chart-main'),
    Object.assign(cOpts('chart-main',300),{width:w}));
  var candles=mainChart.addCandlestickSeries({
    upColor:'#00e5a0',downColor:'#ff4060',
    borderUpColor:'#00e5a0',borderDownColor:'#ff4060',
    wickUpColor:'#00e5a0',wickDownColor:'#ff4060'});
  candles.setData(d.ohlcv.map(function(b){
    return {time:b.t,open:b.o,high:b.h,low:b.l,close:b.c};}));

  // Draw price pivot lines for each signal
  sigs.forEach(function(sig){
    if(!sig.pivot_idxs||sig.pivot_idxs.length<2) return;
    var isBull=sig.direction==='bullish';
    var col=isBull?'rgba(0,229,160,0.8)':'rgba(255,64,96,0.8)';
    var pLine=mainChart.addLineSeries({color:col,lineWidth:2,lineStyle:0,
      priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});
    var pts=sig.pivot_idxs.map(function(idx){
      var b=d.ohlcv[idx];
      if(!b) return null;
      return {time:b.t,value:isBull?d.ohlcv[idx].l:d.ohlcv[idx].h};
    }).filter(Boolean);
    if(pts.length>=2) pLine.setData(pts);
    // Markers
    var markers=pts.map(function(p,i){
      return {time:p.time,position:isBull?'belowBar':'aboveBar',
        color:col,shape:'circle',size:1};
    });
    if(markers.length) pLine.setMarkers(markers);
  });
  mainChart.timeScale().fitContent();

  // ── RSI chart ────────────────────────────────────────────────────────────
  rsiChart=LightweightCharts.createChart(document.getElementById('chart-rsi'),{
    width:w, height:180,
    layout:{background:{color:'transparent'},textColor:'#3d4f72'},
    grid:{vertLines:{color:'rgba(19,24,42,0.8)'},horzLines:{color:'rgba(19,24,42,0.8)'}},
    crosshair:{mode:1},
    rightPriceScale:{borderColor:'#13182a',scaleMargins:{top:0.1,bottom:0.1}},
    timeScale:{borderColor:'#13182a',timeVisible:true},
  });
  var rsiLine=rsiChart.addLineSeries({color:'#a78bfa',lineWidth:1.5,
    priceLineVisible:false,lastValueVisible:true});
  rsiLine.setData(d.rsi);
  // Overbought/oversold lines
  rsiLine.createPriceLine({price:70,color:'rgba(255,64,96,0.4)',lineWidth:1,lineStyle:2,
    axisLabelVisible:true,title:'70'});
  rsiLine.createPriceLine({price:30,color:'rgba(0,229,160,0.4)',lineWidth:1,lineStyle:2,
    axisLabelVisible:true,title:'30'});
  rsiLine.createPriceLine({price:50,color:'rgba(61,79,114,0.3)',lineWidth:1,lineStyle:0,
    axisLabelVisible:false});

  // Draw RSI divergence lines
  sigs.forEach(function(sig){
    if(!sig.rsi_idxs||sig.rsi_idxs.length<2) return;
    var isBull=sig.direction==='bullish';
    var col=isBull?'rgba(0,229,160,0.8)':'rgba(255,64,96,0.8)';
    var rDivLine=rsiChart.addLineSeries({color:col,lineWidth:2,lineStyle:0,
      priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});
    var pts=sig.rsi_idxs.map(function(idx,ii){
      var b=d.ohlcv[idx];
      if(!b||sig.rsi_vals[ii]==null) return null;
      return {time:b.t,value:sig.rsi_vals[ii]};
    }).filter(Boolean);
    if(pts.length>=2) rDivLine.setData(pts);
    // Circle markers at each RSI pivot
    if(pts.length>=1){
      rDivLine.setMarkers(pts.map(function(p){
        return {time:p.time,
          position:isBull?'belowBar':'aboveBar',
          color:col,shape:'circle',size:2};
      }));
    }
  });

  // Sync time scales
  mainChart.timeScale().subscribeVisibleLogicalRangeChange(function(range){
    if(range) rsiChart.timeScale().setVisibleLogicalRange(range);
  });
  rsiChart.timeScale().subscribeVisibleLogicalRangeChange(function(range){
    if(range) mainChart.timeScale().setVisibleLogicalRange(range);
  });
}

function closeChart(){
  document.getElementById('chart-overlay').style.display='none';
  if(mainChart){mainChart.remove();mainChart=null;}
  if(rsiChart){rsiChart.remove();rsiChart=null;}
}
document.getElementById('chart-overlay').addEventListener('click',function(e){
  if(e.target===this) closeChart();
});

// ── Settings ───────────────────────────────────────────────────────────────
function setSc(k,on){
  scToggles[k]=on;
  document.getElementById('tog-'+k).style.background=on?'var(--green)':'var(--border)';
  document.getElementById('th-'+k).style.left=on?'23px':'3px';
}
function toggleSc(k){setSc(k,!scToggles[k]);}

function openSettings(){
  fetch('/api/settings').then(function(r){return r.json();}).then(function(d){
    document.getElementById('s-rsi').value=d.rsi_period||14;
    document.getElementById('s-fp').value=d.fractal_periods||2;
    document.getElementById('s-lb').value=d.lookback_bars||60;
    document.getElementById('s-mn').value=d.min_pivot_dist||8;
    document.getElementById('s-mx').value=d.max_pivot_dist||30;
    document.getElementById('s-rd').value=d.min_rsi_diff||3;
    document.getElementById('s-rec').value=d.recency_bars||10;
    document.getElementById('s-cr').value=d.consol_range_pct||8;
    document.getElementById('s-cs').value=d.consol_slope_pct||3;
    document.getElementById('s-ms').value=d.min_score||40;
    document.getElementById('s-as').value=d.alert_score||70;
    document.getElementById('s-mv').value=d.min_vol_24m||2;
    setSc('sc1',d.sc1_enabled!==false);
    setSc('sc2',d.sc2_enabled!==false);
    setSc('sc3',d.sc3_enabled!==false);
    setSc('sc4',d.sc4_enabled!==false);
    document.getElementById('set-status').textContent='';
    document.getElementById('set-panel').style.display='flex';
  });
}
function saveSettings(){
  var data={
    rsi_period:       parseInt(document.getElementById('s-rsi').value)||14,
    fractal_periods:  parseInt(document.getElementById('s-fp').value)||2,
    lookback_bars:    parseInt(document.getElementById('s-lb').value)||60,
    min_pivot_dist:   parseInt(document.getElementById('s-mn').value)||8,
    max_pivot_dist:   parseInt(document.getElementById('s-mx').value)||30,
    min_rsi_diff:     parseFloat(document.getElementById('s-rd').value)||3,
    recency_bars:     parseInt(document.getElementById('s-rec').value)||10,
    consol_range_pct: parseFloat(document.getElementById('s-cr').value)||8,
    consol_slope_pct: parseFloat(document.getElementById('s-cs').value)||3,
    min_score:        parseInt(document.getElementById('s-ms').value)||40,
    alert_score:      parseInt(document.getElementById('s-as').value)||70,
    min_vol_24m:      parseFloat(document.getElementById('s-mv').value)||2,
    sc1_enabled: scToggles.sc1, sc2_enabled: scToggles.sc2,
    sc3_enabled: scToggles.sc3, sc4_enabled: scToggles.sc4,
  };
  fetch('/api/settings',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(data)})
  .then(function(r){return r.json();}).then(function(){
    var el=document.getElementById('set-status');
    el.textContent='Saved!';el.style.color='var(--green)';
    setTimeout(function(){el.textContent='';},2000);
  });
}

// ── Telegram ───────────────────────────────────────────────────────────────
var tgEnabled=false;
function openTg(){
  fetch('/api/telegram/config').then(function(r){return r.json();}).then(function(d){
    tgEnabled=d.enabled;
    document.getElementById('tg-token').value=d.token||'';
    document.getElementById('tg-chatid').value=d.chat_id||'';
    setTgTog(tgEnabled);
    fetch('/api/settings').then(function(r2){return r2.json();}).then(function(s){
      document.getElementById('tg-maxbars').value=s.alert_max_bars_ago||3;
    });
    document.getElementById('tg-status').textContent='';
    document.getElementById('tg-panel').style.display='flex';
  });
}
function setTgTog(on){
  tgEnabled=on;
  document.getElementById('tg-tog').style.background=on?'var(--green)':'var(--border)';
  document.getElementById('tg-thumb').style.left=on?'23px':'3px';
}
function tgToggle(){setTgTog(!tgEnabled);}
function tgSave(){
  fetch('/api/telegram/config',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({enabled:tgEnabled,
      token:document.getElementById('tg-token').value.trim(),
      chat_id:document.getElementById('tg-chatid').value.trim()})});
  var maxBars=parseInt(document.getElementById('tg-maxbars').value)||3;
  fetch('/api/settings',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({alert_max_bars_ago:maxBars})})
  .then(function(r){return r.json();}).then(function(){
    var el=document.getElementById('tg-status');
    el.textContent='Saved!';el.style.color='var(--green)';
    setTimeout(function(){el.textContent='';},2000);
  });
}
function tgTest(){
  tgSave();
  setTimeout(function(){
    var el=document.getElementById('tg-status');
    el.textContent='Sending...';el.style.color='var(--muted)';
    fetch('/api/telegram/test',{method:'POST'})
      .then(function(r){return r.json();}).then(function(d){
        el.textContent=d.status==='ok'?'Sent! '+d.msg:'Error: '+d.msg;
        el.style.color=d.status==='ok'?'var(--green)':'var(--red)';
      });
  },400);
}

document.addEventListener('keydown',function(e){
  if(e.key==='Escape'){
    closeChart();
    document.getElementById('set-panel').style.display='none';
    document.getElementById('tg-panel').style.display='none';
  }
});
setInterval(function(){
  document.getElementById('ft').textContent=new Date().toLocaleString();
},1000);
</script>
</body>
</html>"""

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  RSI Divergence Scanner")
    print("  4H candles · RSI 14 · 4 Scenarios")
    print("  Scoring 0-100 with weighted RSI level")
    print("  Open → http://localhost:5014")
    print("="*60 + "\n")
    port = int(os.environ.get("PORT", 5014))
    app.run(debug=False, host="0.0.0.0", port=port, threaded=True)
