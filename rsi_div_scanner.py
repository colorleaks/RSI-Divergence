"""
RSI Slope Divergence Scanner — 4H Timeframe
Detects divergence between price slope and RSI slope over last N bars.

Logic:
  Fit linear regression through last 100 bars of:
    1. Price (close) → price_slope_pct
    2. RSI(14)       → rsi_slope_pct

  Bullish: price slope flat/slight (+) but RSI slope aggressively up
  Bearish: price slope flat/slight (-) but RSI slope aggressively down

  Strength based on divergence ratio and RSI aggressiveness.

Run:  python rsi_slope_scanner.py
Open: http://localhost:5015
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
    if "enabled" in d: tg_cfg["enabled"] = bool(d["enabled"])
    if "token"   in d: tg_cfg["token"]   = str(d["token"]).strip()
    if "chat_id" in d: tg_cfg["chat_id"] = str(d["chat_id"]).strip()
    return jsonify({"status": "ok"})

@app.route("/api/telegram/test", methods=["POST"])
def tg_test():
    try:
        url = f"https://api.telegram.org/bot{tg_cfg['token']}/sendMessage"
        r = SESSION.post(url, json={"chat_id": tg_cfg["chat_id"],
            "text": "✅ <b>RSI Slope Scanner connected!</b>",
            "parse_mode": "HTML"}, timeout=8)
        if r.status_code == 200:
            return jsonify({"status": "ok", "msg": "Test sent!"})
        return jsonify({"status": "error",
                        "msg": r.json().get("description", "Failed")})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})

# ── Settings ───────────────────────────────────────────────────────────────
settings = {
    # Core
    "rsi_period":           14,
    "lookback_bars":        100,
    "min_vol_24m":          2.0,
    # Price slope range for flat/slight
    "price_slope_bull_min": -3.0,
    "price_slope_bull_max":  5.0,
    "price_slope_bear_min": -5.0,
    "price_slope_bear_max":  3.0,
    # RSI aggressiveness threshold
    "rsi_slope_bull_min":   10.0,
    "rsi_slope_bear_max":  -10.0,
    # Divergence ratio: RSI must move at least N× faster than price
    "min_div_ratio":         2.0,
    # Quality
    "min_r2":                0.3,
    "min_score":             30,
    "alert_score":           60,
    "alert_max_bars_ago":    3,
    # Toggles
    "bullish_enabled":       True,
    "bearish_enabled":       True,
    "ema50_filter":          True,
}

@app.route("/api/settings", methods=["GET"])
def settings_get(): return jsonify(settings)

@app.route("/api/settings", methods=["POST"])
def settings_set():
    d = request.get_json() or {}
    int_keys   = ["rsi_period", "lookback_bars", "alert_max_bars_ago", "min_score", "alert_score"]
    float_keys = ["min_vol_24m", "price_slope_bull_min", "price_slope_bull_max",
                  "price_slope_bear_min", "price_slope_bear_max",
                  "rsi_slope_bull_min", "rsi_slope_bear_max",
                  "min_div_ratio", "min_r2"]
    bool_keys  = ["bullish_enabled", "bearish_enabled", "ema50_filter"]
    for k in int_keys:
        if k in d: settings[k] = max(1, int(d[k]))
    for k in float_keys:
        if k in d: settings[k] = float(d[k])
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

# ── Timing ─────────────────────────────────────────────────────────────────
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
    now_ms  = int(time.time() * 1000)
    last_ct = int(klines[-1][6])
    return len(klines) - 1 if last_ct < now_ms else len(klines) - 2

def calc_rsi(closes, period=14):
    """Full RSI series using Wilder smoothing."""
    n = len(closes)
    if n < period + 1:
        return [None] * n
    gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, n)]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, n)]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    def rv(ag, al):
        return 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    out = [None] * period
    out.append(rv(avg_g, avg_l))
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        out.append(rv(avg_g, avg_l))
    return out

def linear_regression(values):
    """
    Fit y = mx + b through values.
    Returns (slope, intercept, r2, start_val, end_val, slope_pct)
    slope_pct = (end - start) / |start| * 100
    """
    n = len(values)
    if n < 2:
        return (0, values[0] if values else 0, 0, 0, 0, 0)
    xs = list(range(n))
    xm = sum(xs) / n
    ym = sum(values) / n
    ss_xx = sum((x - xm) ** 2 for x in xs)
    ss_xy = sum((xs[i] - xm) * (values[i] - ym) for i in range(n))
    slope  = ss_xy / ss_xx if ss_xx > 0 else 0
    interc = ym - slope * xm
    y_pred = [slope * x + interc for x in xs]
    ss_res = sum((values[i] - y_pred[i]) ** 2 for i in range(n))
    ss_tot = sum((v - ym) ** 2 for v in values) or 1e-10
    r2     = max(0.0, 1.0 - ss_res / ss_tot)
    start_val = interc
    end_val   = slope * (n - 1) + interc
    slope_pct = (end_val - start_val) / abs(start_val) * 100 if start_val != 0 else 0
    return (slope, interc, r2, start_val, end_val, slope_pct)

def ema_val(closes, period):
    if len(closes) < period: return None
    k = 2.0 / (period + 1)
    v = sum(closes[:period]) / period
    for c in closes[period:]:
        v = c * k + v * (1 - k)
    return v

def daily_ema50_slope(k_daily):
    if not k_daily or len(k_daily) < 60: return 'flat'
    c_pos = last_closed_pos(k_daily)
    if c_pos < 55: return 'flat'
    cl    = [float(k[4]) for k in k_daily[:c_pos + 1]]
    e_now  = ema_val(cl,      50)
    e_prev = ema_val(cl[:-5], 50)
    if not e_now or not e_prev: return 'flat'
    chg = (e_now - e_prev) / e_prev * 100
    if chg >  0.1: return 'up'
    if chg < -0.1: return 'down'
    return 'flat'

def calc_strength(price_slope_pct, rsi_slope_pct, div_ratio):
    """
    Strength 0–100:
      50% from divergence ratio (how much faster RSI vs price)
      50% from RSI aggressiveness (how strongly RSI is moving)
    """
    ratio_score = min(50, div_ratio / 20 * 50)
    rsi_agg     = min(50, abs(rsi_slope_pct) / 30 * 50)
    return round(min(100, ratio_score + rsi_agg), 1)

# ── Core Analysis ──────────────────────────────────────────────────────────
def analyze(sym, klines, ticker, k_daily=None):
    if not klines or len(klines) < settings["lookback_bars"] + 20:
        return []

    c_pos = last_closed_pos(klines)
    bars  = klines[:c_pos + 1]
    n     = len(bars)
    lb    = settings["lookback_bars"]

    if n < lb + 15:
        return []

    # Use last lb closed bars
    window = bars[n - lb:]
    closes = [float(k[4]) for k in window]
    vols   = [float(k[5]) for k in window]
    price  = closes[-1]

    # RSI on full bar set for warmup, then slice last lb
    all_closes = [float(k[4]) for k in bars]
    rsi_full   = calc_rsi(all_closes, settings["rsi_period"])
    rsi_window = rsi_full[n - lb:]

    # Filter out None RSI values (warmup)
    valid_pairs = [(closes[i], rsi_window[i])
                   for i in range(lb) if rsi_window[i] is not None]
    if len(valid_pairs) < lb * 0.8:
        return []

    c_vals   = [p[0] for p in valid_pairs]
    rsi_vals = [p[1] for p in valid_pairs]

    # Linear regression on price
    _, _, price_r2, p_start, p_end, price_slope_pct = linear_regression(c_vals)

    # Linear regression on RSI
    # Normalise RSI slope: divide by 50 (midpoint) × 100
    _, _, rsi_r2, r_start, r_end, rsi_slope_raw = linear_regression(rsi_vals)
    rsi_slope_pct = (r_end - r_start) / 50 * 100

    # R² quality gate
    if price_r2 < settings["min_r2"] and rsi_r2 < settings["min_r2"]:
        return []

    # Daily slope
    slope = daily_ema50_slope(k_daily) if settings["ema50_filter"] else 'flat'

    # Average volume
    avg_vol = sum(vols) / len(vols) if vols else 1

    results = []

    # ── BULLISH ────────────────────────────────────────────────────────────
    if settings["bullish_enabled"]:
        p_ok = settings["price_slope_bull_min"] <= price_slope_pct <= settings["price_slope_bull_max"]
        r_ok = rsi_slope_pct >= settings["rsi_slope_bull_min"]
        # Divergence ratio: RSI rising at least N× faster than price
        price_abs = max(abs(price_slope_pct), 0.1)
        div_ratio = rsi_slope_pct / price_abs
        ratio_ok  = div_ratio >= settings["min_div_ratio"]

        if p_ok and r_ok and ratio_ok:
            strength = calc_strength(price_slope_pct, rsi_slope_pct, div_ratio)
            if strength >= settings["min_score"]:
                ema_warn = settings["ema50_filter"] and slope == 'down'
                results.append({
                    "symbol":          sym,
                    "base":            sym.replace("USDT", ""),
                    "direction":       "bullish",
                    "price":           round(price, 8),
                    "chg_24h":         round(float(ticker.get("priceChangePercent", 0)), 2),
                    "vol_24m":         round(float(ticker.get("quoteVolume", 0)) / 1e6, 1),
                    "price_slope_pct": round(price_slope_pct, 2),
                    "rsi_slope_pct":   round(rsi_slope_pct, 2),
                    "div_ratio":       round(div_ratio, 1),
                    "price_r2":        round(price_r2, 2),
                    "rsi_r2":          round(rsi_r2, 2),
                    "rsi_start":       round(r_start, 1),
                    "rsi_end":         round(rsi_vals[-1], 1),
                    "price_start":     round(p_start, 4),
                    "price_end":       round(c_vals[-1], 4),
                    "strength":        strength,
                    "slope":           slope,
                    "ema_warn":        ema_warn,
                    "lookback":        lb,
                })

    # ── BEARISH ────────────────────────────────────────────────────────────
    if settings["bearish_enabled"]:
        p_ok = settings["price_slope_bear_min"] <= price_slope_pct <= settings["price_slope_bear_max"]
        r_ok = rsi_slope_pct <= settings["rsi_slope_bear_max"]
        price_abs = max(abs(price_slope_pct), 0.1)
        div_ratio = abs(rsi_slope_pct) / price_abs
        ratio_ok  = div_ratio >= settings["min_div_ratio"]

        if p_ok and r_ok and ratio_ok:
            strength = calc_strength(price_slope_pct, rsi_slope_pct, div_ratio)
            if strength >= settings["min_score"]:
                ema_warn = settings["ema50_filter"] and slope == 'up'
                results.append({
                    "symbol":          sym,
                    "base":            sym.replace("USDT", ""),
                    "direction":       "bearish",
                    "price":           round(price, 8),
                    "chg_24h":         round(float(ticker.get("priceChangePercent", 0)), 2),
                    "vol_24m":         round(float(ticker.get("quoteVolume", 0)) / 1e6, 1),
                    "price_slope_pct": round(price_slope_pct, 2),
                    "rsi_slope_pct":   round(rsi_slope_pct, 2),
                    "div_ratio":       round(div_ratio, 1),
                    "price_r2":        round(price_r2, 2),
                    "rsi_r2":          round(rsi_r2, 2),
                    "rsi_start":       round(r_start, 1),
                    "rsi_end":         round(rsi_vals[-1], 1),
                    "price_start":     round(p_start, 4),
                    "price_end":       round(c_vals[-1], 4),
                    "strength":        strength,
                    "slope":           slope,
                    "ema_warn":        ema_warn,
                    "lookback":        lb,
                })

    return results

# ── Chart API ──────────────────────────────────────────────────────────────
@app.route("/api/chart/<symbol>")
def api_chart(symbol):
    try:
        klines = get_json("/fapi/v1/klines",
                          {"symbol": symbol, "interval": "4h", "limit": 130},
                          timeout=12)
        if not klines: return jsonify({"error": "No data"}), 404
        c_pos = last_closed_pos(klines)
        bars  = klines[:c_pos + 1]
        lb    = settings["lookback_bars"]

        ohlcv = [{"t": int(b[0])//1000, "o": float(b[1]), "h": float(b[2]),
                  "l": float(b[3]), "c": float(b[4]), "v": float(b[5])}
                 for b in bars]

        all_closes = [b["c"] for b in ohlcv]
        rsi_full   = calc_rsi(all_closes, settings["rsi_period"])
        rsi_data   = [{"t": ohlcv[i]["t"], "v": round(rsi_full[i], 2)}
                      for i in range(len(ohlcv)) if rsi_full[i] is not None]

        # Regression lines for last lb bars
        n = len(ohlcv)
        start_idx = max(0, n - lb)
        c_vals   = all_closes[start_idx:]
        rsi_vals = [rsi_full[i] for i in range(start_idx, n)
                    if rsi_full[i] is not None]
        t_vals   = [ohlcv[start_idx + i]["t"] for i in range(len(c_vals))]
        t_rsi    = [ohlcv[start_idx + i]["t"]
                    for i in range(len(c_vals)) if rsi_full[start_idx + i] is not None]

        # Price regression line
        _, _, _, ps, pe, _ = linear_regression(c_vals)
        price_reg = [{"t": t_vals[0],  "v": round(ps, 8)},
                     {"t": t_vals[-1], "v": round(pe, 8)}]

        # RSI regression line
        if rsi_vals:
            _, _, _, rs, re, _ = linear_regression(rsi_vals)
            rsi_reg = [{"t": t_rsi[0],  "v": round(rs, 2)},
                       {"t": t_rsi[-1], "v": round(re, 2)}]
        else:
            rsi_reg = []

        sig = next((s for s in cache["signals"] if s["symbol"] == symbol), None)

        return jsonify({
            "symbol":    symbol,
            "ohlcv":     ohlcv[-lb:],
            "rsi":       rsi_data[-lb:],
            "price_reg": price_reg,
            "rsi_reg":   rsi_reg,
            "signal":    sig,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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
             and float(t.get("quoteVolume", 0)) >= min_vol],
            key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)[:300]

        total = len(usdt); done = [0]; found = []; lock = threading.Lock()
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process(t):
            sym = t["symbol"]
            try:
                from concurrent.futures import ThreadPoolExecutor as TPE
                with TPE(max_workers=2) as ex:
                    f4h = ex.submit(get_json, "/fapi/v1/klines",
                                    {"symbol": sym, "interval": "4h", "limit": 130})
                    f1d = ex.submit(get_json, "/fapi/v1/klines",
                                    {"symbol": sym, "interval": "1d", "limit": 60})
                    k4h = f4h.result(); k1d = f1d.result()
                return analyze(sym, k4h, t, k1d)
            except Exception as e:
                print(f"[SCAN] {sym}: {e}")
                return []
            finally:
                with lock:
                    done[0] += 1
                    cache["progress"]     = f"Scanning [{done[0]}/{total}]..."
                    cache["progress_pct"] = int(done[0] / total * 95)

        with ThreadPoolExecutor(max_workers=15) as ex:
            futures = {ex.submit(process, t): t for t in usdt}
            for f in as_completed(futures, timeout=180):
                try:
                    res = f.result(timeout=15)
                    if res:
                        with lock: found.extend(res)
                except Exception as e:
                    print(f"[FUTURES] {e}")

        found.sort(key=lambda x: -x["strength"])

        # Telegram alerts
        alert_list = [s for s in found if s["strength"] >= settings["alert_score"]]
        if tg_cfg["enabled"] and alert_list:
            scan_time = time.strftime("%Y-%m-%d %H:%M")
            bull = [s for s in alert_list if s["direction"] == "bullish"]
            bear = [s for s in alert_list if s["direction"] == "bearish"]
            parts = [
                f"📐 <b>RSI Slope Divergence — {scan_time}</b>",
                f"🟢 Bullish: {len(bull)}  🔴 Bearish: {len(bear)}",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ]
            for s in alert_list[:15]:
                ico  = "🟢" if s["direction"] == "bullish" else "🔴"
                warn = " ⚠️" if s["ema_warn"] else ""
                parts.append(
                    f"{ico} <b>{s['base']}/USDT</b>{warn}  Strength: <b>{s['strength']}</b>\n"
                    f"   Price slope: {s['price_slope_pct']:+.1f}%  "
                    f"RSI slope: {s['rsi_slope_pct']:+.1f}%  "
                    f"Ratio: {s['div_ratio']}×\n"
                    f"   RSI: {s['rsi_start']} → {s['rsi_end']}  "
                    f"Daily: {s['slope']}"
                )
            parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            tg_send("\n".join(parts))

        cache["signals"]      = found
        cache["last_scan"]    = time.strftime("%Y-%m-%d %H:%M:%S")
        cache["scan_count"]   = cache.get("scan_count", 0) + 1
        cache["progress"]     = f"Done — {len(found)} signals found"
        cache["progress_pct"] = 100
        cache["next_scan_at"] = next_4h_ms() / 1000

    except Exception as e:
        import traceback
        cache["error"]    = str(e)
        cache["progress"] = f"Error: {str(e)[:80]}"
        print(traceback.format_exc())
    finally:
        cache["scanning"] = False
        scan_lock.release()

def sync_scanner():
    time.sleep(5)
    while True:
        try:
            secs = secs_to_next_4h()
            time.sleep(secs + 0.5)
            if not cache["scanning"]:
                cache["next_scan_at"] = next_4h_ms() / 1000
                threading.Thread(target=do_scan, daemon=True).start()
            else:
                time.sleep(60)
        except Exception as e:
            print(f"[SYNC] {e}")
            time.sleep(60)

cache["next_scan_at"] = next_4h_ms() / 1000
threading.Thread(target=sync_scanner, daemon=True).start()

@app.route("/")
def index(): return Response(HTML, mimetype="text/html")

@app.route("/health")
def health(): return jsonify({"status": "ok", "signals": len(cache["signals"])})

@app.route("/api/scan", methods=["POST"])
def api_scan():
    if cache["scanning"]: return jsonify({"status": "already_scanning"})
    threading.Thread(target=do_scan, daemon=True).start()
    return jsonify({"status": "started"})

@app.route("/api/status")
def api_status():
    now  = time.time()
    sigs = cache["signals"]
    return jsonify({
        "scanning":     cache["scanning"],
        "progress":     cache["progress"],
        "progress_pct": cache["progress_pct"],
        "last_scan":    cache["last_scan"],
        "error":        cache["error"],
        "scan_count":   cache.get("scan_count", 0),
        "next_scan_in": max(0, cache.get("next_scan_at", 0) - now),
        "signals":      sigs,
        "settings":     settings,
        "stats": {
            "total":   len(sigs),
            "bullish": sum(1 for s in sigs if s["direction"] == "bullish"),
            "bearish": sum(1 for s in sigs if s["direction"] == "bearish"),
        }
    })

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>RSI Slope Divergence Scanner</title>
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
             radial-gradient(ellipse at 80% 80%,rgba(240,180,41,0.03) 0%,transparent 50%);}
header{position:sticky;top:0;z-index:100;padding:12px 24px;
  border-bottom:1px solid var(--border);background:rgba(5,7,13,.97);
  backdrop-filter:blur(16px);display:flex;align-items:center;
  justify-content:space-between;flex-wrap:wrap;gap:8px;}
.logo{font-family:'Outfit',sans-serif;font-weight:900;font-size:18px;
  display:flex;align-items:center;gap:8px;}
.logo-icon{width:28px;height:28px;border-radius:6px;
  background:linear-gradient(135deg,var(--blue),var(--purple));
  display:flex;align-items:center;justify-content:center;font-size:13px;}
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
.btn-primary{background:linear-gradient(135deg,var(--blue),var(--purple));color:#fff;font-weight:700;}
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
  color:var(--blue);min-width:50px;text-align:center;}
#prog{display:none;height:2px;background:var(--border);}
#prog-fill{height:100%;background:linear-gradient(90deg,var(--blue),var(--purple));
  transition:width .3s;width:0%;}
#prog-fill.ind{width:30%;animation:ind 1s ease-in-out infinite;}
@keyframes ind{0%{transform:translateX(-200%)}100%{transform:translateX(500%)}}
#prog-lbl{padding:3px 24px;font-size:9px;color:var(--muted);display:none;}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(100px,1fr));
  gap:1px;background:var(--border);border-bottom:1px solid var(--border);}
.stat{background:var(--bg2);padding:10px 14px;}
.stat-lbl{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:3px;}
.stat-val{font-family:'Outfit',sans-serif;font-size:20px;font-weight:700;}
.cg{color:var(--green)}.cr{color:var(--red)}.cb{color:var(--blue)}
.cgold{color:var(--gold)}.cpur{color:var(--purple)}.cmuted{color:var(--muted);font-size:12px!important}
.filters{padding:8px 24px;display:flex;gap:8px;flex-wrap:wrap;align-items:center;
  border-bottom:1px solid var(--border);background:var(--bg2);}
.fl{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;}
.fg{display:flex;gap:3px;}
.fc{font-family:'DM Mono',monospace;font-size:9px;background:transparent;
  border:1px solid var(--border);color:var(--muted);padding:3px 9px;
  border-radius:3px;cursor:pointer;transition:all .15s;text-transform:uppercase;}
.fc:hover{color:var(--text);}
.fc.on{border-color:var(--blue);color:var(--blue);background:rgba(77,159,255,0.06);}
.fc.on-g{border-color:var(--green);color:var(--green);background:rgba(0,229,160,0.06);}
.fc.on-r{border-color:var(--red);color:var(--red);background:rgba(255,64,96,0.06);}
.tw{padding:12px 24px;overflow-x:auto;}
table{width:100%;border-collapse:collapse;min-width:950px;}
thead tr{border-bottom:1px solid var(--border);}
th{font-size:8px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;
  padding:6px 8px;text-align:left;white-space:nowrap;}
tbody tr{border-bottom:1px solid rgba(19,24,42,0.5);transition:background .1s;cursor:pointer;}
tbody tr:hover{background:rgba(77,159,255,0.03);}
td{padding:7px 8px;font-size:11px;white-space:nowrap;}
.pair{font-family:'Outfit',sans-serif;font-weight:700;font-size:14px;}
.pair-sub{font-size:8px;color:var(--muted);margin-top:1px;}
.badge{display:inline-flex;align-items:center;padding:3px 8px;border-radius:4px;
  font-size:9px;font-weight:700;}
.b-bull{background:rgba(0,229,160,0.1);color:var(--green);border:1px solid rgba(0,229,160,0.25);}
.b-bear{background:rgba(255,64,96,0.1);color:var(--red);border:1px solid rgba(255,64,96,0.25);}
.str-wrap{display:flex;align-items:center;gap:5px;}
.str-bg{width:55px;height:5px;background:var(--border);border-radius:3px;overflow:hidden;}
.str-fg{height:100%;border-radius:3px;}
.slope-bar{display:flex;align-items:center;gap:4px;font-size:10px;}
.slope-track{width:60px;height:6px;background:var(--border);border-radius:3px;
  position:relative;overflow:visible;}
.slope-zero{position:absolute;left:50%;top:-2px;width:1px;height:10px;
  background:var(--muted);opacity:0.5;}
.slope-fill{position:absolute;height:100%;border-radius:3px;top:0;}
.empty{text-align:center;padding:60px;color:var(--muted);}
.empty-ico{font-size:36px;margin-bottom:12px;opacity:.2;}
footer{padding:8px 24px;border-top:1px solid var(--border);font-size:9px;
  color:var(--muted);display:flex;justify-content:space-between;flex-wrap:wrap;gap:4px;}
.panel-overlay{display:none;position:fixed;inset:0;z-index:9500;
  background:rgba(0,0,0,0.9);backdrop-filter:blur(6px);
  align-items:flex-start;justify-content:center;overflow-y:auto;padding:20px;}
.panel-box{background:var(--bg2);border:1px solid var(--border);border-radius:10px;
  width:92vw;max-width:580px;overflow:hidden;margin:auto;}
.panel-hdr{display:flex;align-items:center;justify-content:space-between;
  padding:14px 18px;border-bottom:1px solid var(--border);background:var(--bg2);
  position:sticky;top:0;z-index:1;}
.panel-body{padding:18px;}
.p-section{margin-bottom:18px;}
.p-title{font-size:9px;color:var(--muted);text-transform:uppercase;
  letter-spacing:2px;margin-bottom:10px;padding-bottom:6px;
  border-bottom:1px solid var(--border);}
.p-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;gap:12px;}
.p-lbl{font-size:10px;color:var(--text);}
.p-sub{font-size:9px;color:var(--muted);margin-top:2px;}
.p-inp{background:var(--bg3);border:1px solid var(--border);border-radius:5px;
  padding:6px 10px;color:var(--gold);font-family:'DM Mono',monospace;
  font-size:11px;font-weight:700;outline:none;text-align:right;width:80px;}
.p-inp:focus{border-color:var(--gold);}
.p-save{width:100%;font-family:'DM Mono',monospace;font-size:10px;font-weight:700;
  letter-spacing:1px;padding:9px;border-radius:5px;border:none;cursor:pointer;
  background:linear-gradient(135deg,var(--blue),var(--purple));color:#fff;margin-top:8px;}
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
  width:96vw;max-width:1100px;max-height:92vh;overflow:hidden;
  display:flex;flex-direction:column;}
.chart-hdr{display:flex;align-items:center;justify-content:space-between;
  padding:12px 18px;border-bottom:1px solid var(--border);flex-shrink:0;}
.chart-title{font-family:'Outfit',sans-serif;font-weight:900;font-size:20px;}
.chart-meta{display:flex;gap:12px;align-items:center;margin-top:3px;flex-wrap:wrap;}
.chart-meta span{font-size:9px;color:var(--muted);}
.chart-body{flex:1;overflow-y:auto;}
.chart-pane{padding:12px 18px;}
.pane-lbl{font-size:8px;color:var(--muted);text-transform:uppercase;
  letter-spacing:2px;margin-bottom:6px;}
.chart-legend{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:6px;}
.leg-item{display:flex;align-items:center;gap:5px;font-size:9px;color:var(--muted);}
.leg-dot{width:10px;height:3px;border-radius:2px;}
::-webkit-scrollbar{width:3px;height:3px;}
::-webkit-scrollbar-track{background:var(--bg);}
::-webkit-scrollbar-thumb{background:var(--border);}
</style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">&#8726;</div>
    RSI Slope Divergence
    <span class="logo-sub">4H &middot; SLOPE ANALYSIS &middot; BINANCE PERPS</span>
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
  <div class="stat"><div class="stat-lbl">Total Signals</div><div class="stat-val cgold" id="s-total">—</div></div>
  <div class="stat"><div class="stat-lbl">&#129001; Bullish</div><div class="stat-val cg" id="s-bull">—</div></div>
  <div class="stat"><div class="stat-lbl">&#128308; Bearish</div><div class="stat-val cr" id="s-bear">—</div></div>
  <div class="stat"><div class="stat-lbl">Scan #</div><div class="stat-val cmuted" id="s-count">—</div></div>
  <div class="stat"><div class="stat-lbl">Last Scan</div><div class="stat-val cmuted" id="s-time">—</div></div>
</div>
<div class="filters">
  <span class="fl">Direction:</span>
  <div class="fg">
    <button class="fc on" id="f-all"  onclick="setFilt('all',this)">ALL</button>
    <button class="fc"    id="f-bull" onclick="setFilt('bullish',this)">&#129001; BULLISH</button>
    <button class="fc"    id="f-bear" onclick="setFilt('bearish',this)">&#128308; BEARISH</button>
  </div>
</div>
<div class="tw">
  <table>
    <thead><tr>
      <th>PAIR</th><th>DIRECTION</th><th>STRENGTH</th>
      <th>PRICE SLOPE</th><th>RSI SLOPE</th><th>RATIO</th>
      <th>RSI START</th><th>RSI NOW</th>
      <th>PRICE R²</th><th>RSI R²</th>
      <th>DAILY SLOPE</th><th>24H CHG</th>
    </tr></thead>
    <tbody id="tb">
      <tr><td colspan="12">
        <div class="empty">
          <div class="empty-ico">&#8726;</div>
          <div>Click SCAN NOW to detect RSI slope divergences</div>
          <div style="margin-top:8px;font-size:10px;color:var(--muted);">
            Price slope flat + RSI slope aggressive = divergence
          </div>
        </div>
      </td></tr>
    </tbody>
  </table>
</div>
<footer>
  <span>RSI Slope Divergence &middot; 4H &middot; Linear Regression &middot; 100-bar window &middot; Fires at 4H close</span>
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
        <div class="p-title">Core</div>
        <div class="p-row">
          <div><div class="p-lbl">RSI Period</div><div class="p-sub">Default 14</div></div>
          <input class="p-inp" id="s-rp" type="number" min="2" max="50" value="14">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Lookback Bars</div><div class="p-sub">Bars for slope (default 100)</div></div>
          <input class="p-inp" id="s-lb" type="number" min="20" max="120" value="100">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Min 24H Volume (M)</div></div>
          <input class="p-inp" id="s-mv" type="number" min="0" step="1" value="2">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Price Slope Range (flat/slight)</div>
        <div class="p-row">
          <div><div class="p-lbl">Bullish — Min price slope %</div><div class="p-sub">Default -3%</div></div>
          <input class="p-inp" id="s-pbmin" type="number" min="-20" max="0" step="0.5" value="-3">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Bullish — Max price slope %</div><div class="p-sub">Default +5%</div></div>
          <input class="p-inp" id="s-pbmax" type="number" min="0" max="20" step="0.5" value="5">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Bearish — Min price slope %</div><div class="p-sub">Default -5%</div></div>
          <input class="p-inp" id="s-prmin" type="number" min="-20" max="0" step="0.5" value="-5">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Bearish — Max price slope %</div><div class="p-sub">Default +3%</div></div>
          <input class="p-inp" id="s-prmax" type="number" min="0" max="20" step="0.5" value="3">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">RSI Aggressiveness</div>
        <div class="p-row">
          <div><div class="p-lbl">Bullish — Min RSI slope %</div><div class="p-sub">Default +10%</div></div>
          <input class="p-inp" id="s-rbmin" type="number" min="0" max="50" step="1" value="10">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Bearish — Max RSI slope %</div><div class="p-sub">Default -10%</div></div>
          <input class="p-inp" id="s-rrmax" type="number" min="-50" max="0" step="1" value="-10">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Min Divergence Ratio</div><div class="p-sub">RSI must be N× faster than price (default 2×)</div></div>
          <input class="p-inp" id="s-dr" type="number" min="1" max="20" step="0.5" value="2">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Quality</div>
        <div class="p-row">
          <div><div class="p-lbl">Min R² (regression quality)</div><div class="p-sub">Default 0.3</div></div>
          <input class="p-inp" id="s-r2" type="number" min="0" max="0.9" step="0.05" value="0.3">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Min Score to Show</div></div>
          <input class="p-inp" id="s-ms" type="number" min="0" max="90" step="5" value="30">
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Alert Score (Telegram)</div></div>
          <input class="p-inp" id="s-as" type="number" min="0" max="100" step="5" value="60">
        </div>
      </div>
      <div class="p-section">
        <div class="p-title">Toggles</div>
        <div class="p-row">
          <div><div class="p-lbl">Bullish Signals</div></div>
          <div class="tog" id="tog-bull" onclick="toggleBool('bull')"><div class="tog-thumb" id="th-bull"></div></div>
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Bearish Signals</div></div>
          <div class="tog" id="tog-bear" onclick="toggleBool('bear')"><div class="tog-thumb" id="th-bear"></div></div>
        </div>
        <div class="p-row">
          <div><div class="p-lbl">Daily EMA50 Slope Warning</div></div>
          <div class="tog" id="tog-ema" onclick="toggleBool('ema')"><div class="tog-thumb" id="th-ema"></div></div>
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
          <div style="font-size:9px;color:var(--muted);margin-top:2px;">Fires at every 4H scan</div>
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
          <span id="chart-dir-lbl">—</span>
          <span id="chart-str-lbl">—</span>
          <span id="chart-ratio-lbl">—</span>
          <span id="chart-warn-lbl" style="color:var(--gold)"></span>
        </div>
      </div>
      <button class="close-btn" onclick="closeChart()">&#10005; CLOSE</button>
    </div>
    <div class="chart-body">
      <div id="chart-loading" style="text-align:center;padding:60px;color:var(--muted);">Loading...</div>
      <div id="chart-content" style="display:none">
        <!-- Price pane -->
        <div class="chart-pane">
          <div class="pane-lbl">Price &middot; 4H Candles &middot; Last 100 Bars</div>
          <div class="chart-legend">
            <div class="leg-item"><div class="leg-dot" style="background:var(--green)"></div>Up candle</div>
            <div class="leg-item"><div class="leg-dot" style="background:var(--red)"></div>Down candle</div>
            <div class="leg-item"><div class="leg-dot" style="background:var(--gold)"></div>Price regression</div>
          </div>
          <div id="chart-main" style="height:300px;"></div>
        </div>
        <!-- RSI pane -->
        <div class="chart-pane" style="border-top:1px solid var(--border)">
          <div class="pane-lbl">RSI 14 &middot; Slope Divergence</div>
          <div class="chart-legend">
            <div class="leg-item"><div class="leg-dot" style="background:var(--purple)"></div>RSI line</div>
            <div class="leg-item"><div class="leg-dot" style="background:var(--gold)"></div>RSI regression (slope)</div>
            <div class="leg-item"><div class="leg-dot" style="background:rgba(255,64,96,0.4)"></div>Overbought 70</div>
            <div class="leg-item"><div class="leg-dot" style="background:rgba(0,229,160,0.4)"></div>Oversold 30</div>
          </div>
          <div id="chart-rsi" style="height:200px;"></div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
var signals=[], filt='all';
var scanPoll=null, isScanning=false;
var mainChart=null, rsiChart=null;
var boolToggles={bull:true, bear:true, ema:true};

function updateClock(nextIn){
  var r=Math.max(0,Math.round(nextIn));
  var h=Math.floor(r/3600), m=Math.floor((r%3600)/60);
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

function slopeBar(val, maxAbs, col){
  var pct = Math.min(100, Math.abs(val) / maxAbs * 50);
  var isPos = val >= 0;
  var left  = isPos ? 50 : (50 - pct);
  var width = pct;
  return '<div class="slope-bar">'
    +'<div class="slope-track">'
      +'<div class="slope-zero"></div>'
      +'<div class="slope-fill" style="left:'+left+'%;width:'+width+'%;background:'+col+'"></div>'
    +'</div>'
    +'<span style="color:'+col+';font-size:10px;font-weight:700">'+(val>=0?'+':'')+val+'%</span>'
    +'</div>';
}

function setFilt(val, el){
  filt=val;
  ['f-all','f-bull','f-bear'].forEach(function(id){
    document.getElementById(id).className='fc';
  });
  var cls=val==='bullish'?'on-g':val==='bearish'?'on-r':'on';
  el.className='fc '+cls;
  render();
}

function render(){
  var data=signals.slice();
  if(filt==='bullish') data=data.filter(function(s){return s.direction==='bullish';});
  if(filt==='bearish') data=data.filter(function(s){return s.direction==='bearish';});

  var tb=document.getElementById('tb');
  if(!data.length){
    tb.innerHTML='<tr><td colspan="12"><div class="empty">'
      +'<div class="empty-ico">&#8726;</div>'
      +'<div>No divergences found</div></div></td></tr>';
    return;
  }

  tb.innerHTML=data.slice(0,100).map(function(s){
    var isBull=s.direction==='bullish';
    var dirBadge=isBull
      ?'<span class="badge b-bull">&#129001; BULLISH</span>'
      :'<span class="badge b-bear">&#128308; BEARISH</span>';

    // Strength bar
    var strCol=s.strength>=70?'var(--green)':s.strength>=50?'var(--blue)':'var(--muted)';
    var strBar='<div class="str-wrap">'
      +'<div class="str-bg"><div class="str-fg" style="width:'+s.strength+'%;background:'+strCol+'"></div></div>'
      +'<span style="color:'+strCol+';font-weight:700;font-family:Outfit,sans-serif;font-size:14px">'+s.strength+'</span>'
      +'</div>';

    // Slope bars
    var priceCol=Math.abs(s.price_slope_pct)<5?'var(--muted)':'var(--gold)';
    var rsiCol=isBull?'var(--green)':'var(--red)';
    var pBar=slopeBar(s.price_slope_pct, 20, priceCol);
    var rBar=slopeBar(s.rsi_slope_pct,   40, rsiCol);

    var ratioCol=s.div_ratio>=5?'var(--green)':s.div_ratio>=3?'var(--blue)':'var(--muted)';
    var warnBadge=s.ema_warn?'<span style="color:var(--gold);font-size:9px">&#9888;</span>':'';
    var slopeCol=s.slope==='up'?'var(--green)':s.slope==='down'?'var(--red)':'var(--muted)';
    var slopeIco=s.slope==='up'?'&#8593;':s.slope==='down'?'&#8595;':'&#8212;';
    var dCol=s.chg_24h>=0?'var(--green)':'var(--red)';
    var r2Col=function(v){return v>=0.7?'var(--green)':v>=0.4?'var(--gold)':'var(--muted)';};

    return '<tr data-sym="'+s.symbol+'" onclick="openChart(this.dataset.sym)">'
      +'<td><div class="pair">'+s.base+'/USDT</div>'
        +'<div class="pair-sub">$'+s.vol_24m+'M</div></td>'
      +'<td>'+dirBadge+' '+warnBadge+'</td>'
      +'<td>'+strBar+'</td>'
      +'<td>'+pBar+'</td>'
      +'<td>'+rBar+'</td>'
      +'<td style="color:'+ratioCol+';font-weight:700">'+s.div_ratio+'&times;</td>'
      +'<td style="color:var(--muted)">'+s.rsi_start+'</td>'
      +'<td style="color:'+rsiCol+'">'+s.rsi_end+'</td>'
      +'<td style="color:'+r2Col(s.price_r2)+'">'+s.price_r2+'</td>'
      +'<td style="color:'+r2Col(s.rsi_r2)+'">'+s.rsi_r2+'</td>'
      +'<td style="color:'+slopeCol+'">'+slopeIco+' '+s.slope+'</td>'
      +'<td style="color:'+dCol+'">'+(s.chg_24h>=0?'+':'')+s.chg_24h+'%</td>'
      +'</tr>';
  }).join('');
}

// ── Chart ───────────────────────────────────────────────────────────────
function openChart(symbol){
  document.getElementById('chart-overlay').style.display='flex';
  document.getElementById('chart-loading').style.display='block';
  document.getElementById('chart-content').style.display='none';

  var sig=signals.find(function(s){return s.symbol===symbol;})||{};
  var isBull=sig.direction==='bullish';
  document.getElementById('chart-sym').textContent=symbol.replace('USDT','/USDT');
  document.getElementById('chart-dir-lbl').textContent=
    sig.direction?(isBull?'🟢 BULLISH':'🔴 BEARISH'):'';
  document.getElementById('chart-str-lbl').textContent=
    sig.strength?'Strength: '+sig.strength:'';
  document.getElementById('chart-ratio-lbl').textContent=
    sig.div_ratio?'Ratio: '+sig.div_ratio+'×':'';
  document.getElementById('chart-warn-lbl').textContent=
    sig.ema_warn?'⚠️ EMA50 slope opposes':'';

  if(mainChart){mainChart.remove();mainChart=null;}
  if(rsiChart){rsiChart.remove();rsiChart=null;}

  fetch('/api/chart/'+symbol)
    .then(function(r){return r.json();})
    .then(function(d){
      if(d.error){document.getElementById('chart-loading').textContent='Error: '+d.error;return;}
      document.getElementById('chart-loading').style.display='none';
      document.getElementById('chart-content').style.display='block';
      setTimeout(function(){drawCharts(d, sig);}, 50);
    })
    .catch(function(e){document.getElementById('chart-loading').textContent='Failed: '+e;});
}

function getW(){
  var box=document.querySelector('.chart-box');
  return box?(box.clientWidth-36):900;
}

function baseOpts(h){
  var w=getW();
  return {width:w,height:h,
    layout:{background:{color:'transparent'},textColor:'#3d4f72'},
    grid:{vertLines:{color:'rgba(19,24,42,0.8)'},horzLines:{color:'rgba(19,24,42,0.8)'}},
    crosshair:{mode:1},
    rightPriceScale:{borderColor:'#13182a'},
    timeScale:{borderColor:'#13182a',timeVisible:true}};
}

function drawCharts(d, sig){
  var isBull = sig.direction==='bullish';
  var divCol = isBull ? '#00e5a0' : '#ff4060';

  // ── Price chart ──────────────────────────────────────────────────────
  mainChart = LightweightCharts.createChart(
    document.getElementById('chart-main'), baseOpts(300));

  var candles = mainChart.addCandlestickSeries({
    upColor:'#00e5a0',downColor:'#ff4060',
    borderUpColor:'#00e5a0',borderDownColor:'#ff4060',
    wickUpColor:'#00e5a0',wickDownColor:'#ff4060'});
  candles.setData(d.ohlcv.map(function(b){
    return {time:b.t,open:b.o,high:b.h,low:b.l,close:b.c};}));

  // Price regression line (gold dashed)
  if(d.price_reg && d.price_reg.length===2){
    var pReg = mainChart.addLineSeries({
      color:'#f0b429',lineWidth:2,lineStyle:2,
      priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});
    pReg.setData(d.price_reg);
  }
  mainChart.timeScale().fitContent();

  // ── RSI chart ────────────────────────────────────────────────────────
  rsiChart = LightweightCharts.createChart(
    document.getElementById('chart-rsi'),
    Object.assign(baseOpts(200),{
      rightPriceScale:{borderColor:'#13182a',scaleMargins:{top:0.1,bottom:0.1}}}));

  // RSI line
  var rsiLine = rsiChart.addLineSeries({
    color:'#a78bfa',lineWidth:1.5,
    priceLineVisible:false,lastValueVisible:true});
  rsiLine.setData(d.rsi);

  // 70/50/30 levels
  rsiLine.createPriceLine({price:70,color:'rgba(255,64,96,0.4)',lineWidth:1,
    lineStyle:2,axisLabelVisible:true,title:'70'});
  rsiLine.createPriceLine({price:50,color:'rgba(61,79,114,0.3)',lineWidth:1,
    lineStyle:0,axisLabelVisible:false});
  rsiLine.createPriceLine({price:30,color:'rgba(0,229,160,0.4)',lineWidth:1,
    lineStyle:2,axisLabelVisible:true,title:'30'});

  // RSI regression line (same color as direction)
  if(d.rsi_reg && d.rsi_reg.length===2){
    var rReg = rsiChart.addLineSeries({
      color:'#f0b429',lineWidth:2,lineStyle:2,
      priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});
    rReg.setData(d.rsi_reg);
  }

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

// ── Settings ─────────────────────────────────────────────────────────
function setTog(k, on){
  boolToggles[k]=on;
  document.getElementById('tog-'+k).style.background=on?'var(--green)':'var(--border)';
  document.getElementById('th-'+k).style.left=on?'23px':'3px';
}
function toggleBool(k){setTog(k,!boolToggles[k]);}

function openSettings(){
  fetch('/api/settings').then(function(r){return r.json();}).then(function(d){
    document.getElementById('s-rp').value=d.rsi_period||14;
    document.getElementById('s-lb').value=d.lookback_bars||100;
    document.getElementById('s-mv').value=d.min_vol_24m||2;
    document.getElementById('s-pbmin').value=d.price_slope_bull_min||-3;
    document.getElementById('s-pbmax').value=d.price_slope_bull_max||5;
    document.getElementById('s-prmin').value=d.price_slope_bear_min||-5;
    document.getElementById('s-prmax').value=d.price_slope_bear_max||3;
    document.getElementById('s-rbmin').value=d.rsi_slope_bull_min||10;
    document.getElementById('s-rrmax').value=d.rsi_slope_bear_max||-10;
    document.getElementById('s-dr').value=d.min_div_ratio||2;
    document.getElementById('s-r2').value=d.min_r2||0.3;
    document.getElementById('s-ms').value=d.min_score||30;
    document.getElementById('s-as').value=d.alert_score||60;
    setTog('bull', d.bullish_enabled!==false);
    setTog('bear', d.bearish_enabled!==false);
    setTog('ema',  d.ema50_filter!==false);
    document.getElementById('set-status').textContent='';
    document.getElementById('set-panel').style.display='flex';
  });
}
function saveSettings(){
  var data={
    rsi_period:           parseInt(document.getElementById('s-rp').value)||14,
    lookback_bars:        parseInt(document.getElementById('s-lb').value)||100,
    min_vol_24m:          parseFloat(document.getElementById('s-mv').value)||2,
    price_slope_bull_min: parseFloat(document.getElementById('s-pbmin').value)||-3,
    price_slope_bull_max: parseFloat(document.getElementById('s-pbmax').value)||5,
    price_slope_bear_min: parseFloat(document.getElementById('s-prmin').value)||-5,
    price_slope_bear_max: parseFloat(document.getElementById('s-prmax').value)||3,
    rsi_slope_bull_min:   parseFloat(document.getElementById('s-rbmin').value)||10,
    rsi_slope_bear_max:   parseFloat(document.getElementById('s-rrmax').value)||-10,
    min_div_ratio:        parseFloat(document.getElementById('s-dr').value)||2,
    min_r2:               parseFloat(document.getElementById('s-r2').value)||0.3,
    min_score:            parseInt(document.getElementById('s-ms').value)||30,
    alert_score:          parseInt(document.getElementById('s-as').value)||60,
    bullish_enabled: boolToggles.bull,
    bearish_enabled: boolToggles.bear,
    ema50_filter:    boolToggles.ema,
  };
  fetch('/api/settings',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(data)})
  .then(function(r){return r.json();}).then(function(){
    var el=document.getElementById('set-status');
    el.textContent='Saved!';el.style.color='var(--green)';
    setTimeout(function(){el.textContent='';},2000);
  });
}

// ── Telegram ──────────────────────────────────────────────────────────
var tgEnabled=false;
function openTg(){
  fetch('/api/telegram/config').then(function(r){return r.json();}).then(function(d){
    tgEnabled=d.enabled;
    document.getElementById('tg-token').value=d.token||'';
    document.getElementById('tg-chatid').value=d.chat_id||'';
    setTgTog(tgEnabled);
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
      chat_id:document.getElementById('tg-chatid').value.trim()})
  }).then(function(r){return r.json();}).then(function(){
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
    print("  RSI Slope Divergence Scanner")
    print("  4H · Linear regression on price + RSI")
    print("  Bullish: price flat + RSI aggressively up")
    print("  Bearish: price flat + RSI aggressively down")
    print("  Open → http://localhost:5015")
    print("="*60 + "\n")
    port = int(os.environ.get("PORT", 5015))
    app.run(debug=False, host="0.0.0.0", port=port, threaded=True)
