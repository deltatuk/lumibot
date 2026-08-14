#!/usr/bin/env python3
"""Walk-forward research harness for StockSuggestionStrategy.

V1 goals
--------
* Parse the live strategy parameters directly from strategy.py without importing
  LumiBot or touching the broker.
* Replay the stock scoring logic exactly from completed daily OHLCV bars.
* Support dated universe membership to reduce survivorship bias.
* Approximate the live intraday screener with a historical daily proxy.
* Model long options and debit verticals with Black-Scholes plus explicit
  bid/ask/slippage assumptions when a true historical option chain is absent.
* Enforce the live strategy's major sizing / portfolio concentration controls.
* Optimize only on training folds, then report untouched out-of-sample folds.
* Export auditable candidates, folds, trades, equity curves, and JSON summary.

This is a research harness. It never submits orders and does not import the live
Strategy class.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import itertools
import json
import math
import os
import statistics
import sys
import urllib.parse
import urllib.request
import warnings
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv


# Match the live strategy credential-loading behavior. Prefer the project
# working directory (the normal `python walk_forward.py ...` launch location),
# then the directory containing this script. Existing exported environment
# variables remain authoritative because override=False.
_CWD_ENV = Path.cwd() / ".env"
_SCRIPT_ENV = Path(__file__).resolve().parent / ".env"
if _CWD_ENV.exists():
    load_dotenv(dotenv_path=_CWD_ENV, override=False)
if _SCRIPT_ENV != _CWD_ENV and _SCRIPT_ENV.exists():
    load_dotenv(dotenv_path=_SCRIPT_ENV, override=False)

NORMAL = NormalDist()
REQUIRED_BAR_COLUMNS = {"date", "symbol", "open", "high", "low", "close", "volume"}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return pd.concat(frames, ignore_index=True)


def atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(path)


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temp, index=False)
    temp.replace(path)


def load_strategy_parameters(strategy_path: Path) -> dict[str, Any]:
    """Read StockSuggestionStrategy.parameters using AST only.

    This keeps the backtest parameter defaults tied to the live source without
    importing LumiBot / Alpaca or constructing broker clients.
    """
    tree = ast.parse(strategy_path.read_text(encoding="utf-8"), filename=str(strategy_path))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "StockSuggestionStrategy":
            for item in node.body:
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name) and target.id == "parameters":
                            params = ast.literal_eval(item.value)
                            if not isinstance(params, dict):
                                raise ValueError("strategy parameters are not a dict")
                            return params
    raise ValueError("StockSuggestionStrategy.parameters not found")


def config_hash(config: dict[str, Any], params: dict[str, Any]) -> str:
    raw = json.dumps({"config": config, "strategy_parameters": params}, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


@dataclass
class UniverseMembership:
    symbol: str
    start_date: Optional[pd.Timestamp] = None
    end_date: Optional[pd.Timestamp] = None

    def active(self, day: pd.Timestamp) -> bool:
        d = pd.Timestamp(day).normalize()
        if self.start_date is not None and d < self.start_date:
            return False
        if self.end_date is not None and d > self.end_date:
            return False
        return True


class UniverseProvider:
    def __init__(self, memberships: list[UniverseMembership]):
        self.memberships = memberships
        self._by_symbol: dict[str, list[UniverseMembership]] = {}
        for member in memberships:
            self._by_symbol.setdefault(member.symbol, []).append(member)

    @classmethod
    def from_csv(cls, path: Path) -> "UniverseProvider":
        frame = pd.read_csv(path)
        if "symbol" not in frame.columns:
            raise ValueError("universe CSV must contain symbol")
        memberships = []
        for _, row in frame.iterrows():
            symbol = str(row["symbol"]).strip().upper()
            if not symbol:
                continue
            start = pd.to_datetime(row.get("start_date"), errors="coerce")
            end = pd.to_datetime(row.get("end_date"), errors="coerce")
            memberships.append(
                UniverseMembership(
                    symbol=symbol,
                    start_date=None if pd.isna(start) else pd.Timestamp(start).normalize(),
                    end_date=None if pd.isna(end) else pd.Timestamp(end).normalize(),
                )
            )
        if not memberships:
            raise ValueError("universe CSV has no usable symbols")
        return cls(memberships)

    def symbols(self) -> list[str]:
        return sorted(self._by_symbol)

    def active_symbols(self, day: pd.Timestamp) -> set[str]:
        return {
            symbol
            for symbol, memberships in self._by_symbol.items()
            if any(member.active(day) for member in memberships)
        }


class AlpacaStockBarsDownloader:
    """Minimal REST downloader so the harness has no extra SDK dependency.

    It uses the user's existing ALPACA_API_KEY / ALPACA_API_SECRET values,
    loading them from the local .env exactly like the live strategy when they
    are not already exported in the shell. It only calls the historical
    stock-bars endpoint.
    """

    base_url = "https://data.alpaca.markets/v2/stocks/bars"

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        self.api_key = api_key or os.getenv("ALPACA_API_KEY")
        self.api_secret = api_secret or os.getenv("ALPACA_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise RuntimeError("ALPACA_API_KEY and ALPACA_API_SECRET are required for download")

    def _request_page(self, symbols: list[str], start: str, end: str, page_token: Optional[str]) -> dict[str, Any]:
        query = {
            "symbols": ",".join(symbols),
            "timeframe": "1Day",
            "start": start,
            "end": end,
            "adjustment": "all",
            "limit": "10000",
            "sort": "asc",
        }
        if page_token:
            query["page_token"] = page_token
        url = self.base_url + "?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(
            url,
            headers={
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.api_secret,
                "Accept": "application/json",
                "User-Agent": "lumibot-walk-forward/1.0",
            },
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))

    def download(self, symbols: list[str], start: str, end: str, chunk_size: int = 100) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for offset in range(0, len(symbols), chunk_size):
            chunk = symbols[offset : offset + chunk_size]
            token = None
            while True:
                payload = self._request_page(chunk, start, end, token)
                bars = payload.get("bars", {}) or {}
                for symbol, items in bars.items():
                    for bar in items or []:
                        rows.append(
                            {
                                "date": pd.Timestamp(bar["t"]).date().isoformat(),
                                "symbol": str(symbol).upper(),
                                "open": bar["o"],
                                "high": bar["h"],
                                "low": bar["l"],
                                "close": bar["c"],
                                "volume": bar["v"],
                                "trade_count": bar.get("n"),
                                "vwap": bar.get("vw"),
                            }
                        )
                token = payload.get("next_page_token")
                if not token:
                    break
        result = pd.DataFrame(rows)
        if result.empty:
            return result
        return normalize_bars(result)


def normalize_bars(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    if "timestamp" in frame.columns and "date" not in frame.columns:
        frame = frame.rename(columns={"timestamp": "date"})
    missing = REQUIRED_BAR_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"bars missing required columns: {sorted(missing)}")
    frame["date"] = pd.to_datetime(frame["date"], utc=True, errors="coerce").dt.tz_convert(None).dt.normalize()
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    for col in ["open", "high", "low", "close", "volume"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "open", "high", "low", "close", "volume"])
    frame = frame[(frame[["open", "high", "low", "close"]] > 0).all(axis=1)]
    frame = frame.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last")
    return frame.reset_index(drop=True)


def build_feature_table(bars: pd.DataFrame) -> pd.DataFrame:
    bars = normalize_bars(bars)
    groups = []
    for symbol, df in bars.groupby("symbol", sort=False):
        df = df.sort_values("date").copy()
        close = df["close"].astype(float)
        volume = df["volume"].astype(float)
        ret = close.pct_change()
        df["sma20"] = close.rolling(20, min_periods=20).mean()
        df["sma50"] = close.rolling(50, min_periods=50).mean()
        df["momentum20"] = close / close.shift(20) - 1.0
        df["momentum60"] = close / close.shift(60) - 1.0
        df["volatility20"] = ret.rolling(20, min_periods=20).std()
        df["avg_volume20"] = volume.rolling(20, min_periods=20).mean()
        df["avg_dollar_volume20"] = (close * volume).rolling(20, min_periods=20).mean()
        df["relative_volume"] = np.where(df["avg_volume20"] > 0, volume / df["avg_volume20"], np.nan)
        df["bullish_trend"] = ((close > df["sma20"]) & (df["sma20"] > df["sma50"])).astype(int)
        df["bearish_trend"] = ((close < df["sma20"]) & (df["sma20"] < df["sma50"])).astype(int)
        df["day_return"] = ret
        groups.append(df)
    return pd.concat(groups, ignore_index=True).sort_values(["date", "symbol"]).reset_index(drop=True)


def historical_proxy_screen(day_frame: pd.DataFrame, params: dict[str, Any], mode: str) -> pd.DataFrame:
    if mode == "membership_all":
        return day_frame.copy()
    if mode != "historical_proxy_screener":
        raise ValueError(f"unknown universe_screen_mode: {mode}")
    if day_frame.empty:
        return day_frame
    n_active = max(1, int(params.get("most_active_volume_count", 100)))
    n_movers = max(1, int(params.get("market_movers_count", 50)))
    active = day_frame.nlargest(n_active, "volume")
    gainers = day_frame.nlargest(n_movers, "day_return")
    losers = day_frame.nsmallest(n_movers, "day_return")
    symbols = set(active["symbol"]) | set(gainers["symbol"]) | set(losers["symbol"])
    return day_frame[day_frame["symbol"].isin(symbols)].copy()


def score_day(day_frame: pd.DataFrame, params: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = [
        "sma20", "sma50", "momentum20", "momentum60", "volatility20",
        "avg_dollar_volume20", "relative_volume", "bullish_trend", "bearish_trend",
    ]
    base = day_frame.dropna(subset=required).copy()
    eligible = base[
        (base["close"] >= float(params["minimum_price"]))
        & (base["avg_dollar_volume20"] >= float(params["minimum_dollar_volume"]))
        & (base["relative_volume"] >= float(params["minimum_relative_volume"]))
    ].copy()

    def score_side(frame: pd.DataFrame, bullish: bool) -> pd.DataFrame:
        if frame.empty:
            return frame.assign(score=pd.Series(dtype=float))
        frame = frame.copy()
        sign = 1.0 if bullish else -1.0
        frame["momentum20_score"] = (sign * frame["momentum20"]).rank(pct=True)
        frame["momentum60_score"] = (sign * frame["momentum60"]).rank(pct=True)
        frame["volume_score"] = frame["relative_volume"].rank(pct=True)
        frame["volatility_score"] = 1.0 - frame["volatility20"].rank(pct=True)
        trend_col = "bullish_trend" if bullish else "bearish_trend"
        frame["score"] = 100.0 * (
            frame["momentum20_score"] * 0.30
            + frame["momentum60_score"] * 0.25
            + frame["volume_score"] * 0.20
            + frame["volatility_score"] * 0.10
            + frame[trend_col] * 0.15
        )
        return frame.sort_values("score", ascending=False).head(int(params["top_results"]))

    bullish = eligible[eligible["momentum20"] >= float(params["bullish_momentum_threshold"])].copy()
    bearish = eligible[eligible["momentum20"] <= float(params["bearish_momentum_threshold"])].copy()
    return score_side(bullish, True), score_side(bearish, False)


def bs_price_delta(spot: float, strike: float, t_years: float, rate: float, vol: float, option_type: str) -> tuple[float, float]:
    spot = max(1e-9, float(spot))
    strike = max(1e-9, float(strike))
    t_years = max(0.0, float(t_years))
    vol = max(1e-6, float(vol))
    if t_years <= 1e-9:
        if option_type == "call":
            intrinsic = max(0.0, spot - strike)
            delta = 1.0 if spot > strike else (0.5 if spot == strike else 0.0)
        else:
            intrinsic = max(0.0, strike - spot)
            delta = -1.0 if spot < strike else (-0.5 if spot == strike else 0.0)
        return intrinsic, delta
    sqrt_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * t_years) / (vol * sqrt_t)
    d2 = d1 - vol * sqrt_t
    if option_type == "call":
        price = spot * NORMAL.cdf(d1) - strike * math.exp(-rate * t_years) * NORMAL.cdf(d2)
        delta = NORMAL.cdf(d1)
    else:
        price = strike * math.exp(-rate * t_years) * NORMAL.cdf(-d2) - spot * NORMAL.cdf(-d1)
        delta = NORMAL.cdf(d1) - 1.0
    return max(0.0, price), delta


def round_strike(value: float, tick: float) -> float:
    tick = max(0.01, float(tick))
    return max(tick, round(value / tick) * tick)


def choose_strike_for_delta(
    spot: float,
    dte: int,
    rate: float,
    vol: float,
    option_type: str,
    target_abs_delta: float,
    strike_band_pct: float,
    strike_tick: float,
    lower_bound: Optional[float] = None,
    upper_bound: Optional[float] = None,
) -> tuple[float, float, float]:
    low = max(strike_tick, spot * (1.0 - strike_band_pct))
    high = spot * (1.0 + strike_band_pct)
    if lower_bound is not None:
        low = max(low, lower_bound)
    if upper_bound is not None:
        high = min(high, upper_bound)
    if high < low:
        raise ValueError("empty strike search range")
    tick = max(0.01, strike_tick)
    first = math.ceil(low / tick) * tick
    count = max(1, int(math.floor((high - first) / tick)) + 1)
    strikes = [round(first + i * tick, 8) for i in range(count)]
    best = None
    t_years = max(1, dte) / 365.0
    for strike in strikes:
        price, delta = bs_price_delta(spot, strike, t_years, rate, vol, option_type)
        diff = abs(abs(delta) - target_abs_delta)
        key = (diff, abs(strike - spot))
        if best is None or key < best[0]:
            best = (key, strike, price, delta)
    assert best is not None
    return best[1], best[2], best[3]


def synthetic_quote(mid: float, spread_pct: float, slippage_pct: float, min_tick: float = 0.01) -> tuple[float, float]:
    mid = max(0.0, mid)
    half = max(0.0, spread_pct) / 2.0
    slip = max(0.0, slippage_pct)
    bid = max(0.0, mid * (1.0 - half - slip))
    ask = max(min_tick, mid * (1.0 + half + slip))
    return round(bid, 4), round(ask, 4)


def proxy_option_score(
    spread_pct: float,
    abs_delta: float,
    dte: int,
    params: dict[str, Any],
) -> float:
    max_spread = max(1e-6, float(params["option_max_spread_pct"]))
    spread_score = max(0.0, 1.0 - spread_pct / max_spread)
    min_delta = float(params["option_min_abs_delta"])
    max_delta = float(params["option_max_abs_delta"])
    target_delta = float(params["option_target_abs_delta"])
    if abs_delta < min_delta or abs_delta > max_delta:
        delta_score = 0.0
    else:
        delta_score = max(0.0, 1.0 - abs(abs_delta - target_delta) / max(1e-6, max_delta - min_delta))
    min_dte = float(params["option_min_dte"])
    max_dte = float(params["option_max_dte"])
    target_dte = float(params["option_target_dte"])
    if dte < min_dte or dte > max_dte:
        dte_score = 0.0
    else:
        dte_score = max(0.0, 1.0 - abs(dte - target_dte) / max(1.0, max_dte - min_dte))
    # OI, quote size, and historical daily volume are unavailable in synthetic
    # mode. Renormalize only the observable live-score components.
    weights = {
        "spread": float(params["option_score_spread_weight"]),
        "delta": float(params["option_score_delta_weight"]),
        "dte": float(params["option_score_dte_weight"]),
    }
    total = sum(weights.values())
    return 100.0 * (
        spread_score * weights["spread"] + delta_score * weights["delta"] + dte_score * weights["dte"]
    ) / max(1e-9, total)


@dataclass
class SyntheticStructure:
    direction: str
    decision: str
    entry_spot: float
    expiration_date: pd.Timestamp
    long_strike: float
    short_strike: Optional[float]
    iv: float
    entry_debit: float
    max_risk: float
    max_reward: float
    reward_risk: float
    stock_score: float
    option_score: float
    structure_score: float
    long_delta: float
    short_delta: Optional[float] = None
    model_note: str = "SYNTHETIC_BS_PROXY"


def build_synthetic_structure(
    direction: str,
    stock_score: float,
    entry_spot: float,
    realized_vol20: float,
    entry_date: pd.Timestamp,
    params: dict[str, Any],
    cfg: dict[str, Any],
) -> Optional[SyntheticStructure]:
    option_type = "call" if direction == "BULLISH" else "put"
    dte = int(params["option_target_dte"])
    expiry = pd.Timestamp(entry_date).normalize() + pd.Timedelta(days=dte)
    vol = float(np.clip(realized_vol20 * math.sqrt(252.0) * float(cfg["synthetic_iv_multiplier"]), float(cfg["synthetic_iv_floor"]), float(cfg["synthetic_iv_cap"])))
    rate = float(cfg["risk_free_rate"])
    strike_tick = float(cfg["synthetic_strike_tick"])
    long_strike, long_mid, long_delta = choose_strike_for_delta(
        entry_spot, dte, rate, vol, option_type,
        float(params["option_target_abs_delta"]),
        float(params["option_strike_band_pct"]), strike_tick,
    )
    spread_pct = float(cfg["synthetic_option_spread_pct"])
    slippage = float(cfg["synthetic_option_slippage_pct"])
    long_bid, long_ask = synthetic_quote(long_mid, spread_pct, slippage)
    if long_ask < float(params["option_min_mid_price"]):
        return None
    opt_score = proxy_option_score(spread_pct, abs(long_delta), dte, params)
    combined = stock_score * float(params["option_stock_weight"]) + opt_score * float(params["option_contract_weight"])

    iv_penalty = 0.0
    threshold = float(params["long_option_high_iv_threshold"])
    max_penalty = float(params["long_option_max_iv_penalty"])
    if threshold > 0 and vol > threshold:
        iv_penalty = min(max_penalty, ((vol - threshold) / threshold) * max_penalty)
    long_structure_score = max(0.0, combined - iv_penalty)
    if option_type == "call":
        long_max_reward = float("inf")
        long_rr = float("inf")
        long_decision = "LONG CALL"
    else:
        long_max_reward = max(0.0, (long_strike - long_ask) * 100.0)
        long_rr = long_max_reward / max(1e-9, long_ask * 100.0)
        long_decision = "LONG PUT"
    long_structure = SyntheticStructure(
        direction=direction,
        decision=long_decision,
        entry_spot=entry_spot,
        expiration_date=expiry,
        long_strike=long_strike,
        short_strike=None,
        iv=vol,
        entry_debit=long_ask,
        max_risk=long_ask * 100.0,
        max_reward=long_max_reward,
        reward_risk=long_rr,
        stock_score=stock_score,
        option_score=opt_score,
        structure_score=long_structure_score,
        long_delta=long_delta,
    )

    # Synthetic vertical candidate: pick the short strike closest to the live
    # short-delta target inside the live width bounds.
    min_width = max(strike_tick, entry_spot * float(params["vertical_min_width_pct"]))
    max_width = max(min_width, entry_spot * float(params["vertical_max_width_pct"]))
    if option_type == "call":
        lower = long_strike + min_width
        upper = long_strike + max_width
    else:
        lower = max(strike_tick, long_strike - max_width)
        upper = max(strike_tick, long_strike - min_width)
    vertical = None
    try:
        short_strike, short_mid, short_delta = choose_strike_for_delta(
            entry_spot, dte, rate, vol, option_type,
            (float(params["vertical_short_min_abs_delta"]) + float(params["vertical_short_max_abs_delta"])) / 2.0,
            float(params["option_strike_band_pct"]), strike_tick,
            lower_bound=lower, upper_bound=upper,
        )
        short_bid, short_ask = synthetic_quote(short_mid, spread_pct, slippage)
        width = abs(short_strike - long_strike)
        debit = long_ask - short_bid
        if 0 < debit < width:
            max_risk = debit * 100.0
            max_reward = (width - debit) * 100.0
            rr = max_reward / max_risk
            if rr >= float(params["vertical_min_reward_risk"]):
                rr_full = max(0.01, float(params["vertical_full_reward_risk_score"]))
                reward_risk_score = min(1.0, rr / rr_full)
                debit_efficiency = max(0.0, min(1.0, 1.0 - debit / width))
                short_spread_score = max(0.0, 1.0 - spread_pct / max(1e-9, float(params["option_max_spread_pct"])))
                risk_reduction_score = max(0.0, min(1.0, 1.0 - max_risk / max(1e-9, long_ask * 100.0)))
                # Historical short-leg OI is not available in synthetic mode;
                # omit its 15% live weight and renormalize the observable 85%.
                vertical_quality = 100.0 * (
                    reward_risk_score * 0.30
                    + debit_efficiency * 0.20
                    + short_spread_score * 0.15
                    + risk_reduction_score * 0.20
                ) / 0.85
                score = combined * 0.65 + vertical_quality * 0.35
                vertical = SyntheticStructure(
                    direction=direction,
                    decision="BULL CALL SPREAD" if option_type == "call" else "BEAR PUT SPREAD",
                    entry_spot=entry_spot,
                    expiration_date=expiry,
                    long_strike=long_strike,
                    short_strike=short_strike,
                    iv=vol,
                    entry_debit=debit,
                    max_risk=max_risk,
                    max_reward=max_reward,
                    reward_risk=rr,
                    stock_score=stock_score,
                    option_score=opt_score,
                    structure_score=score,
                    long_delta=long_delta,
                    short_delta=short_delta,
                )
    except ValueError:
        vertical = None

    choices = [long_structure] + ([vertical] if vertical is not None else [])
    best = max(choices, key=lambda s: (s.structure_score, s.reward_risk if math.isfinite(s.reward_risk) else 1e9))
    return best


def executable_structure_value(structure: SyntheticStructure, spot: float, day: pd.Timestamp, cfg: dict[str, Any]) -> float:
    remaining_days = max(0, (pd.Timestamp(structure.expiration_date).normalize() - pd.Timestamp(day).normalize()).days)
    t = remaining_days / 365.0
    option_type = "call" if structure.direction == "BULLISH" else "put"
    rate = float(cfg["risk_free_rate"])
    spread_pct = float(cfg["synthetic_option_spread_pct"])
    slippage = float(cfg["synthetic_option_slippage_pct"])
    long_mid, _ = bs_price_delta(spot, structure.long_strike, t, rate, structure.iv, option_type)
    long_bid, _ = synthetic_quote(long_mid, spread_pct, slippage)
    if structure.short_strike is None:
        return max(0.0, long_bid)
    short_mid, _ = bs_price_delta(spot, structure.short_strike, t, rate, structure.iv, option_type)
    _, short_ask = synthetic_quote(short_mid, spread_pct, slippage)
    return max(0.0, long_bid - short_ask)


@dataclass
class Candidate:
    signal_date: pd.Timestamp
    entry_date: pd.Timestamp
    symbol: str
    direction: str
    structure: SyntheticStructure


@dataclass
class OpenTrade:
    trade_id: str
    fold_id: int
    symbol: str
    direction: str
    decision: str
    signal_date: pd.Timestamp
    entry_date: pd.Timestamp
    expiration_date: pd.Timestamp
    structure: SyntheticStructure
    quantity: int
    entry_value: float
    initial_risk: float
    last_value: float
    mae_pct: float = 0.0
    mfe_pct: float = 0.0


@dataclass
class BacktestResult:
    trades: pd.DataFrame
    equity: pd.DataFrame
    metrics: dict[str, Any]


def build_candidates(
    features: pd.DataFrame,
    universe: UniverseProvider,
    params: dict[str, Any],
    cfg: dict[str, Any],
) -> pd.DataFrame:
    dates = sorted(features["date"].dropna().unique())
    next_date = {pd.Timestamp(dates[i]): pd.Timestamp(dates[i + 1]) for i in range(len(dates) - 1)}
    by_date = {pd.Timestamp(d): frame.copy() for d, frame in features.groupby("date")}
    output = []
    screen_mode = str(cfg.get("universe_screen_mode", "historical_proxy_screener"))
    for raw_day in dates[:-1]:
        day = pd.Timestamp(raw_day)
        active = universe.active_symbols(day)
        frame = by_date[day]
        frame = frame[frame["symbol"].isin(active)].copy()
        if frame.empty:
            continue
        frame = historical_proxy_screen(frame, params, screen_mode)
        bullish, bearish = score_day(frame, params)
        entry_day = next_date[day]
        entry_frame = by_date.get(entry_day)
        if entry_frame is None:
            continue
        entry_map = entry_frame.set_index("symbol")
        for direction, scored in [("BULLISH", bullish), ("BEARISH", bearish)]:
            for _, row in scored.iterrows():
                symbol = str(row["symbol"])
                if symbol not in entry_map.index:
                    continue
                entry_row = entry_map.loc[symbol]
                if isinstance(entry_row, pd.DataFrame):
                    entry_row = entry_row.iloc[-1]
                entry_spot = _finite(entry_row["open"], 0.0)
                rv20 = _finite(row["volatility20"], 0.0)
                if entry_spot <= 0 or rv20 <= 0:
                    continue
                structure = build_synthetic_structure(
                    direction=direction,
                    stock_score=float(row["score"]),
                    entry_spot=entry_spot,
                    realized_vol20=rv20,
                    entry_date=entry_day,
                    params=params,
                    cfg=cfg,
                )
                if structure is None:
                    continue
                output.append(
                    {
                        "signal_date": day,
                        "entry_date": entry_day,
                        "symbol": symbol,
                        "direction": direction,
                        "decision": structure.decision,
                        "expiration_date": structure.expiration_date,
                        "entry_spot": entry_spot,
                        "long_strike": structure.long_strike,
                        "short_strike": structure.short_strike,
                        "iv": structure.iv,
                        "entry_debit": structure.entry_debit,
                        "max_risk": structure.max_risk,
                        "max_reward": structure.max_reward,
                        "reward_risk": structure.reward_risk,
                        "stock_score": structure.stock_score,
                        "option_score_proxy": structure.option_score,
                        "structure_score_proxy": structure.structure_score,
                        "long_delta": structure.long_delta,
                        "short_delta": structure.short_delta,
                        "model_note": structure.model_note,
                    }
                )
    result = pd.DataFrame(output)
    if result.empty:
        return result
    return result.sort_values(["entry_date", "structure_score_proxy", "stock_score"], ascending=[True, False, False]).reset_index(drop=True)


def row_to_structure(row: pd.Series) -> SyntheticStructure:
    max_reward = row.get("max_reward", float("nan"))
    if pd.isna(max_reward):
        max_reward = float("inf")
    return SyntheticStructure(
        direction=str(row["direction"]),
        decision=str(row["decision"]),
        entry_spot=float(row["entry_spot"]),
        expiration_date=pd.Timestamp(row["expiration_date"]),
        long_strike=float(row["long_strike"]),
        short_strike=None if pd.isna(row.get("short_strike")) else float(row["short_strike"]),
        iv=float(row["iv"]),
        entry_debit=float(row["entry_debit"]),
        max_risk=float(row["max_risk"]),
        max_reward=float(max_reward),
        reward_risk=float(row["reward_risk"]),
        stock_score=float(row["stock_score"]),
        option_score=float(row["option_score_proxy"]),
        structure_score=float(row["structure_score_proxy"]),
        long_delta=float(row["long_delta"]),
        short_delta=None if pd.isna(row.get("short_delta")) else float(row["short_delta"]),
        model_note=str(row.get("model_note", "SYNTHETIC_BS_PROXY")),
    )


def thesis_state(direction: str, row: pd.Series) -> tuple[str, bool]:
    price = _finite(row.get("close"), 0.0)
    sma20 = _finite(row.get("sma20"), 0.0)
    momentum20 = _finite(row.get("momentum20"), 0.0)
    if price <= 0 or sma20 <= 0:
        return "UNKNOWN", False
    if direction == "BULLISH":
        price_break = price < sma20
        momentum_break = momentum20 <= 0
    else:
        price_break = price > sma20
        momentum_break = momentum20 >= 0
    invalid = price_break and momentum_break
    if invalid:
        return "INVALID", True
    if price_break or momentum_break:
        return "WEAKENING", False
    return "VALID", False


def _score_bucket(score: float) -> str:
    if score < 70:
        return "<70"
    if score < 75:
        return "70-75"
    if score < 80:
        return "75-80"
    if score < 85:
        return "80-85"
    return "85+"


def metrics_from_trades(trades: pd.DataFrame, equity: pd.DataFrame, initial_equity: float) -> dict[str, Any]:
    if trades.empty:
        return {
            "completed_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "total_pnl": 0.0,
            "return_pct": 0.0,
            "expectancy": None,
            "profit_factor": None,
            "max_drawdown_dollars": 0.0,
            "max_drawdown_pct": 0.0,
            "average_trade_return_pct": None,
            "average_holding_days": None,
            "average_mae_pct": None,
            "average_mfe_pct": None,
        }
    pnl = trades["realized_pnl"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    gross_profit = float(wins.sum())
    gross_loss = float(-losses.sum())
    pf = None if gross_loss == 0 and gross_profit == 0 else (float("inf") if gross_loss == 0 else gross_profit / gross_loss)
    max_dd = 0.0
    max_dd_pct = 0.0
    if not equity.empty:
        curve = equity["equity"].astype(float)
        peak = curve.cummax()
        dd = curve - peak
        dd_pct = np.where(peak > 0, dd / peak, 0.0)
        max_dd = float(-dd.min())
        max_dd_pct = float(-np.min(dd_pct))
    return {
        "completed_trades": int(len(trades)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate": float((pnl > 0).mean()),
        "total_pnl": float(pnl.sum()),
        "return_pct": float(pnl.sum() / initial_equity),
        "expectancy": float(pnl.mean()),
        "profit_factor": pf,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "max_drawdown_dollars": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "average_trade_return_pct": float(trades["return_pct"].mean()),
        "average_holding_days": float(trades["holding_days"].mean()),
        "average_mae_pct": float(trades["mae_pct"].mean()),
        "average_mfe_pct": float(trades["mfe_pct"].mean()),
    }


def simulate_period(
    features: pd.DataFrame,
    candidates: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    params: dict[str, Any],
    cfg: dict[str, Any],
    combo: dict[str, float],
    fold_id: int,
) -> BacktestResult:
    initial_equity = float(cfg["initial_equity"])
    realized_total = 0.0
    open_trades: list[OpenTrade] = []
    closed_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    date_frames = {pd.Timestamp(d): f.set_index("symbol") for d, f in features.groupby("date")}
    all_dates = sorted(d for d in date_frames if pd.Timestamp(start_date) <= d <= pd.Timestamp(end_date))
    candidates_by_entry = {
        pd.Timestamp(d): f.copy()
        for d, f in candidates[(candidates["entry_date"] >= start_date) & (candidates["entry_date"] <= end_date)].groupby("entry_date")
    } if not candidates.empty else {}

    min_structure_score = float(combo["trade_structure_min_score"])
    profit_target = float(combo["exit_profit_target_pct"])
    max_loss = float(combo["exit_max_loss_pct"])
    same_day_priority = str(cfg.get("same_day_barrier_priority", "stop")).lower()

    for day in all_dates:
        day_frame = date_frames[day]
        still_open: list[OpenTrade] = []
        unrealized_total = 0.0
        for trade in open_trades:
            if trade.symbol not in day_frame.index:
                still_open.append(trade)
                continue
            bar = day_frame.loc[trade.symbol]
            if isinstance(bar, pd.DataFrame):
                bar = bar.iloc[-1]
            favorable_spot = float(bar["high"] if trade.direction == "BULLISH" else bar["low"])
            adverse_spot = float(bar["low"] if trade.direction == "BULLISH" else bar["high"])
            best_value = executable_structure_value(trade.structure, favorable_spot, day, cfg)
            worst_value = executable_structure_value(trade.structure, adverse_spot, day, cfg)
            close_value = executable_structure_value(trade.structure, float(bar["close"]), day, cfg)
            best_pct = best_value / trade.entry_value - 1.0
            worst_pct = worst_value / trade.entry_value - 1.0
            trade.mfe_pct = max(trade.mfe_pct, best_pct)
            trade.mae_pct = min(trade.mae_pct, worst_pct)
            target_hit = best_pct >= profit_target
            stop_hit = worst_pct <= -max_loss
            days_held = max(0, (pd.Timestamp(day) - trade.entry_date).days)
            dte = max(0, (trade.expiration_date - pd.Timestamp(day)).days)
            state, invalid = thesis_state(trade.direction, bar)
            exit_reason = None
            exit_value = None
            if target_hit and stop_hit:
                if same_day_priority == "target":
                    exit_reason = "PROFIT_TARGET"
                    exit_value = trade.entry_value * (1.0 + profit_target)
                else:
                    exit_reason = "MAX_LOSS"
                    exit_value = trade.entry_value * (1.0 - max_loss)
            elif stop_hit:
                exit_reason = "MAX_LOSS"
                exit_value = trade.entry_value * (1.0 - max_loss)
            elif target_hit:
                exit_reason = "PROFIT_TARGET"
                exit_value = trade.entry_value * (1.0 + profit_target)
            elif dte <= int(params["exit_dte_days"]):
                exit_reason = "DTE_EXIT"
                exit_value = close_value
            elif days_held >= int(params["exit_max_holding_days"]):
                exit_reason = "MAX_HOLD"
                exit_value = close_value
            elif bool(params.get("exit_thesis_invalidation_enabled", True)) and invalid:
                exit_reason = "THESIS_INVALID"
                exit_value = close_value

            if exit_reason is not None:
                barrier_slip = max(0.0, float(cfg.get("barrier_fill_extra_slippage_pct", 0.0)))
                if exit_reason in {"PROFIT_TARGET", "MAX_LOSS"}:
                    exit_value = max(0.0, float(exit_value) * (1.0 - barrier_slip))
                pnl = (float(exit_value) - trade.entry_value) * 100.0 * trade.quantity
                realized_total += pnl
                closed_rows.append(
                    {
                        "trade_id": trade.trade_id,
                        "fold_id": fold_id,
                        "symbol": trade.symbol,
                        "direction": trade.direction,
                        "decision": trade.decision,
                        "signal_date": trade.signal_date,
                        "entry_date": trade.entry_date,
                        "exit_date": day,
                        "expiration_date": trade.expiration_date,
                        "quantity": trade.quantity,
                        "entry_spot": trade.structure.entry_spot,
                        "long_strike": trade.structure.long_strike,
                        "short_strike": trade.structure.short_strike,
                        "iv": trade.structure.iv,
                        "entry_value": trade.entry_value,
                        "exit_value": float(exit_value),
                        "realized_pnl": pnl,
                        "return_pct": float(exit_value) / trade.entry_value - 1.0,
                        "holding_days": days_held,
                        "exit_reason": exit_reason,
                        "thesis_state_at_exit": state,
                        "mae_pct": trade.mae_pct,
                        "mfe_pct": trade.mfe_pct,
                        "stock_score": trade.structure.stock_score,
                        "option_score_proxy": trade.structure.option_score,
                        "structure_score_proxy": trade.structure.structure_score,
                        "score_bucket": _score_bucket(trade.structure.structure_score),
                        "initial_risk": trade.initial_risk,
                        "model_note": trade.structure.model_note,
                    }
                )
            else:
                trade.last_value = close_value
                unrealized_total += (close_value - trade.entry_value) * 100.0 * trade.quantity
                still_open.append(trade)
        open_trades = still_open

        # Open new candidates after existing positions have been marked/exited.
        day_candidates = candidates_by_entry.get(day, pd.DataFrame())
        if not day_candidates.empty:
            day_candidates = day_candidates[day_candidates["structure_score_proxy"] >= min_structure_score].copy()
            day_candidates = day_candidates.sort_values(["structure_score_proxy", "stock_score"], ascending=False)
            marked_equity = initial_equity + realized_total + unrealized_total
            options_bp = marked_equity
            per_trade_budget = min(
                marked_equity * float(params["position_risk_pct_equity"]),
                float(params["position_max_risk_dollars"]),
                options_bp * float(params["position_max_options_bp_pct_per_trade"]),
            )
            total_run_budget = min(
                marked_equity * float(params["position_total_new_risk_pct_equity"]),
                options_bp * float(params["position_max_options_bp_pct_total"]),
            )
            remaining_run = total_run_budget
            max_new = int(params["position_max_alerts_per_run"])
            opened_today = 0
            allocated_today_total = 0.0
            allocated_today_direction: dict[str, float] = {}
            allocated_today_expiration: dict[pd.Timestamp, float] = {}
            for _, row in day_candidates.iterrows():
                if opened_today >= max_new:
                    break
                symbol = str(row["symbol"])
                if any(t.symbol == symbol for t in open_trades):
                    continue
                if len(open_trades) + opened_today >= int(params["portfolio_max_active_tracked_setups"]):
                    break
                structure = row_to_structure(row)
                per_contract_risk = structure.max_risk
                current_total_risk = sum(t.initial_risk for t in open_trades) + allocated_today_total
                current_direction_risk = (
                    sum(t.initial_risk for t in open_trades if t.direction == structure.direction)
                    + allocated_today_direction.get(structure.direction, 0.0)
                )
                current_exp_risk = (
                    sum(t.initial_risk for t in open_trades if t.expiration_date == structure.expiration_date)
                    + allocated_today_expiration.get(structure.expiration_date, 0.0)
                )
                total_capacity = max(0.0, marked_equity * float(params["portfolio_max_active_tracked_risk_pct_equity"]) - current_total_risk)
                direction_capacity = max(0.0, marked_equity * float(params["portfolio_max_directional_tracked_risk_pct_equity"]) - current_direction_risk)
                expiration_capacity = max(0.0, marked_equity * float(params["portfolio_max_expiration_tracked_risk_pct_equity"]) - current_exp_risk)
                trade_budget = min(per_trade_budget, remaining_run, total_capacity, direction_capacity, expiration_capacity)
                qty = min(int(params["position_max_contracts_per_trade"]), math.floor(trade_budget / max(1e-9, per_contract_risk)))
                if qty < 1:
                    continue
                initial_risk = per_contract_risk * qty
                trade_id = hashlib.sha256(f"{fold_id}|{day.date()}|{symbol}|{structure.decision}|{structure.long_strike}|{structure.short_strike}".encode()).hexdigest()[:20]
                new_trade = OpenTrade(
                    trade_id=trade_id,
                    fold_id=fold_id,
                    symbol=symbol,
                    direction=structure.direction,
                    decision=structure.decision,
                    signal_date=pd.Timestamp(row["signal_date"]),
                    entry_date=day,
                    expiration_date=structure.expiration_date,
                    structure=structure,
                    quantity=qty,
                    entry_value=structure.entry_debit,
                    initial_risk=initial_risk,
                    last_value=structure.entry_debit,
                )

                # The live strategy enters during the session, so a daily-bar
                # research model must permit the stop/target to be reached on
                # the entry day after the modeled open fill. Ignoring this
                # would create a favorable holding-period bias.
                same_day_closed = False
                if symbol in day_frame.index:
                    entry_bar = day_frame.loc[symbol]
                    if isinstance(entry_bar, pd.DataFrame):
                        entry_bar = entry_bar.iloc[-1]
                    favorable_spot = float(entry_bar["high"] if structure.direction == "BULLISH" else entry_bar["low"])
                    adverse_spot = float(entry_bar["low"] if structure.direction == "BULLISH" else entry_bar["high"])
                    best_value = executable_structure_value(structure, favorable_spot, day, cfg)
                    worst_value = executable_structure_value(structure, adverse_spot, day, cfg)
                    best_pct = best_value / new_trade.entry_value - 1.0
                    worst_pct = worst_value / new_trade.entry_value - 1.0
                    new_trade.mfe_pct = max(0.0, best_pct)
                    new_trade.mae_pct = min(0.0, worst_pct)
                    target_hit = best_pct >= profit_target
                    stop_hit = worst_pct <= -max_loss
                    same_day_reason = None
                    same_day_value = None
                    if target_hit and stop_hit:
                        if same_day_priority == "target":
                            same_day_reason = "PROFIT_TARGET"
                            same_day_value = new_trade.entry_value * (1.0 + profit_target)
                        else:
                            same_day_reason = "MAX_LOSS"
                            same_day_value = new_trade.entry_value * (1.0 - max_loss)
                    elif stop_hit:
                        same_day_reason = "MAX_LOSS"
                        same_day_value = new_trade.entry_value * (1.0 - max_loss)
                    elif target_hit:
                        same_day_reason = "PROFIT_TARGET"
                        same_day_value = new_trade.entry_value * (1.0 + profit_target)
                    if same_day_reason is not None:
                        barrier_slip = max(0.0, float(cfg.get("barrier_fill_extra_slippage_pct", 0.0)))
                        same_day_value = max(0.0, float(same_day_value) * (1.0 - barrier_slip))
                        pnl = (same_day_value - new_trade.entry_value) * 100.0 * new_trade.quantity
                        realized_total += pnl
                        closed_rows.append(
                            {
                                "trade_id": new_trade.trade_id,
                                "fold_id": fold_id,
                                "symbol": new_trade.symbol,
                                "direction": new_trade.direction,
                                "decision": new_trade.decision,
                                "signal_date": new_trade.signal_date,
                                "entry_date": day,
                                "exit_date": day,
                                "expiration_date": new_trade.expiration_date,
                                "quantity": new_trade.quantity,
                                "entry_spot": structure.entry_spot,
                                "long_strike": structure.long_strike,
                                "short_strike": structure.short_strike,
                                "iv": structure.iv,
                                "entry_value": new_trade.entry_value,
                                "exit_value": same_day_value,
                                "realized_pnl": pnl,
                                "return_pct": same_day_value / new_trade.entry_value - 1.0,
                                "holding_days": 0,
                                "exit_reason": same_day_reason,
                                "thesis_state_at_exit": "ENTRY_DAY",
                                "mae_pct": new_trade.mae_pct,
                                "mfe_pct": new_trade.mfe_pct,
                                "stock_score": structure.stock_score,
                                "option_score_proxy": structure.option_score,
                                "structure_score_proxy": structure.structure_score,
                                "score_bucket": _score_bucket(structure.structure_score),
                                "initial_risk": initial_risk,
                                "model_note": structure.model_note,
                            }
                        )
                        same_day_closed = True

                if not same_day_closed:
                    open_trades.append(new_trade)
                remaining_run -= initial_risk
                allocated_today_total += initial_risk
                allocated_today_direction[structure.direction] = allocated_today_direction.get(structure.direction, 0.0) + initial_risk
                allocated_today_expiration[structure.expiration_date] = allocated_today_expiration.get(structure.expiration_date, 0.0) + initial_risk
                opened_today += 1

        # End-of-day mark after entries.
        eod_unrealized = 0.0
        for trade in open_trades:
            if trade.symbol not in day_frame.index:
                continue
            bar = day_frame.loc[trade.symbol]
            if isinstance(bar, pd.DataFrame):
                bar = bar.iloc[-1]
            value = executable_structure_value(trade.structure, float(bar["close"]), day, cfg)
            trade.last_value = value
            eod_unrealized += (value - trade.entry_value) * 100.0 * trade.quantity
        equity_rows.append(
            {
                "date": day,
                "fold_id": fold_id,
                "realized_pnl": realized_total,
                "unrealized_pnl": eod_unrealized,
                "equity": initial_equity + realized_total + eod_unrealized,
                "open_positions": len(open_trades),
            }
        )

    # Force-close positions at test/train boundary using last available close.
    if all_dates:
        final_day = all_dates[-1]
        final_frame = date_frames[final_day]
        for trade in open_trades:
            if trade.symbol not in final_frame.index:
                continue
            bar = final_frame.loc[trade.symbol]
            if isinstance(bar, pd.DataFrame):
                bar = bar.iloc[-1]
            exit_value = executable_structure_value(trade.structure, float(bar["close"]), final_day, cfg)
            pnl = (exit_value - trade.entry_value) * 100.0 * trade.quantity
            realized_total += pnl
            days_held = max(0, (final_day - trade.entry_date).days)
            closed_rows.append(
                {
                    "trade_id": trade.trade_id,
                    "fold_id": fold_id,
                    "symbol": trade.symbol,
                    "direction": trade.direction,
                    "decision": trade.decision,
                    "signal_date": trade.signal_date,
                    "entry_date": trade.entry_date,
                    "exit_date": final_day,
                    "expiration_date": trade.expiration_date,
                    "quantity": trade.quantity,
                    "entry_spot": trade.structure.entry_spot,
                    "long_strike": trade.structure.long_strike,
                    "short_strike": trade.structure.short_strike,
                    "iv": trade.structure.iv,
                    "entry_value": trade.entry_value,
                    "exit_value": exit_value,
                    "realized_pnl": pnl,
                    "return_pct": exit_value / trade.entry_value - 1.0,
                    "holding_days": days_held,
                    "exit_reason": "PERIOD_END_MARK",
                    "thesis_state_at_exit": "N/A",
                    "mae_pct": trade.mae_pct,
                    "mfe_pct": trade.mfe_pct,
                    "stock_score": trade.structure.stock_score,
                    "option_score_proxy": trade.structure.option_score,
                    "structure_score_proxy": trade.structure.structure_score,
                    "score_bucket": _score_bucket(trade.structure.structure_score),
                    "initial_risk": trade.initial_risk,
                    "model_note": trade.structure.model_note,
                }
            )
        if equity_rows:
            equity_rows[-1]["realized_pnl"] = realized_total
            equity_rows[-1]["unrealized_pnl"] = 0.0
            equity_rows[-1]["equity"] = initial_equity + realized_total
            equity_rows[-1]["open_positions"] = 0

    trades = pd.DataFrame(closed_rows)
    equity = pd.DataFrame(equity_rows)
    metrics = metrics_from_trades(trades, equity, initial_equity)
    return BacktestResult(trades=trades, equity=equity, metrics=metrics)


def objective_value(metrics: dict[str, Any], cfg: dict[str, Any]) -> float:
    min_trades = int(cfg.get("minimum_training_trades", 10))
    if int(metrics.get("completed_trades", 0)) < min_trades:
        return -1e12 + float(metrics.get("completed_trades", 0))
    objective = str(cfg.get("optimization_objective", "calmar")).lower()
    if objective == "net_profit":
        return float(metrics.get("total_pnl", 0.0))
    if objective == "expectancy":
        return float(metrics.get("expectancy") or -1e9)
    ret = float(metrics.get("return_pct", 0.0))
    dd = float(metrics.get("max_drawdown_pct", 0.0))
    if dd <= 1e-9:
        return ret * 100.0
    return ret / dd


def optimization_combos(cfg: dict[str, Any], params: dict[str, Any]) -> list[dict[str, float]]:
    grid = cfg.get("optimization_grid", {}) or {}
    structure_scores = grid.get("trade_structure_min_score", [params["trade_structure_min_score"]])
    profit_targets = grid.get("exit_profit_target_pct", [params["exit_profit_target_pct"]])
    max_losses = grid.get("exit_max_loss_pct", [params["exit_max_loss_pct"]])
    combos = []
    for score, target, loss in itertools.product(structure_scores, profit_targets, max_losses):
        combos.append(
            {
                "trade_structure_min_score": float(score),
                "exit_profit_target_pct": float(target),
                "exit_max_loss_pct": float(loss),
            }
        )
    return combos


def make_folds(dates: list[pd.Timestamp], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    train_days = int(cfg["train_trading_days"])
    test_days = int(cfg["test_trading_days"])
    step_days = int(cfg.get("step_trading_days", test_days))
    folds = []
    start = 0
    fold_id = 1
    while start + train_days + test_days <= len(dates):
        train_slice = dates[start : start + train_days]
        test_slice = dates[start + train_days : start + train_days + test_days]
        folds.append(
            {
                "fold_id": fold_id,
                "train_start": train_slice[0],
                "train_end": train_slice[-1],
                "test_start": test_slice[0],
                "test_end": test_slice[-1],
            }
        )
        fold_id += 1
        start += step_days
    return folds


def grouped_trade_metrics(trades: pd.DataFrame, column: str) -> dict[str, Any]:
    if trades.empty or column not in trades.columns:
        return {}
    out = {}
    for key, group in trades.groupby(column, dropna=False):
        pnl = group["realized_pnl"].astype(float)
        losses = -pnl[pnl < 0].sum()
        wins = pnl[pnl > 0].sum()
        out[str(key)] = {
            "trades": int(len(group)),
            "win_rate": float((pnl > 0).mean()),
            "total_pnl": float(pnl.sum()),
            "expectancy": float(pnl.mean()),
            "profit_factor": None if losses == 0 and wins == 0 else (float("inf") if losses == 0 else float(wins / losses)),
            "average_return_pct": float(group["return_pct"].mean()),
        }
    return out


def run_walk_forward(
    bars: pd.DataFrame,
    universe: UniverseProvider,
    params: dict[str, Any],
    cfg: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    features = build_feature_table(bars)
    candidates = build_candidates(features, universe, params, cfg)
    if candidates.empty:
        raise RuntimeError("no synthetic candidates were generated; check universe, dates, and liquidity thresholds")
    unique_dates = sorted(pd.Timestamp(d) for d in features["date"].unique())
    start_cfg = pd.Timestamp(cfg["start_date"]) if cfg.get("start_date") else unique_dates[0]
    end_cfg = pd.Timestamp(cfg["end_date"]) if cfg.get("end_date") else unique_dates[-1]
    research_dates = [d for d in unique_dates if start_cfg <= d <= end_cfg]
    folds = make_folds(research_dates, cfg)
    if not folds:
        raise RuntimeError("not enough trading days for configured train/test windows")

    combo_rows = []
    fold_rows = []
    oos_trades = []
    oos_equity = []
    baseline_trades = []
    baseline_equity = []
    combos = optimization_combos(cfg, params)
    baseline_combo = {
        "trade_structure_min_score": float(params["trade_structure_min_score"]),
        "exit_profit_target_pct": float(params["exit_profit_target_pct"]),
        "exit_max_loss_pct": float(params["exit_max_loss_pct"]),
    }

    for fold in folds:
        best_combo = None
        best_objective = -float("inf")
        best_train_metrics = None
        for combo in combos:
            result = simulate_period(
                features, candidates,
                fold["train_start"], fold["train_end"],
                params, cfg, combo, fold_id=fold["fold_id"],
            )
            objective = objective_value(result.metrics, cfg)
            combo_rows.append(
                {
                    "fold_id": fold["fold_id"],
                    "train_start": fold["train_start"],
                    "train_end": fold["train_end"],
                    **combo,
                    "objective": objective,
                    **{f"train_{k}": v for k, v in result.metrics.items()},
                }
            )
            if objective > best_objective:
                best_objective = objective
                best_combo = combo
                best_train_metrics = result.metrics
        assert best_combo is not None
        test = simulate_period(
            features, candidates,
            fold["test_start"], fold["test_end"],
            params, cfg, best_combo, fold_id=fold["fold_id"],
        )
        baseline_test = simulate_period(
            features, candidates,
            fold["test_start"], fold["test_end"],
            params, cfg, baseline_combo, fold_id=fold["fold_id"],
        )
        fold_rows.append(
            {
                **fold,
                **{f"selected_{k}": v for k, v in best_combo.items()},
                "training_objective": best_objective,
                **{f"train_{k}": v for k, v in (best_train_metrics or {}).items()},
                **{f"test_{k}": v for k, v in test.metrics.items()},
                **{f"baseline_test_{k}": v for k, v in baseline_test.metrics.items()},
            }
        )
        if not test.trades.empty:
            test.trades = test.trades.copy()
            test.trades["walk_forward_segment"] = "OOS"
            oos_trades.append(test.trades)
        if not test.equity.empty:
            eq = test.equity.copy()
            eq["walk_forward_segment"] = "OOS"
            oos_equity.append(eq)
        if not baseline_test.trades.empty:
            bt = baseline_test.trades.copy()
            bt["walk_forward_segment"] = "OOS_BASELINE"
            baseline_trades.append(bt)
        if not baseline_test.equity.empty:
            be = baseline_test.equity.copy()
            be["walk_forward_segment"] = "OOS_BASELINE"
            baseline_equity.append(be)

    trades = _concat_frames(oos_trades)
    equity = _concat_frames(oos_equity)
    baseline_trades_df = _concat_frames(baseline_trades)
    baseline_equity_df = _concat_frames(baseline_equity)
    fold_df = pd.DataFrame(fold_rows)
    grid_df = pd.DataFrame(combo_rows)
    # Aggregate OOS trades independently of per-fold reset equity. For return,
    # stitch fold percentage returns multiplicatively.
    fold_returns = fold_df["test_return_pct"].fillna(0.0).astype(float) if not fold_df.empty else pd.Series(dtype=float)
    stitched_return = float(np.prod(1.0 + fold_returns) - 1.0) if len(fold_returns) else 0.0
    total_pnl = float(trades["realized_pnl"].sum()) if not trades.empty else 0.0
    losses = float(-trades.loc[trades["realized_pnl"] < 0, "realized_pnl"].sum()) if not trades.empty else 0.0
    wins = float(trades.loc[trades["realized_pnl"] > 0, "realized_pnl"].sum()) if not trades.empty else 0.0
    baseline_fold_returns = fold_df["baseline_test_return_pct"].fillna(0.0).astype(float) if not fold_df.empty else pd.Series(dtype=float)
    baseline_stitched_return = float(np.prod(1.0 + baseline_fold_returns) - 1.0) if len(baseline_fold_returns) else 0.0
    baseline_losses = float(-baseline_trades_df.loc[baseline_trades_df["realized_pnl"] < 0, "realized_pnl"].sum()) if not baseline_trades_df.empty else 0.0
    baseline_wins = float(baseline_trades_df.loc[baseline_trades_df["realized_pnl"] > 0, "realized_pnl"].sum()) if not baseline_trades_df.empty else 0.0
    selected_frequency = {}
    if not fold_df.empty:
        freq_cols = ["selected_trade_structure_min_score", "selected_exit_profit_target_pct", "selected_exit_max_loss_pct"]
        freq = fold_df.groupby(freq_cols, dropna=False).size().sort_values(ascending=False)
        selected_frequency = {"|".join(map(str, idx if isinstance(idx, tuple) else (idx,))): int(count) for idx, count in freq.items()}
    summary = {
        "framework_version": "1.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_hash": config_hash(cfg, params),
        "strategy_source": str(cfg.get("strategy_path", "strategy.py")),
        "bars_rows": int(len(bars)),
        "symbols": int(bars["symbol"].nunique()),
        "candidate_count": int(len(candidates)),
        "fold_count": int(len(fold_df)),
        "oos_completed_trades": int(len(trades)),
        "oos_wins": int((trades["realized_pnl"] > 0).sum()) if not trades.empty else 0,
        "oos_losses": int((trades["realized_pnl"] < 0).sum()) if not trades.empty else 0,
        "oos_win_rate": float((trades["realized_pnl"] > 0).mean()) if not trades.empty else None,
        "oos_total_pnl_sum_of_fold_dollars": total_pnl,
        "oos_stitched_return_pct": stitched_return,
        "oos_expectancy_dollars": float(trades["realized_pnl"].mean()) if not trades.empty else None,
        "oos_profit_factor": None if losses == 0 and wins == 0 else (float("inf") if losses == 0 else wins / losses),
        "baseline_live_parameters": baseline_combo,
        "baseline_oos_completed_trades": int(len(baseline_trades_df)),
        "baseline_oos_win_rate": float((baseline_trades_df["realized_pnl"] > 0).mean()) if not baseline_trades_df.empty else None,
        "baseline_oos_stitched_return_pct": baseline_stitched_return,
        "baseline_oos_expectancy_dollars": float(baseline_trades_df["realized_pnl"].mean()) if not baseline_trades_df.empty else None,
        "baseline_oos_profit_factor": None if baseline_losses == 0 and baseline_wins == 0 else (float("inf") if baseline_losses == 0 else baseline_wins / baseline_losses),
        "selected_parameter_frequency": selected_frequency,
        "by_structure": grouped_trade_metrics(trades, "decision"),
        "by_direction": grouped_trade_metrics(trades, "direction"),
        "by_score_bucket": grouped_trade_metrics(trades, "score_bucket"),
        "by_exit_reason": grouped_trade_metrics(trades, "exit_reason"),
        "research_limitations": [
            "Historical stock-score replay uses completed daily bars and is designed to avoid future-bar lookahead.",
            "The live intraday Alpaca screener cannot be reconstructed exactly from daily history; historical_proxy_screener uses daily volume and movers as a proxy unless membership_all is selected.",
            "V1 option economics use Black-Scholes with realized-vol-derived IV plus explicit synthetic spread/slippage assumptions; OI, quote size, daily option volume, historical Greeks and exact historical NBBO are not claimed.",
            "Event/earnings replay is not included unless a dated historical event dataset is added in a future adapter.",
            "V1 results are a research screen, not evidence of exact live fill performance or a guarantee of profitability.",
        ],
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(candidates, output_dir / "walk_forward_candidates.csv")
    atomic_write_csv(fold_df, output_dir / "walk_forward_folds.csv")
    atomic_write_csv(grid_df, output_dir / "walk_forward_training_grid.csv")
    atomic_write_csv(trades, output_dir / "walk_forward_trades.csv")
    atomic_write_csv(equity, output_dir / "walk_forward_equity.csv")
    atomic_write_csv(baseline_trades_df, output_dir / "walk_forward_baseline_trades.csv")
    atomic_write_csv(baseline_equity_df, output_dir / "walk_forward_baseline_equity.csv")
    atomic_write_json(summary, output_dir / "walk_forward_summary.json")
    return summary


def load_config(path: Path) -> dict[str, Any]:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    return cfg


def command_download(args: argparse.Namespace) -> int:
    cfg = load_config(Path(args.config))
    universe = UniverseProvider.from_csv(Path(args.universe))
    downloader = AlpacaStockBarsDownloader()
    bars = downloader.download(universe.symbols(), cfg["download_start_date"], cfg["download_end_date"])
    if bars.empty:
        print("No bars returned.", file=sys.stderr)
        return 2
    path = Path(args.output)
    atomic_write_csv(bars, path)
    print(f"Downloaded {len(bars):,} daily bars across {bars['symbol'].nunique():,} symbols -> {path}")
    return 0


def command_run(args: argparse.Namespace) -> int:
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    strategy_path = Path(args.strategy or cfg.get("strategy_path", "strategy.py"))
    if not strategy_path.is_absolute():
        strategy_path = (cfg_path.parent / strategy_path).resolve()
    params = load_strategy_parameters(strategy_path)
    bars = normalize_bars(pd.read_csv(args.bars))
    universe = UniverseProvider.from_csv(Path(args.universe))
    out = Path(args.output_dir)
    cfg = dict(cfg)
    cfg["strategy_path"] = str(strategy_path)
    summary = run_walk_forward(bars, universe, params, cfg, out)
    print(json.dumps(summary, indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Walk-forward research harness for StockSuggestionStrategy")
    sub = parser.add_subparsers(dest="command", required=True)

    d = sub.add_parser("download", help="download historical daily stock bars from Alpaca")
    d.add_argument("--config", default="walk_forward_config.json")
    d.add_argument("--universe", default="walk_forward_universe.csv")
    d.add_argument("--output", default="walk_forward_bars.csv")
    d.set_defaults(func=command_download)

    r = sub.add_parser("run", help="run train/test walk-forward analysis")
    r.add_argument("--config", default="walk_forward_config.json")
    r.add_argument("--strategy", default=None)
    r.add_argument("--bars", default="walk_forward_bars.csv")
    r.add_argument("--universe", default="walk_forward_universe.csv")
    r.add_argument("--output-dir", default="walk_forward_results")
    r.set_defaults(func=command_run)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
