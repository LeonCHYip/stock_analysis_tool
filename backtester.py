"""
backtester.py — Historical Trading Strategy Backtester (phase 1).

Simulates buying/selling one ticker under configurable entry, recurring-buy,
exit, and position-sizing rules; tracks cash/shares/transactions/daily
portfolio value without look-ahead bias.

Phase-1 trigger scope (deliberately trimmed; see the project plan for the
full-spec list deferred to later phases):
  - First-buy:      price X% below the all-time historical High.
  - Recurring-buy:  periodic, every X trading days.
  - Sell:           one of {profit-target % above avg cost, trailing-stop %
                     from the peak price since first buy}.
Position sizing is full-spec (all 6 buy-sizing + 5 sell-sizing methods).

Price data: reuses swing_analysis.fetch_and_cache_swing_history(), which
already returns fully split/dividend-adjusted OHLCV. Section 2 of the
original spec describes deriving adjusted OHLC from an Adjusted-Close-only
source via `Adjustment Factor = Adj Close / Close` — that path is NOT
implemented here because it's dead code given this data source: yfinance's
auto_adjust=True already returns fully adjusted O/H/L/C together, not
Close-only.

Look-ahead-bias handling: the historical-high reference used by the
first-buy trigger is an expanding max shifted by one day, so day t's
threshold never includes day t's own high. The trailing-stop reference
updates using the CURRENT day's high before that same day's sell check
(a deliberate, user-chosen "live-ratchet" convention — not look-ahead,
since by the time a day's high is reached that price has already occurred;
it does mean the reference and the exit check share the same daily bar,
a limitation of daily-bar granularity rather than of information timing).
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Literal

import numpy as np
import pandas as pd
import ta

import swing_analysis
from market_calendar import get_trading_days, next_trading_day_on_or_after

MAX_HISTORY_YEARS_DEFAULT = 30
TRADING_DAYS_PER_YEAR = 252


# ── Config dataclasses ───────────────────────────────────────────────────────

@dataclass
class BuySizingConfig:
    method: Literal["fixed_amount", "fixed_shares", "pct_starting_capital",
                     "pct_available_cash", "pct_total_value_at_first_buy", "equal_cash_remaining",
                     "equal_starting_capital_over_max_buys", "custom_table"]
    amount: float | None = None
    shares: float | None = None
    pct: float | None = None
    custom_table: list[dict] | None = None   # [{"seq": int, "unit": "amount"|"shares"|"pct_cash", "value": float}]


@dataclass
class SellSizingConfig:
    method: Literal["entire_position", "fixed_amount", "fixed_shares",
                     "pct_shares", "pct_shares_at_first_sell", "custom_table"]
    amount: float | None = None
    shares: float | None = None
    pct: float | None = None
    custom_table: list[dict] | None = None   # unit: "amount"|"shares"|"pct_shares"


@dataclass
class FirstBuyTriggerConfig:
    kind: Literal["pct_below_historical_high", "rolling_high_low_pct",
                   "ma_state_pct", "rsi_state"] = "pct_below_historical_high"
    pct_below: float = 8.0                      # pct_below_historical_high
    lookback_days: int | None = None            # rolling_high_low_pct
    reference: Literal["rolling_high", "rolling_low"] | None = None   # rolling_high_low_pct
    direction: Literal["above", "below"] | None = None                 # rolling_high_low_pct, ma_state_pct
    pct: float | None = None                     # rolling_high_low_pct, ma_state_pct (distance %)
    ma_type: Literal["SMA", "EMA"] | None = None  # ma_state_pct
    ma_period: int | None = None                   # ma_state_pct
    rsi_period: int = 14                            # rsi_state
    rsi_threshold: float = 30.0                      # rsi_state -- buy when RSI < threshold


@dataclass
class RecurringBuyTriggerConfig:
    kind: Literal["periodic_trading_days"] = "periodic_trading_days"
    interval_trading_days: int = 20


@dataclass
class SellTriggerConfig:
    kind: Literal["profit_target_pct", "trailing_stop_pct", "stop_loss_pct", "hold_days",
                   "rolling_high_low_pct", "ma_state_pct", "rsi_state"] = "profit_target_pct"
    pct: float = 20.0                             # profit_target_pct, trailing_stop_pct, stop_loss_pct,
                                                    # rolling_high_low_pct, ma_state_pct (distance %)
    hold_days: int | None = None                   # hold_days
    lookback_days: int | None = None                # rolling_high_low_pct
    reference: Literal["rolling_high", "rolling_low"] | None = None   # rolling_high_low_pct
    direction: Literal["above", "below"] | None = None                 # rolling_high_low_pct, ma_state_pct
    ma_type: Literal["SMA", "EMA"] | None = None    # ma_state_pct
    ma_period: int | None = None                     # ma_state_pct
    rsi_period: int = 14                              # rsi_state
    rsi_threshold: float = 70.0                        # rsi_state -- sell when RSI > threshold


@dataclass(kw_only=True)
class BacktestParams:
    ticker: str
    start_date: str | None = None   # None -> earliest available price history
    end_date: str | None = None     # None -> latest available price history
    starting_capital: float = 1_000_000.0
    max_buys: int = 20
    max_sells: int = 20
    allow_fractional_shares: bool = True
    transaction_fee: float = 0.0             # flat $ per transaction
    slippage_pct: float = 0.0
    end_of_backtest_action: Literal["mark_to_market", "sell_final_date"] = "mark_to_market"
    execution_price_basis: Literal["adjusted_close", "adjusted_open"] = "adjusted_close"
    cash_overflow_policy: Literal["reduce_to_max_affordable", "skip", "stop_with_error"] = "reduce_to_max_affordable"
    prevent_same_day_buy_sell: bool = False
    first_buy: FirstBuyTriggerConfig = field(default_factory=FirstBuyTriggerConfig)
    recurring_buy: RecurringBuyTriggerConfig = field(default_factory=RecurringBuyTriggerConfig)
    sell: SellTriggerConfig = field(default_factory=SellTriggerConfig)
    buy_sizing: BuySizingConfig = field(default_factory=lambda: BuySizingConfig(method="fixed_amount", amount=50_000.0))
    sell_sizing: SellSizingConfig = field(default_factory=lambda: SellSizingConfig(method="entire_position"))


@dataclass
class Transaction:
    trade_num: int
    date: str
    ticker: str
    side: Literal["BUY", "SELL"]
    trigger_type: str
    trigger_description: str
    execution_price: float
    quantity: float
    transaction_amount: float
    transaction_fee: float
    realized_pnl: float | None
    realized_return_pct: float | None
    shares_after: float
    avg_purchase_price_after: float | None
    stock_value_after: float
    cash_after: float
    total_value_after: float
    portfolio_return_pct_after: float
    days_since_prev_trade: int | None
    days_since_first_buy: int | None


@dataclass
class DailyRecord:
    date: str
    adj_close: float
    cash: float
    shares: float
    avg_purchase_price: float | None
    stock_value: float
    total_value: float
    daily_return_pct: float | None
    cumulative_return_pct: float
    drawdown_pct: float
    historical_high: float | None
    rolling_ref_high_low: float | None
    ma_ref: float | None            # reserved for later trigger types; always None in phase 1
    buy_signal: bool
    sell_signal: bool


@dataclass
class BacktestResult:
    params: BacktestParams
    transactions: pd.DataFrame
    daily_history: pd.DataFrame
    summary: dict
    buy_and_hold: dict
    warnings: list[str]
    status: str = "complete"
    error_message: str | None = None


# ── Serialization ────────────────────────────────────────────────────────────

def params_to_dict(params: BacktestParams) -> dict:
    return asdict(params)


def params_from_dict(d: dict) -> BacktestParams:
    d = dict(d)
    d["first_buy"] = FirstBuyTriggerConfig(**d["first_buy"])
    d["recurring_buy"] = RecurringBuyTriggerConfig(**d["recurring_buy"])
    d["sell"] = SellTriggerConfig(**d["sell"])
    d["buy_sizing"] = BuySizingConfig(**d["buy_sizing"])
    d["sell_sizing"] = SellSizingConfig(**d["sell_sizing"])
    return BacktestParams(**d)


# ── Validation ───────────────────────────────────────────────────────────────

def _validate_trigger_config(cfg: "FirstBuyTriggerConfig | SellTriggerConfig", side: str) -> None:
    """Validate one FirstBuyTriggerConfig or SellTriggerConfig, raising
    ValueError for the fields required by its specific `kind`. `side` is
    'first_buy' or 'sell', used only for error message prefixes."""
    k = cfg.kind
    if k == "pct_below_historical_high":
        if not (0 < cfg.pct_below < 100):
            raise ValueError(f"{side}.pct_below must be between 0 and 100 (exclusive).")
    elif k in ("profit_target_pct", "stop_loss_pct"):
        if cfg.pct is None or cfg.pct <= 0:
            raise ValueError(f"{side}.pct must be positive for kind={k!r}.")
    elif k == "trailing_stop_pct":
        if cfg.pct is None or cfg.pct <= 0:
            raise ValueError(f"{side}.pct must be positive for kind={k!r}.")
        if cfg.pct >= 100:
            raise ValueError(
                "Trailing stop percentage must be under 100 (100%+ implies selling at $0 "
                "or below). Profit-target/stop-loss percentages have no upper limit."
            )
    elif k == "hold_days":
        if side != "sell":
            raise ValueError("kind='hold_days' is only valid for the sell trigger.")
        if cfg.hold_days is None or cfg.hold_days < 1:
            raise ValueError(f"{side}.hold_days must be at least 1.")
    elif k == "rolling_high_low_pct":
        if cfg.lookback_days is None or cfg.lookback_days < 1:
            raise ValueError(f"{side}.lookback_days must be at least 1 for kind={k!r}.")
        if cfg.reference not in ("rolling_high", "rolling_low"):
            raise ValueError(f"{side}.reference must be 'rolling_high' or 'rolling_low' for kind={k!r}.")
        if cfg.direction not in ("above", "below"):
            raise ValueError(f"{side}.direction must be 'above' or 'below' for kind={k!r}.")
        if cfg.pct is None or cfg.pct <= 0:
            raise ValueError(f"{side}.pct must be positive for kind={k!r}.")
    elif k == "ma_state_pct":
        if cfg.ma_type not in ("SMA", "EMA"):
            raise ValueError(f"{side}.ma_type must be 'SMA' or 'EMA' for kind={k!r}.")
        if cfg.ma_period is None or cfg.ma_period < 2:
            raise ValueError(f"{side}.ma_period must be at least 2 for kind={k!r}.")
        if cfg.direction not in ("above", "below"):
            raise ValueError(f"{side}.direction must be 'above' or 'below' for kind={k!r}.")
        if cfg.pct is None or cfg.pct <= 0:
            raise ValueError(f"{side}.pct must be positive for kind={k!r}.")
    elif k == "rsi_state":
        if cfg.rsi_period is None or cfg.rsi_period < 2:
            raise ValueError(f"{side}.rsi_period must be at least 2 for kind={k!r}.")
        if not (0 < cfg.rsi_threshold < 100):
            raise ValueError(f"{side}.rsi_threshold must be between 0 and 100 (exclusive) for kind={k!r}.")
    else:
        raise ValueError(f"Unknown {side} trigger kind: {k!r}")


def validate_params(params: BacktestParams, price_data: pd.DataFrame) -> list[str]:
    """Raise ValueError for hard-invalid configs; return a list of soft
    warning strings for issues worth surfacing but not blocking a run."""
    if params.start_date >= params.end_date:
        raise ValueError("start_date must be before end_date.")
    if params.max_buys < 1:
        raise ValueError("max_buys must be at least 1.")
    if params.max_sells < 1:
        raise ValueError("max_sells must be at least 1.")
    if params.starting_capital <= 0:
        raise ValueError("starting_capital must be positive.")
    if price_data.empty:
        raise ValueError(f"No price data available for {params.ticker!r}.")
    _validate_trigger_config(params.first_buy, side="first_buy")
    _validate_trigger_config(params.sell, side="sell")
    if params.recurring_buy.interval_trading_days < 1:
        raise ValueError("recurring_buy.interval_trading_days must be at least 1.")

    if params.buy_sizing.method == "custom_table":
        tbl = params.buy_sizing.custom_table or []
        if len(tbl) != params.max_buys:
            raise ValueError(
                f"Custom buy-sizing table has {len(tbl)} row(s); must exactly match "
                f"max_buys ({params.max_buys})."
            )
    if params.sell_sizing.method == "custom_table":
        tbl = params.sell_sizing.custom_table or []
        if len(tbl) != params.max_sells:
            raise ValueError(
                f"Custom sell-sizing table has {len(tbl)} row(s); must exactly match "
                f"max_sells ({params.max_sells})."
            )

    warnings: list[str] = []
    pre_start = price_data[price_data.index < pd.to_datetime(params.start_date)]
    if len(pre_start) < 30:
        warnings.append(
            f"Only {len(pre_start)} trading day(s) of history before start_date are "
            "available for the historical-high calculation; early first-buy signals "
            "may be skipped until enough history accumulates."
        )
    return warnings


# ── Position sizing ──────────────────────────────────────────────────────────

def _apply_fractional_rule(shares: float, allow_fractional: bool) -> float:
    if allow_fractional:
        return max(shares, 0.0)
    return max(math.floor(shares), 0.0)


def compute_buy_size(config: BuySizingConfig, fill_price: float, available_cash: float,
                      starting_capital: float, buy_seq: int, max_buys: int,
                      buys_remaining_incl_this: int, allow_fractional: bool,
                      total_value_at_first_buy: float | None = None) -> float:
    """Return the desired share quantity for this buy (before cash-overflow
    constraints, which the caller applies). `buy_seq` is 1-indexed.

    `total_value_at_first_buy` = cash + stock market value at the moment the
    CURRENT position was opened (frozen for that position's lifetime, not
    recomputed live on every buy) -- required only for
    method="pct_total_value_at_first_buy". Every recurring buy into the same
    position therefore requests the same dollar amount; a fresh reference is
    captured only when a new position opens (after a full exit).
    """
    m = config.method
    if m == "fixed_amount":
        raw = config.amount / fill_price
    elif m == "fixed_shares":
        raw = config.shares
    elif m == "pct_starting_capital":
        raw = starting_capital * config.pct / 100.0 / fill_price
    elif m == "pct_available_cash":
        raw = available_cash * config.pct / 100.0 / fill_price
    elif m == "pct_total_value_at_first_buy":
        if total_value_at_first_buy is None:
            raise ValueError("total_value_at_first_buy is required for method='pct_total_value_at_first_buy'.")
        raw = total_value_at_first_buy * config.pct / 100.0 / fill_price
    elif m == "equal_cash_remaining":
        raw = available_cash / max(buys_remaining_incl_this, 1) / fill_price
    elif m == "equal_starting_capital_over_max_buys":
        raw = (starting_capital / max_buys) / fill_price
    elif m == "custom_table":
        row = next((r for r in (config.custom_table or []) if r["seq"] == buy_seq), None)
        if row is None:
            raise ValueError(f"No custom buy-sizing row found for seq={buy_seq}.")
        raw = _dispatch_custom_row(row, fill_price, available_cash, shares_held=None)
    else:
        raise ValueError(f"Unknown buy sizing method: {m!r}")
    return _apply_fractional_rule(raw, allow_fractional)


def compute_sell_size(config: SellSizingConfig, fill_price: float, shares_held: float,
                       sell_seq: int, max_sells: int, allow_fractional: bool,
                       shares_at_first_sell: float | None = None) -> float:
    """Return the desired share quantity for this sell (NOT yet clamped to
    shares_held — the caller clamps unconditionally). `sell_seq` is 1-indexed.

    `shares_at_first_sell` = total share count held at the moment of the
    FIRST sell of the CURRENT position (frozen for that position's
    lifetime) -- required only for method="pct_shares_at_first_sell". Unlike
    "pct_shares" (which uses the LIVE, shrinking share count and decays
    geometrically -- 50% then 50%-of-the-remainder, never fully exiting),
    this method requests the SAME share count every time: X=50% sells the
    original position in exactly 2 equal-sized sells.
    """
    m = config.method
    if m == "entire_position":
        raw = shares_held
    elif m == "fixed_amount":
        raw = config.amount / fill_price
    elif m == "fixed_shares":
        raw = config.shares
    elif m == "pct_shares":
        raw = shares_held * config.pct / 100.0
    elif m == "pct_shares_at_first_sell":
        if shares_at_first_sell is None:
            raise ValueError("shares_at_first_sell is required for method='pct_shares_at_first_sell'.")
        raw = shares_at_first_sell * config.pct / 100.0
    elif m == "custom_table":
        row = next((r for r in (config.custom_table or []) if r["seq"] == sell_seq), None)
        if row is None:
            raise ValueError(f"No custom sell-sizing row found for seq={sell_seq}.")
        raw = _dispatch_custom_row(row, fill_price, available_cash=None, shares_held=shares_held)
    else:
        raise ValueError(f"Unknown sell sizing method: {m!r}")
    return _apply_fractional_rule(raw, allow_fractional)


def _dispatch_custom_row(row: dict, fill_price: float, available_cash: float | None,
                          shares_held: float | None) -> float:
    unit, value = row["unit"], row["value"]
    if unit == "amount":
        return value / fill_price
    if unit == "shares":
        return value
    if unit == "pct_cash":
        if available_cash is None:
            raise ValueError("pct_cash unit is only valid for buy-sizing rows.")
        return available_cash * value / 100.0 / fill_price
    if unit == "pct_shares":
        if shares_held is None:
            raise ValueError("pct_shares unit is only valid for sell-sizing rows.")
        return shares_held * value / 100.0
    raise ValueError(f"Unknown custom-table unit: {unit!r}")


# ── Intraday execution primitive ────────────────────────────────────────────

def _intraday_threshold_execution(
    open_: float, high: float, low: float, threshold: float,
    direction: Literal["buy_at_or_below", "buy_at_or_above", "sell_at_or_above", "sell_at_or_below"],
) -> float | None:
    """Return the fill price if the threshold condition fires today, else None.

    buy_at_or_below / sell_at_or_below: fires if low <= threshold; fill is
    the threshold price unless the open already gapped through it (open on
    the wrong side of the threshold for this direction), in which case fill
    is the open.
    buy_at_or_above / sell_at_or_above: fires if high >= threshold, same
    gap-through logic. buy_at_or_above is for breakout-style entries (e.g.
    "buy when price breaks above the Y-day high by X%") -- the mirror of
    sell_at_or_above.
    """
    if direction in ("buy_at_or_below", "sell_at_or_below"):
        if low <= threshold:
            return threshold if open_ >= threshold else open_
        return None
    if direction in ("buy_at_or_above", "sell_at_or_above"):
        if high >= threshold:
            return threshold if open_ <= threshold else open_
        return None
    raise ValueError(f"Unknown direction: {direction!r}")


def _historical_high_series(price_data: pd.DataFrame) -> np.ndarray:
    """All-time highest High as of the END of the PRIOR trading day -- day t's
    value never includes day t's own high (no look-ahead). NaN for the first
    day (no prior history)."""
    return price_data["high"].expanding().max().shift(1).to_numpy(dtype=float)


def _rolling_high_low_series(price_data: pd.DataFrame, reference: str, lookback_days: int) -> np.ndarray:
    """Rolling Y-day highest High or lowest Low, as of the END of the PRIOR
    trading day (shift(1) -- same no-look-ahead convention as the historical-
    high series)."""
    if reference == "rolling_high":
        s = price_data["high"].rolling(lookback_days).max()
    elif reference == "rolling_low":
        s = price_data["low"].rolling(lookback_days).min()
    else:
        raise ValueError(f"Unknown reference: {reference!r}")
    return s.shift(1).to_numpy(dtype=float)


def _ma_series(price_data: pd.DataFrame, ma_type: str, ma_period: int) -> np.ndarray:
    """SMA or EMA of Close, as of the END of the PRIOR trading day (shift(1)
    -- the MA value itself never includes today's own close)."""
    close = price_data["close"]
    if ma_type == "SMA":
        s = ta.trend.SMAIndicator(close, window=ma_period).sma_indicator()
    elif ma_type == "EMA":
        s = ta.trend.EMAIndicator(close, window=ma_period).ema_indicator()
    else:
        raise ValueError(f"Unknown ma_type: {ma_type!r}")
    return s.shift(1).to_numpy(dtype=float)


def _rsi_series(price_data: pd.DataFrame, rsi_period: int) -> np.ndarray:
    """RSI of Close, using TODAY'S OWN close -- deliberately NOT shifted,
    unlike every other reference series in this module.

    This is a disclosed, user-confirmed simplification: RSI is a derived
    statistic (not a simple price level), so it has no valid "intraday"
    threshold check the way a price level does. The correct no-look-ahead
    approach would defer to the next trading day, but the chosen convention
    here is to compute RSI through today's close and execute the trade AT
    that same day's close, on the assumption a near-final intraday price is
    a close enough proxy to act on before the close. See SellTriggerConfig/
    FirstBuyTriggerConfig kind="rsi_state" and its evaluation site.
    """
    return ta.momentum.RSIIndicator(price_data["close"], window=rsi_period).rsi().to_numpy(dtype=float)


def _distance_threshold(reference_value: float, pct: float, direction: str, side: str) -> tuple[float, str]:
    """Combine a reference price level with a % distance and above/below
    direction into (threshold_price, execution_direction) for
    _intraday_threshold_execution. `side` is 'buy' or 'sell'."""
    if direction == "below":
        return reference_value * (1 - pct / 100.0), f"{side}_at_or_below"
    if direction == "above":
        return reference_value * (1 + pct / 100.0), f"{side}_at_or_above"
    raise ValueError(f"Unknown direction: {direction!r}")


# ── Walk-forward simulation ─────────────────────────────────────────────────

def _simulate(params: BacktestParams, price_data: pd.DataFrame) -> tuple[list[dict], list[dict], list[str], str, str | None]:
    """Run the day-by-day simulation. Returns
    (transactions, daily_records, warnings, status, error_message)."""
    dates = price_data.index
    opens = price_data["open"].to_numpy(dtype=float)
    highs = price_data["high"].to_numpy(dtype=float)
    lows = price_data["low"].to_numpy(dtype=float)
    closes = price_data["close"].to_numpy(dtype=float)
    n = len(price_data)

    hh_series = _historical_high_series(price_data)

    # Build only the reference series actually needed by the configured
    # first-buy/sell trigger kinds -- first-buy and sell can use different
    # lookback_days/ma_period/rsi_period even if both pick the same kind, so
    # these are computed independently per side rather than shared.
    def _build_series_for(cfg) -> np.ndarray | None:
        if cfg.kind == "rolling_high_low_pct":
            return _rolling_high_low_series(price_data, cfg.reference, cfg.lookback_days)
        if cfg.kind == "ma_state_pct":
            return _ma_series(price_data, cfg.ma_type, cfg.ma_period)
        if cfg.kind == "rsi_state":
            return _rsi_series(price_data, cfg.rsi_period)
        return None

    fb_series = _build_series_for(params.first_buy)
    sell_series = _build_series_for(params.sell)

    start_day = next_trading_day_on_or_after(params.start_date)
    if start_day is None:
        raise ValueError(f"Could not find a trading day on/after {params.start_date}.")
    end_days = get_trading_days(params.start_date, params.end_date)
    if not end_days:
        raise ValueError(f"No trading days found between {params.start_date} and {params.end_date}.")
    end_day = end_days[-1]

    start_ts, end_ts = pd.to_datetime(start_day), pd.to_datetime(end_day)
    if start_ts not in dates or end_ts not in dates:
        raise ValueError(
            f"Requested backtest range [{start_day}, {end_day}] falls outside the "
            f"available price history [{dates.min().date()}, {dates.max().date()}]."
        )
    start_idx = dates.get_loc(start_ts)
    end_idx = dates.get_loc(end_ts)

    warnings: list[str] = []
    hh_nan_warned = False
    buy_cap_warned = False
    sell_cap_warned = False

    cash = params.starting_capital
    shares = 0.0
    avg_price: float | None = None
    first_buy_idx: int | None = None
    trailing_high_ref: float | None = None
    next_scheduled_buy_idx: int | None = None
    position_open_total_value: float | None = None   # frozen at first-buy, for pct_total_value_at_first_buy sizing
    position_first_sell_shares: float | None = None  # frozen at first sell of this position, for pct_shares_at_first_sell sizing
    sold_this_position = False                        # has any sell fired yet for the CURRENT position?
    num_buys = 0
    num_sells = 0
    running_peak_value = params.starting_capital
    last_trade_idx: int | None = None
    trade_num = 0

    transactions: list[dict] = []
    daily_records: list[dict] = []
    status = "complete"
    error_message: str | None = None

    def make_txn(t: int, side: str, trigger_type: str, trigger_desc: str,
                 exec_price: float, qty: float, realized_pnl, realized_return_pct) -> dict:
        nonlocal trade_num, last_trade_idx
        trade_num += 1
        amount = qty * exec_price
        days_since_prev = (t - last_trade_idx) if last_trade_idx is not None else None
        days_since_first_buy = (t - first_buy_idx) if first_buy_idx is not None else None
        stock_value = shares * closes[t]
        # Round cash/stock_value FIRST, then derive total_value from the
        # rounded parts -- rounding all three independently can leave
        # cash_after + stock_value_after off from total_value_after by a
        # cent, which the invariant self-check (rightly) treats as a bug in
        # a financial accounting table.
        cash_r, stock_value_r = round(cash, 2), round(stock_value, 2)
        total_value_r = cash_r + stock_value_r
        txn = {
            "trade_num": trade_num, "date": str(dates[t].date()), "ticker": params.ticker,
            "side": side, "trigger_type": trigger_type, "trigger_description": trigger_desc,
            "execution_price": round(exec_price, 4), "quantity": round(qty, 6),
            "transaction_amount": round(amount, 2), "transaction_fee": round(params.transaction_fee, 2),
            "realized_pnl": round(realized_pnl, 2) if realized_pnl is not None else None,
            "realized_return_pct": round(realized_return_pct, 4) if realized_return_pct is not None else None,
            "shares_after": round(shares, 6), "avg_purchase_price_after": round(avg_price, 4) if avg_price is not None else None,
            "stock_value_after": stock_value_r, "cash_after": cash_r,
            "total_value_after": total_value_r,
            "portfolio_return_pct_after": round((total_value_r / params.starting_capital - 1) * 100, 4),
            "days_since_prev_trade": days_since_prev, "days_since_first_buy": days_since_first_buy,
        }
        last_trade_idx = t
        return txn

    for t in range(start_idx, end_idx + 1):
        open_t, high_t, low_t, close_t = opens[t], highs[t], lows[t], closes[t]

        sell_fire = False
        sell_fill = None
        sell_desc = ""
        recurring_fire = False
        recurring_fill = None
        first_buy_fire = False
        first_buy_fill = None
        first_buy_desc = ""

        # Step 1: live-ratchet trailing-high reference (today's own high),
        # only for days already in position at the START of today.
        if shares > 0:
            trailing_high_ref = max(trailing_high_ref, high_t)

        # Step 2: sell evaluation
        if shares > 0:
            sk = params.sell.kind
            if sk == "profit_target_pct":
                threshold = avg_price * (1 + params.sell.pct / 100.0)
                fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, "sell_at_or_above")
                sell_desc = f"Price >= {params.sell.pct}% above avg purchase price (threshold ${threshold:.2f})"
            elif sk == "trailing_stop_pct":
                threshold = trailing_high_ref * (1 - params.sell.pct / 100.0)
                fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, "sell_at_or_below")
                sell_desc = f"Trailing stop {params.sell.pct}% from peak ${trailing_high_ref:.2f} (threshold ${threshold:.2f})"
            elif sk == "stop_loss_pct":
                threshold = avg_price * (1 - params.sell.pct / 100.0)
                fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, "sell_at_or_below")
                sell_desc = f"Price <= {params.sell.pct}% below avg purchase price (threshold ${threshold:.2f})"
            elif sk == "hold_days":
                fill = None
                if first_buy_idx is not None and (t - first_buy_idx) >= params.sell.hold_days:
                    fill = close_t if params.execution_price_basis == "adjusted_close" else open_t
                sell_desc = f"Held for >= {params.sell.hold_days} trading days"
            elif sk == "rolling_high_low_pct":
                ref_val = sell_series[t]
                fill = None
                if not np.isnan(ref_val):
                    threshold, exec_dir = _distance_threshold(ref_val, params.sell.pct, params.sell.direction, "sell")
                    fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, exec_dir)
                    sell_desc = (f"Price {params.sell.direction} {params.sell.pct}% of "
                                 f"{params.sell.lookback_days}-day {params.sell.reference.replace('rolling_','')} "
                                 f"(threshold ${threshold:.2f})")
            elif sk == "ma_state_pct":
                ref_val = sell_series[t]
                fill = None
                if not np.isnan(ref_val):
                    threshold, exec_dir = _distance_threshold(ref_val, params.sell.pct, params.sell.direction, "sell")
                    fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, exec_dir)
                    sell_desc = (f"Price {params.sell.direction} {params.sell.pct}% of "
                                 f"{params.sell.ma_period}-{params.sell.ma_type} (threshold ${threshold:.2f})")
            else:  # rsi_state -- see _rsi_series docstring for the same-day-close rationale
                rsi_val = sell_series[t]
                fill = None
                if not np.isnan(rsi_val) and rsi_val > params.sell.rsi_threshold:
                    fill = close_t
                    sell_desc = f"RSI({params.sell.rsi_period})={rsi_val:.1f} > {params.sell.rsi_threshold}"
            if fill is not None and num_sells < params.max_sells:
                sell_fire, sell_fill = True, fill

        # Step 3: recurring-buy evaluation (schedule-based, not threshold)
        if (shares > 0 and num_buys < params.max_buys and next_scheduled_buy_idx is not None
                and t >= next_scheduled_buy_idx):
            recurring_fire = True
            recurring_fill = close_t if params.execution_price_basis == "adjusted_close" else open_t

        # Step 4: first-buy evaluation
        if shares == 0 and num_buys < params.max_buys:
            fk = params.first_buy.kind
            if fk == "pct_below_historical_high":
                hh = hh_series[t]
                if np.isnan(hh):
                    if not hh_nan_warned:
                        warnings.append(
                            "Insufficient pre-start history for the historical-high calculation on "
                            "some early days; first-buy trigger was skipped until enough history accumulated."
                        )
                        hh_nan_warned = True
                else:
                    threshold = hh * (1 - params.first_buy.pct_below / 100.0)
                    fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, "buy_at_or_below")
                    if fill is not None:
                        first_buy_fire, first_buy_fill = True, fill
                        first_buy_desc = f"{params.first_buy.pct_below}% below historical high (threshold ${threshold:.2f})"
            elif fk in ("rolling_high_low_pct", "ma_state_pct"):
                ref_val = fb_series[t]
                if not np.isnan(ref_val):
                    threshold, exec_dir = _distance_threshold(ref_val, params.first_buy.pct,
                                                                params.first_buy.direction, "buy")
                    fill = _intraday_threshold_execution(open_t, high_t, low_t, threshold, exec_dir)
                    if fill is not None:
                        first_buy_fire, first_buy_fill = True, fill
                        if fk == "rolling_high_low_pct":
                            first_buy_desc = (f"Price {params.first_buy.direction} {params.first_buy.pct}% of "
                                               f"{params.first_buy.lookback_days}-day "
                                               f"{params.first_buy.reference.replace('rolling_','')} (threshold ${threshold:.2f})")
                        else:
                            first_buy_desc = (f"Price {params.first_buy.direction} {params.first_buy.pct}% of "
                                               f"{params.first_buy.ma_period}-{params.first_buy.ma_type} (threshold ${threshold:.2f})")
            else:  # rsi_state -- see _rsi_series docstring for the same-day-close rationale
                rsi_val = fb_series[t]
                if not np.isnan(rsi_val) and rsi_val < params.first_buy.rsi_threshold:
                    first_buy_fire, first_buy_fill = True, close_t
                    first_buy_desc = f"RSI({params.first_buy.rsi_period})={rsi_val:.1f} < {params.first_buy.rsi_threshold}"

        # Step 5: execution ordering -- sell first, then buy
        if sell_fire:
            num_sells += 1
            if num_sells >= params.max_sells and t < end_idx and not sell_cap_warned:
                warnings.append(
                    f"Max sells ({params.max_sells}) reached on {dates[t].date()} -- no further "
                    f"sell signals were evaluated for the remaining {end_idx - t} trading day(s) of "
                    "the backtest. Increase 'Max sells' to allow more exits."
                )
                sell_cap_warned = True
            price_incl = sell_fill * (1 - params.slippage_pct / 100.0)
            # pct_shares_at_first_sell sizing: freeze the share count held at
            # THIS sell only if it's the first sell of the current position;
            # every later sell of this same position reuses that frozen count.
            if not sold_this_position:
                position_first_sell_shares = shares
                sold_this_position = True
            qty = compute_sell_size(params.sell_sizing, price_incl, shares, num_sells,
                                     params.max_sells, params.allow_fractional_shares,
                                     shares_at_first_sell=position_first_sell_shares)
            qty = min(qty, shares)
            realized_pnl = qty * (price_incl - avg_price) - params.transaction_fee
            realized_return_pct = (price_incl / avg_price - 1) * 100.0
            cash += qty * price_incl - params.transaction_fee
            shares -= qty
            trigger_type = "sell_" + params.sell.kind
            txn = make_txn(t, "SELL", trigger_type, sell_desc, price_incl, qty,
                            realized_pnl, realized_return_pct)
            transactions.append(txn)
            if shares <= 1e-9:
                shares = 0.0
                avg_price = None
                first_buy_idx = None
                trailing_high_ref = None
                next_scheduled_buy_idx = None
                position_open_total_value = None
                position_first_sell_shares = None
                recurring_fire = False   # cancel: belonged to the now-closed position

        if params.prevent_same_day_buy_sell and sell_fire:
            recurring_fire = False
            first_buy_fire = False

        buy_fire = recurring_fire or first_buy_fire
        if buy_fire:
            is_first_buy = first_buy_fire
            fill = first_buy_fill if is_first_buy else recurring_fill
            price_incl = fill * (1 + params.slippage_pct / 100.0)
            buy_seq = num_buys + 1
            buys_remaining_incl_this = params.max_buys - num_buys
            # pct_total_value_at_first_buy sizing: freeze the portfolio's total
            # value at the moment THIS position opens (shares==0 pre-buy, so
            # this is just cash -- any same-day sell already applied above),
            # then reuse that SAME frozen value for every later recurring buy
            # into this position, so every buy requests the same dollar amount
            # rather than drifting with the live portfolio value.
            if is_first_buy:
                position_open_total_value = cash + shares * price_incl
                sold_this_position = False   # fresh position -- re-arm pct_shares_at_first_sell sizing
            desired = compute_buy_size(params.buy_sizing, price_incl, cash, params.starting_capital,
                                        buy_seq, params.max_buys, buys_remaining_incl_this,
                                        params.allow_fractional_shares,
                                        total_value_at_first_buy=position_open_total_value)
            cost = desired * price_incl + params.transaction_fee
            if cost > cash:
                policy = params.cash_overflow_policy
                if policy == "stop_with_error":
                    status, error_message = "error", (
                        f"Insufficient cash on {dates[t].date()}: needed ${cost:,.2f}, had ${cash:,.2f}."
                    )
                    desired = 0.0
                else:
                    affordable = max((cash - params.transaction_fee) / price_incl, 0.0)
                    affordable = _apply_fractional_rule(affordable, params.allow_fractional_shares)
                    min_unit = 1.0 if not params.allow_fractional_shares else 1e-9
                    if affordable < min_unit:
                        warnings.append(
                            f"Skipped a buy on {dates[t].date()}: insufficient cash "
                            f"(needed ~${cost:,.2f}, had ${cash:,.2f})."
                        )
                        desired = 0.0
                    elif policy == "reduce_to_max_affordable":
                        desired = affordable
                    else:  # skip
                        warnings.append(
                            f"Skipped a buy on {dates[t].date()}: requested amount exceeds "
                            f"available cash and cash_overflow_policy='skip'."
                        )
                        desired = 0.0

            if desired > 0:
                num_buys += 1
                if num_buys >= params.max_buys and t < end_idx and not buy_cap_warned:
                    warnings.append(
                        f"Max buys ({params.max_buys}) reached on {dates[t].date()} -- no further "
                        f"buy signals (first-buy or recurring) were evaluated for the remaining "
                        f"{end_idx - t} trading day(s) of the backtest. Increase 'Max buys' to allow "
                        "more entries, especially for long backtests with frequent recurring buys."
                    )
                    buy_cap_warned = True
                cost = desired * price_incl + params.transaction_fee
                shares_old = shares
                cash -= cost
                shares += desired
                if shares_old == 0:
                    avg_price = price_incl
                    first_buy_idx = t
                    trailing_high_ref = high_t   # bootstrap: step 1 ran before shares>0 today
                else:
                    avg_price = (avg_price * shares_old + price_incl * desired) / shares
                next_scheduled_buy_idx = t + params.recurring_buy.interval_trading_days
                trigger_type = "first_buy" if is_first_buy else "recurring_buy"
                desc = (first_buy_desc if is_first_buy
                        else f"Periodic every {params.recurring_buy.interval_trading_days} trading days")
                txn = make_txn(t, "BUY", trigger_type, desc, price_incl, desired, None, None)
                transactions.append(txn)

            if status == "error":
                stock_value = shares * close_t
                total_value = cash + stock_value
                daily_records.append(_daily_record(dates[t], close_t, cash, shares, avg_price,
                                                     stock_value, total_value, daily_records,
                                                     running_peak_value, hh_series[t], trailing_high_ref,
                                                     sell_fire, buy_fire))
                break

        # Step 7: end-of-day bookkeeping
        stock_value = shares * close_t
        total_value = cash + stock_value
        running_peak_value = max(running_peak_value, total_value)
        rec = _daily_record(dates[t], close_t, cash, shares, avg_price, stock_value, total_value,
                             daily_records, running_peak_value, hh_series[t], trailing_high_ref,
                             sell_fire, buy_fire)
        daily_records.append(rec)

    # After the loop: forced liquidation on the final date, if requested
    if status == "complete" and params.end_of_backtest_action == "sell_final_date" and shares > 0:
        t = end_idx
        price_incl = closes[t] * (1 - params.slippage_pct / 100.0)
        qty = shares
        realized_pnl = qty * (price_incl - avg_price) - params.transaction_fee
        realized_return_pct = (price_incl / avg_price - 1) * 100.0
        cash += qty * price_incl - params.transaction_fee
        shares = 0.0
        num_sells += 1
        txn = make_txn(t, "SELL", "forced_final_liquidation",
                        "Forced liquidation of remaining position on the final backtest date",
                        price_incl, qty, realized_pnl, realized_return_pct)
        transactions.append(txn)
        avg_price = None
        stock_value = 0.0
        total_value = cash
        running_peak_value = max(running_peak_value, total_value)
        if daily_records:
            daily_records[-1] = _daily_record(
                dates[t], closes[t], cash, shares, avg_price, stock_value, total_value,
                daily_records[:-1], running_peak_value, hh_series[t], None, True, True,
            )

    return transactions, daily_records, warnings, status, error_message


def _daily_record(date, close, cash, shares, avg_price, stock_value, total_value,
                   prior_records: list[dict], running_peak_value, hh, trailing_ref,
                   sell_signal: bool, buy_signal: bool) -> dict:
    # Round cash/stock_value FIRST, then derive total_value from the rounded
    # parts (see the matching comment in make_txn) so cash + stock_value ==
    # total_value exactly, to the cent, every day -- not just approximately.
    cash_r, stock_value_r = round(cash, 2), round(stock_value, 2)
    total_value_r = cash_r + stock_value_r

    prev_total = prior_records[-1]["total_value"] if prior_records else None
    # cumulative_return_pct always measured against the very first daily record's
    # total_value (== starting_capital at t0, since day 0 has no trades executed yet
    # until later in the same iteration -- close enough as the backtest's capital base)
    base = prior_records[0]["_base_capital"] if prior_records else total_value_r
    daily_return_pct = ((total_value_r / prev_total) - 1) * 100 if prev_total else None
    cumulative_return_pct = (total_value_r / base - 1) * 100
    drawdown_pct = (total_value_r / running_peak_value - 1) * 100 if running_peak_value else 0.0
    return {
        "date": str(date.date()), "adj_close": round(close, 4), "cash": cash_r,
        "shares": round(shares, 6), "avg_purchase_price": round(avg_price, 4) if avg_price is not None else None,
        "stock_value": stock_value_r, "total_value": total_value_r,
        "daily_return_pct": round(daily_return_pct, 4) if daily_return_pct is not None else None,
        "cumulative_return_pct": round(cumulative_return_pct, 4),
        "drawdown_pct": round(drawdown_pct, 4),
        "historical_high": round(hh, 4) if not np.isnan(hh) else None,
        "rolling_ref_high_low": round(trailing_ref, 4) if trailing_ref is not None else None,
        "ma_ref": None,
        "buy_signal": bool(buy_signal), "sell_signal": bool(sell_signal),
        "_base_capital": base,
    }


# ── Invariant self-check (permanent regression guard) ───────────────────────

def _check_invariants(daily_df: pd.DataFrame) -> None:
    if daily_df.empty:
        return
    if (daily_df["cash"] < -1e-6).any():
        raise RuntimeError("Invariant violated: negative cash in daily history.")
    if (daily_df["shares"] < -1e-6).any():
        raise RuntimeError("Invariant violated: negative shares in daily history.")
    accounting = (daily_df["cash"] + daily_df["stock_value"] - daily_df["total_value"]).abs()
    if (accounting > 0.01).any():
        raise RuntimeError("Invariant violated: cash + stock_value != total_value on some day.")


# ── Summary statistics ───────────────────────────────────────────────────────

def _na(v):
    return "N/A" if v is None else v


def compute_summary_stats(transactions: pd.DataFrame, daily_history: pd.DataFrame,
                           params: BacktestParams) -> dict:
    buys = transactions[transactions["side"] == "BUY"] if not transactions.empty else transactions
    sells = transactions[transactions["side"] == "SELL"] if not transactions.empty else transactions
    dh = daily_history

    first_buy_date = buys["date"].min() if not buys.empty else None
    last_buy_date = buys["date"].max() if not buys.empty else None
    first_sell_date = sells["date"].min() if not sells.empty else None
    last_sell_date = sells["date"].max() if not sells.empty else None

    invested_days = int((dh["shares"] > 0).sum()) if not dh.empty else 0
    total_days = len(dh)
    pct_time_invested = (invested_days / total_days * 100) if total_days else 0.0

    holding_periods = _compute_holding_periods(transactions, dh)
    avg_holding = float(np.mean(holding_periods)) if holding_periods else None
    longest_holding = max(holding_periods) if holding_periods else None

    strategy_period = {
        "backtest_start_date": params.start_date, "backtest_end_date": params.end_date,
        "first_buy_date": _na(first_buy_date), "last_buy_date": _na(last_buy_date),
        "first_sell_date": _na(first_sell_date), "last_sell_date": _na(last_sell_date),
        "num_invested_trading_days": invested_days,
        "pct_time_invested": round(pct_time_invested, 2),
        "avg_holding_period_trading_days": round(avg_holding, 1) if avg_holding is not None else "N/A",
        "longest_holding_period_trading_days": _na(longest_holding),
    }

    total_capital_deployed = float(buys["transaction_amount"].sum()) if not buys.empty else 0.0
    max_capital_deployed = float(dh["stock_value"].max()) if not dh.empty else 0.0
    total_sale_proceeds = float(sells["transaction_amount"].sum()) if not sells.empty else 0.0
    remaining_cash = float(dh["cash"].iloc[-1]) if not dh.empty else params.starting_capital
    remaining_shares = float(dh["shares"].iloc[-1]) if not dh.empty else 0.0
    final_avg_price = dh["avg_purchase_price"].iloc[-1] if not dh.empty else None
    final_avg_price = float(final_avg_price) if final_avg_price is not None and not pd.isna(final_avg_price) else None
    realized_pnl = float(sells["realized_pnl"].sum()) if not sells.empty else 0.0
    final_close = float(dh["adj_close"].iloc[-1]) if not dh.empty else None
    unrealized_pnl = (
        remaining_shares * (final_close - final_avg_price)
        if remaining_shares > 0 and final_avg_price is not None and final_close is not None else 0.0
    )
    total_pnl = realized_pnl + unrealized_pnl

    trading_activity = {
        "num_buys": int(len(buys)), "num_sells": int(len(sells)),
        "total_capital_deployed": round(total_capital_deployed, 2),
        "max_capital_deployed": round(max_capital_deployed, 2),
        "total_sale_proceeds": round(total_sale_proceeds, 2),
        "remaining_cash": round(remaining_cash, 2), "remaining_shares": round(remaining_shares, 6),
        "final_avg_purchase_price": round(final_avg_price, 4) if final_avg_price is not None else "N/A",
        "realized_pnl": round(realized_pnl, 2), "unrealized_pnl": round(unrealized_pnl, 2),
        "total_pnl": round(total_pnl, 2),
    }

    final_value = float(dh["total_value"].iloc[-1]) if not dh.empty else params.starting_capital
    total_return_pct = (final_value / params.starting_capital - 1) * 100
    total_return_amount = final_value - params.starting_capital
    years = max(total_days / TRADING_DAYS_PER_YEAR, 1e-9)
    annualized_return = ((final_value / params.starting_capital) ** (1 / years) - 1) * 100 if final_value > 0 else None

    performance = {
        "final_portfolio_value": round(final_value, 2),
        "total_return_pct": round(total_return_pct, 4),
        "total_return_amount": round(total_return_amount, 2),
        "annualized_return_pct": round(annualized_return, 4) if annualized_return is not None else "N/A",
    }
    for horizon in (30, 90, 180, 360):
        pct, amt = _horizon_return(dh, first_buy_date, horizon)
        performance[f"return_{horizon}d_pct"] = pct
        performance[f"return_{horizon}d_amount"] = amt

    risk = _compute_risk_stats(dh)

    trade_statistics = _compute_trade_statistics(buys, sells)

    return {
        "strategy_period": strategy_period,
        "trading_activity": trading_activity,
        "performance": performance,
        "risk": risk,
        "trade_statistics": trade_statistics,
    }


def _compute_holding_periods(transactions: pd.DataFrame, dh: pd.DataFrame) -> list[int]:
    """Trading-day duration of each fully-closed position (first-buy index to
    the sell that fully closed it), plus the still-open final position (if
    any) measured through the last available day."""
    if transactions.empty:
        return []
    date_to_idx = {d: i for i, d in enumerate(dh["date"])}
    periods = []
    open_idx = None
    for _, row in transactions.iterrows():
        idx = date_to_idx.get(row["date"])
        if idx is None:
            continue
        if row["trigger_type"] == "first_buy":
            open_idx = idx
        elif row["side"] == "SELL" and row["shares_after"] == 0 and open_idx is not None:
            periods.append(idx - open_idx)
            open_idx = None
    if open_idx is not None and not dh.empty:
        periods.append((len(dh) - 1) - open_idx)
    return periods


def _horizon_return(dh: pd.DataFrame, first_buy_date, horizon_days: int):
    if dh.empty or first_buy_date is None or first_buy_date == "N/A":
        return "N/A", "N/A"
    target = pd.to_datetime(first_buy_date) + pd.Timedelta(days=horizon_days)
    dh_dates = pd.to_datetime(dh["date"])
    if target > dh_dates.max():
        return "N/A", "N/A"
    idx = dh_dates.searchsorted(target)
    if idx >= len(dh):
        return "N/A", "N/A"
    base_idx = dh_dates.searchsorted(pd.to_datetime(first_buy_date))
    base_value = float(dh["total_value"].iloc[base_idx])
    target_value = float(dh["total_value"].iloc[idx])
    return round((target_value / base_value - 1) * 100, 4), round(target_value - base_value, 2)


def _compute_risk_stats(dh: pd.DataFrame) -> dict:
    if dh.empty:
        return {k: "N/A" for k in (
            "max_drawdown_pct", "max_drawdown_amount", "drawdown_peak_date", "drawdown_trough_date",
            "recovery_date", "max_drawdown_duration_trading_days", "highest_gain_pct",
            "highest_gain_amount", "highest_gain_date", "highest_portfolio_value",
            "lowest_portfolio_value", "best_daily_return_pct", "worst_daily_return_pct",
            "annualized_volatility_pct",
        )}
    values = dh["total_value"].to_numpy()
    dates = dh["date"].to_numpy()
    running_peak = np.maximum.accumulate(values)
    drawdown = values / running_peak - 1
    trough_i = int(np.argmin(drawdown))
    max_dd_pct = float(drawdown[trough_i]) * 100
    peak_val_at_trough = running_peak[trough_i]
    peak_i = int(np.argmax(values[: trough_i + 1] == peak_val_at_trough))
    max_dd_amount = values[trough_i] - peak_val_at_trough

    recovery_date = "N/A"
    recovery_i = None
    for i in range(trough_i + 1, len(values)):
        if values[i] >= peak_val_at_trough:
            recovery_date, recovery_i = str(dates[i]), i
            break
    dd_duration = (recovery_i - peak_i) if recovery_i is not None else (len(values) - 1 - peak_i)

    running_trough = np.minimum.accumulate(values)
    runup = values / running_trough - 1
    gain_i = int(np.argmax(runup))
    trough_val_before_gain = running_trough[gain_i]
    gain_start_i = int(np.argmin(values[: gain_i + 1] == trough_val_before_gain))

    daily_returns = dh["daily_return_pct"].dropna()

    return {
        "max_drawdown_pct": round(max_dd_pct, 4),
        "max_drawdown_amount": round(float(max_dd_amount), 2),
        "drawdown_peak_date": str(dates[peak_i]),
        "drawdown_trough_date": str(dates[trough_i]),
        "recovery_date": recovery_date,
        "max_drawdown_duration_trading_days": int(dd_duration),
        "highest_gain_pct": round(float(runup[gain_i]) * 100, 4),
        "highest_gain_amount": round(float(values[gain_i] - trough_val_before_gain), 2),
        "highest_gain_date": str(dates[gain_i]),
        "highest_portfolio_value": round(float(values.max()), 2),
        "lowest_portfolio_value": round(float(values.min()), 2),
        "best_daily_return_pct": round(float(daily_returns.max()), 4) if not daily_returns.empty else "N/A",
        "worst_daily_return_pct": round(float(daily_returns.min()), 4) if not daily_returns.empty else "N/A",
        "annualized_volatility_pct": (
            round(float(daily_returns.std() * math.sqrt(TRADING_DAYS_PER_YEAR)), 4)
            if len(daily_returns) > 1 else "N/A"
        ),
    }


def _compute_trade_statistics(buys: pd.DataFrame, sells: pd.DataFrame) -> dict:
    if sells.empty:
        profitable = pd.DataFrame()
        losing = pd.DataFrame()
    else:
        profitable = sells[sells["realized_pnl"] > 0]
        losing = sells[sells["realized_pnl"] < 0]

    def _avg_gap(df: pd.DataFrame) -> float | str:
        if len(df) < 2:
            return "N/A"
        dts = pd.to_datetime(df["date"]).sort_values()
        return round(float(dts.diff().dt.days.dropna().mean()), 2)

    return {
        "num_profitable_sells": int(len(profitable)),
        "pct_profitable_sells": round(len(profitable) / len(sells) * 100, 2) if len(sells) else "N/A",
        "avg_realized_gain": round(float(profitable["realized_pnl"].mean()), 2) if not profitable.empty else "N/A",
        "avg_realized_loss": round(float(losing["realized_pnl"].mean()), 2) if not losing.empty else "N/A",
        "largest_profitable_sell": round(float(profitable["realized_pnl"].max()), 2) if not profitable.empty else "N/A",
        "largest_losing_sell": round(float(losing["realized_pnl"].min()), 2) if not losing.empty else "N/A",
        "avg_buy_price": round(float(buys["execution_price"].mean()), 4) if not buys.empty else "N/A",
        "avg_sell_price": round(float(sells["execution_price"].mean()), 4) if not sells.empty else "N/A",
        "avg_time_between_buys_calendar_days": _avg_gap(buys),
        "avg_time_between_sells_calendar_days": _avg_gap(sells),
    }


def compute_buy_and_hold(price_data: pd.DataFrame, first_buy_date: str | None,
                          end_date: str, starting_capital: float) -> dict:
    if not first_buy_date or first_buy_date == "N/A":
        return {k: "N/A" for k in (
            "strategy_final_value", "buy_and_hold_final_value", "strategy_total_return_pct",
            "buy_and_hold_total_return_pct", "excess_return_pct",
        )}
    fb_ts = pd.to_datetime(first_buy_date)
    end_ts = price_data.index[price_data.index <= pd.to_datetime(end_date)].max()
    bh_start_price = float(price_data.loc[fb_ts, "close"])
    bh_end_price = float(price_data.loc[end_ts, "close"])
    bh_shares = starting_capital / bh_start_price
    bh_final_value = bh_shares * bh_end_price
    bh_return_pct = (bh_final_value / starting_capital - 1) * 100
    return {
        "buy_and_hold_final_value": round(bh_final_value, 2),
        "buy_and_hold_total_return_pct": round(bh_return_pct, 4),
        "_bh_series_start": fb_ts, "_bh_series_shares": bh_shares,   # for future chart use
    }


def _finalize_buy_and_hold(bh: dict, strategy_final_value: float, strategy_total_return_pct: float,
                            strategy_max_dd_pct, bh_price_data: pd.DataFrame | None) -> dict:
    if bh.get("buy_and_hold_final_value") == "N/A":
        return bh
    bh = dict(bh)
    bh["strategy_final_value"] = round(strategy_final_value, 2)
    bh["strategy_total_return_pct"] = round(strategy_total_return_pct, 4)
    bh["excess_return_pct"] = round(strategy_total_return_pct - bh["buy_and_hold_total_return_pct"], 4)
    bh["strategy_max_drawdown_pct"] = strategy_max_dd_pct
    if bh_price_data is not None:
        start_ts, shares = bh["_bh_series_start"], bh["_bh_series_shares"]
        series = bh_price_data.loc[bh_price_data.index >= start_ts, "close"] * shares
        running_peak = series.cummax()
        dd = (series / running_peak - 1) * 100
        bh["buy_and_hold_max_drawdown_pct"] = round(float(dd.min()), 4) if not dd.empty else "N/A"
    else:
        bh["buy_and_hold_max_drawdown_pct"] = "N/A"
    del bh["_bh_series_start"], bh["_bh_series_shares"]
    return bh


# ── Entry point ──────────────────────────────────────────────────────────────

def run_backtest(params: BacktestParams, price_data: pd.DataFrame | None = None) -> BacktestResult:
    """Fetch (or accept injected) price data, validate, simulate, and compute
    summary statistics for one backtest run.

    `price_data` is exposed purely for testability: synthetic tests build a
    tiny hand-crafted DataFrame (DatetimeIndex named 'date', columns
    open/high/low/close/volume) and inject it directly, bypassing yfinance
    and DuckDB entirely.
    """
    if price_data is None:
        price_data = swing_analysis.fetch_and_cache_swing_history(
            params.ticker, years=MAX_HISTORY_YEARS_DEFAULT
        )

    if price_data.empty:
        raise ValueError(f"No price data available for {params.ticker!r}.")

    # A blank start/end date means "as much history as is available" -- mutate
    # params in place (not just a local variable) so the resolved dates are
    # what gets persisted by the caller (storage.save_backtest_run reads
    # params.start_date/end_date after this call returns) and what every
    # downstream calculation (validation, simulation, summary stats,
    # buy-and-hold) uses consistently.
    if not params.start_date:
        params.start_date = str(price_data.index.min().date())
    if not params.end_date:
        params.end_date = str(price_data.index.max().date())

    warnings = validate_params(params, price_data)

    transactions, daily_records, sim_warnings, status, error_message = _simulate(params, price_data)
    warnings = warnings + sim_warnings

    txn_df = pd.DataFrame(transactions)
    daily_df = pd.DataFrame(daily_records)
    if not daily_df.empty and "_base_capital" in daily_df.columns:
        daily_df = daily_df.drop(columns=["_base_capital"])

    _check_invariants(daily_df)

    if status == "error":
        return BacktestResult(
            params=params, transactions=txn_df, daily_history=daily_df,
            summary={}, buy_and_hold={}, warnings=warnings,
            status="error", error_message=error_message,
        )

    summary = compute_summary_stats(txn_df, daily_df, params)
    first_buy_date = summary["strategy_period"]["first_buy_date"]
    bh = compute_buy_and_hold(price_data, first_buy_date, params.end_date, params.starting_capital)
    bh = _finalize_buy_and_hold(
        bh,
        summary["performance"]["final_portfolio_value"],
        summary["performance"]["total_return_pct"],
        summary["risk"].get("max_drawdown_pct", "N/A"),
        price_data,
    )

    return BacktestResult(
        params=params, transactions=txn_df, daily_history=daily_df,
        summary=summary, buy_and_hold=bh, warnings=warnings,
        status="complete", error_message=None,
    )
