# strategy.py
import csv
import io
import json
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request

from datetime import date, datetime, time as dt_time, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from dotenv import load_dotenv

from lumibot.strategies import Strategy

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    GetAssetsRequest,
    GetOptionContractsRequest,
    GetOrdersRequest,
)
from alpaca.data.requests import (
    MostActivesRequest,
    MarketMoversRequest,
    OptionBarsRequest,
    OptionSnapshotRequest,
    StockLatestQuoteRequest,
    CorporateActionsRequest,
)
from alpaca.trading.enums import (
    AssetClass,
    AssetStatus,
    ContractType,
    ExerciseStyle,
    QueryOrderStatus,
)
from alpaca.data.historical.option import (
    OptionHistoricalDataClient,
)
from alpaca.data.historical.corporate_actions import (
    CorporateActionsClient,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.data.historical.stock import (
    StockHistoricalDataClient,
)
from alpaca.data.historical.screener import ScreenerClient
from alpaca.data.requests import (
    MostActivesRequest,
    MarketMoversRequest,
)
from alpaca.data.enums import (
    MostActivesBy,
    MarketType,
    OptionsFeed,
)

load_dotenv()


class StockSuggestionStrategy(Strategy):

    parameters = {
        # --------------------------------------------------
        # AUTOMATIC UNIVERSE SETTINGS
        # --------------------------------------------------

        # Top stocks by current share volume
        "most_active_volume_count": 100,

        # Top stocks by current number of trades
        "most_active_trades_count": 100,

        # Number of gainers AND losers to include
        "market_movers_count": 50,

        # Major exchanges we want to consider
        "allowed_exchanges": [
            "NASDAQ",
            "NYSE",
            "ARCA",
            "NYSEARCA",
            "AMEX",
            "BATS",
        ],

        # --------------------------------------------------
        # ELIGIBILITY SETTINGS
        # --------------------------------------------------

        "minimum_price": 10.0,

        "minimum_dollar_volume": 100_000_000,

        "minimum_relative_volume": 0.75,

        "bullish_momentum_threshold": 0.03,

        "bearish_momentum_threshold": -0.03,

        # Number of results to display
        "top_results": 5,

        # --------------------------------------------------
        # MICRO-ACCOUNT FRACTIONAL-STOCK MODE
        # --------------------------------------------------

        # Automatically use the long-only fractional-stock
        # path while effective account equity is at or below
        # this threshold. This default aligns with Alpaca's
        # current sub-$2,000 no-margin/no-short regime.
        "micro_account_equity_threshold": 2000.0,

        # Require the bullish stock score to meet this level
        # before producing a micro-account alert.
        "micro_min_stock_score": 70.0,

        # Allocate 5% of account equity to one fractional
        # stock idea. With $100, this is $5 per idea.
        "micro_position_pct_equity": 0.05,

        # Hard dollar ceiling per micro position. The
        # percentage rule remains the binding constraint for
        # very small accounts.
        "micro_max_position_dollars": 100.0,

        # Cap all NEW micro alerts in one run at 25% of
        # effective equity. With five 5% positions, this
        # leaves at least 75% unallocated.
        "micro_total_allocation_pct_equity": 0.25,

        # Cumulative real-account stock exposure guard. New
        # micro alerts stop once broker stock gross market
        # value reaches 50% of effective equity. Set <= 0 to
        # disable this cap. Simulated test equity ignores it.
        "micro_max_broker_stock_gross_pct_equity": 0.50,

        # Avoid adding another micro alert for a stock already
        # held in the real Alpaca account. Simulated test equity
        # remains an isolated sizing sandbox.
        "micro_block_existing_stock_position": True,

        # Maximum new fractional-stock alerts per run.
        "micro_max_positions_per_run": 5,

        # Alpaca currently supports stock fractional
        # purchases as small as $1 notional.
        "micro_min_notional_dollars": 1.0,

        # Conservative stock-quote quality guard.
        "micro_stock_max_spread_pct": 0.01,

        # Current stock quote must be recent enough for the
        # alert's reference entry/stop/target calculations.
        "micro_stock_quote_max_age_seconds": 60,

        # Planning exits for the micro alert. These are
        # alert-only reference levels, not submitted orders.
        #
        # 5% position size * 8% stop ~= 0.4% account risk
        # per idea; five such ideas ~= 2% planned risk.
        "micro_stop_loss_pct": 0.08,
        "micro_profit_target_pct": 0.16,

        # --------------------------------------------------
        # OPTIONS ELIGIBILITY
        # --------------------------------------------------

        # Preferred expiration window.
        "option_min_dte": 21,
        "option_max_dte": 60,
        "option_target_dte": 35,

        # Keep strikes reasonably close to the underlying
        # before requesting quote/Greek data.
        "option_strike_band_pct": 0.40,

        # Liquidity requirements.
        "option_min_open_interest": 100,
        "option_min_mid_price": 0.25,

        # Maximum bid/ask spread as percentage of midpoint.
        "option_max_spread_pct": 0.20,

        # Directional long-option delta range.
        "option_min_abs_delta": 0.35,
        "option_max_abs_delta": 0.65,
        "option_target_abs_delta": 0.50,

        # Values used to normalize option scores.
        "option_open_interest_full_score": 5000,
        "option_quote_size_full_score": 50,

        # --------------------------------------------------
        # ACTUAL OPTION DAILY ACTIVITY
        # --------------------------------------------------

        # Current-session option volume comes from Alpaca
        # option daily bars, not open interest or quote size.
        # Keep the default at zero while this signal is being
        # validated live; set to 1+ to require a contract to
        # have traded during the current session.
        "option_min_daily_volume": 0,

        # Daily volume at or above this level earns the full
        # normalized volume component of option quality.
        "option_daily_volume_full_score": 1000,

        # Basic Alpaca market-data accounts cannot request the
        # latest 15 minutes of OPRA history. If a current-session
        # bar request is rejected for OPRA entitlement, retry with
        # a conservative 16-minute delay so actual exchange-trade
        # volume can still be used instead of an artificial zero.
        "option_daily_volume_delayed_fallback_minutes": 16,

        # Option-quality component weights. They sum to 1.00.
        # Daily traded volume now has its own 15% component.
        "option_score_spread_weight": 0.30,
        "option_score_delta_weight": 0.20,
        "option_score_open_interest_weight": 0.15,
        "option_score_dte_weight": 0.10,
        "option_score_quote_size_weight": 0.10,
        "option_score_daily_volume_weight": 0.15,

        # Keep several choices for each underlying.
        "option_top_contracts_per_stock": 3,

        # Number shown in final option table.
        "option_top_results": 10,

        # Combined ranking:
        #
        # 60% underlying stock setup
        # 40% option contract quality
        "option_stock_weight": 0.60,
        "option_contract_weight": 0.40,

        # --------------------------------------------------
        # TRADE STRUCTURE RANKING
        # --------------------------------------------------

        # Minimum final structure score required to produce
        # a trade suggestion. Anything below this becomes
        # NO TRADE.
        "trade_structure_min_score": 70.0,

        # --------------------------------------------------
        # IV HISTORICAL CONTEXT
        # --------------------------------------------------

        # Persist one representative scanner-observed IV
        # sample per underlying + option type + calendar day.
        # This is an observed-history percentile/rank, not a
        # vendor-provided historical-IV series.
        "option_iv_history_lookback_samples": 252,
        "option_iv_history_min_samples": 20,

        # Once enough observed history exists, long-premium
        # penalties key off percentile instead of absolute IV.
        "long_option_high_iv_percentile_threshold": 0.80,

        # Warm-up fallback while historical context has fewer
        # than option_iv_history_min_samples observations.
        "long_option_high_iv_threshold": 0.60,
        "long_option_max_iv_penalty": 15.0,

        # --------------------------------------------------
        # EVENT / EARNINGS RISK
        # --------------------------------------------------

        # Corporate actions and configured earnings dates are
        # checked through the full option expiration horizon.
        "event_risk_lookahead_days": 60,

        # Include older Corporate Actions API process records
        # so already-announced future ex/effective dates are
        # still discoverable and then filtered locally.
        "event_risk_history_days": 180,

        # Earnings before expiration are conservatively blocked
        # when an earnings calendar is actually available.
        "event_risk_block_earnings_before_expiration": True,

        # Automatic earnings-calendar cache policy. The provider
        # is refreshed at most once per this many hours; this keeps
        # the full-calendar request well inside low API-rate tiers.
        "earnings_calendar_refresh_hours": 6.0,

        # A failed refresh may still expose a recent cached calendar
        # for diagnostics/known-event blocking up to this age. Stale
        # data is never silently treated as a fresh CLEAR signal.
        "earnings_calendar_max_stale_hours": 72.0,

        # Network timeout for the automatic calendar provider.
        "earnings_calendar_request_timeout_seconds": 15.0,

        # Fail closed on stale/unavailable earnings data by default.
        # Set False only if you intentionally accept earnings-unknown
        # option entries during a provider outage or missing API key.
        "earnings_calendar_fail_closed": True,

        # Ordinary cash dividends are warnings/score penalties,
        # not automatic hard blocks. Structural corporate actions
        # (splits, mergers, spin-offs, etc.) are hard blocks.
        "event_risk_cash_dividend_score_penalty": 5.0,

        # Vertical spread width as a percentage of the
        # underlying price.
        "vertical_min_width_pct": 0.02,
        "vertical_max_width_pct": 0.10,

        # OTM short-leg delta range.
        "vertical_short_min_abs_delta": 0.15,
        "vertical_short_max_abs_delta": 0.40,

        # Require a minimum max-reward / max-risk ratio for
        # debit verticals.
        "vertical_min_reward_risk": 0.75,

        # Reward/risk value that earns a full score in the
        # vertical structure-quality calculation.
        "vertical_full_reward_risk_score": 1.50,

        # --------------------------------------------------
        # POSITION SIZING
        # --------------------------------------------------

        # Default risk budget per new options idea:
        # 0.5% of account equity.
        #
        # This is a configurable safety default, not a
        # universal recommendation.
        "position_risk_pct_equity": 0.005,

        # Hard dollar ceiling for max loss on one new idea.
        "position_max_risk_dollars": 1000.0,

        # Also cap one trade at 10% of currently available
        # Alpaca options buying power.
        "position_max_options_bp_pct_per_trade": 0.10,

        # Portfolio-level cap for all NEW alerts generated
        # in one scanner run.
        "position_total_new_risk_pct_equity": 0.02,

        # Do not allocate more than 25% of current options
        # buying power across all new alerts in one run.
        "position_max_options_bp_pct_total": 0.25,

        # Prevent cheap spreads from producing an
        # impractically large contract count.
        "position_max_contracts_per_trade": 10,

        # Maximum number of actionable alerts in one run.
        "position_max_alerts_per_run": 5,

        # --------------------------------------------------
        # PORTFOLIO EXPOSURE CONTROLS
        # --------------------------------------------------

        # Risk-reserved lifecycle setups carry exposure across
        # scanner runs. Fresh ALERTED records reserve only during
        # their short broker-match window; broker-confirmed states
        # remain reserved until exposure becomes terminal.
        "portfolio_max_active_tracked_risk_pct_equity": 0.05,

        # Cap one directional book (bullish or bearish) at 3%
        # of equity in alert-estimated max risk.
        "portfolio_max_directional_tracked_risk_pct_equity": 0.03,

        # Cap one expiration bucket at 2% of equity in
        # alert-estimated max risk.
        "portfolio_max_expiration_tracked_risk_pct_equity": 0.02,

        # Limit the number of simultaneously risk-reserved option
        # setups in the persistent lifecycle ledger.
        "portfolio_max_active_tracked_setups": 10,

        # Avoid stacking multiple independent option ideas on
        # the same underlying while an older setup still reserves risk.
        "portfolio_max_active_tracked_setups_per_underlying": 1,

        # Read Alpaca's current open positions before sizing new
        # option alerts. If this snapshot cannot be read, fail
        # closed by default rather than assume the book is empty.
        "portfolio_require_broker_positions_snapshot": True,

        # Broker option positions are measured by gross absolute
        # market value. This is intentionally conservative and is
        # NOT a max-loss calculation for multi-leg positions.
        "portfolio_max_broker_options_gross_pct_equity": 0.25,

        # Do not add a new option idea on an underlying that
        # already has an open broker option position.
        "portfolio_block_new_same_underlying_broker_option_position": True,

        # --------------------------------------------------
        # PERSISTENT TRADE LIFECYCLE + BROKER RECONCILIATION
        # --------------------------------------------------

        # Reconcile alert-tracked ideas to current Alpaca
        # positions and recent orders. This is read-only: no
        # submit/replace/cancel/close API is called here.
        "lifecycle_reconciliation_enabled": True,

        # Position snapshots are primary truth for whether a
        # tracked idea is currently open. If unavailable, keep
        # prior lifecycle state rather than guessing.
        "lifecycle_require_positions_snapshot": True,

        # Order history is supplemental evidence used for
        # ENTRY_WORKING/CANCELED/REJECTED/EXPIRED and for
        # detecting working close orders. Missing order history
        # must not silently terminalize an unmatched alert.
        "lifecycle_require_orders_for_terminal_inference": True,

        # Alpaca GetOrdersRequest supports at most 500 orders.
        # Limit the history window so reconciliation stays
        # bounded; a full-limit response is treated as truncated.
        "lifecycle_order_lookback_days": 90,
        "lifecycle_order_limit": 500,

        # An ALERTED idea with no matching position or order may
        # become STALE_UNCONFIRMED after this many calendar days,
        # but only when order history is available and untruncated.
        "lifecycle_alert_match_grace_days": 2,

        # ALERTED recommendations reserve portfolio risk only for
        # this short window while waiting for broker/order evidence.
        # After the window expires, a fully reconciled unmatched
        # alert keeps its historical ALERTED lifecycle record but
        # releases its portfolio-risk reservation. Broker-confirmed
        # states such as ENTRY_WORKING/OPEN always reserve risk.
        "lifecycle_alert_risk_reservation_minutes": 60.0,

        # Bound persistent transition history per tracked idea.
        "lifecycle_history_max_events": 100,

        # Small tolerance for broker quantity comparisons.
        "lifecycle_quantity_tolerance": 1e-6,

        # --------------------------------------------------
        # ALERT GENERATION
        # --------------------------------------------------

        # An actionable alert must meet at least this
        # structure score after all prior filters.
        "alert_min_structure_score": 70.0,

        # Prevent duplicate alerts for the exact same setup
        # during the same calendar day, including across
        # paper.py restarts.
        "alert_once_per_day": True,

        # --------------------------------------------------
        # EXIT MANAGEMENT
        # --------------------------------------------------

        # Close when the conservative mark reaches +50%
        # versus the alert-estimated entry debit.
        "exit_profit_target_pct": 0.50,

        # Close when the conservative mark reaches -50%
        # versus the alert-estimated entry debit.
        "exit_max_loss_pct": 0.50,

        # Hard time-to-expiration exit.
        "exit_dte_days": 7,

        # Before the hard DTE exit, emit an ADJUST alert
        # so a still-valid thesis can be reviewed/rolled.
        "adjust_dte_days": 14,

        # Hard maximum calendar holding period.
        "exit_max_holding_days": 21,

        # Use underlying price/SMA20/momentum20 to evaluate
        # whether the original directional thesis is intact.
        "exit_thesis_invalidation_enabled": True,

        # CLOSE/ADJUST alerts repeat at most once per
        # calendar day for the same tracked setup/action.
        "exit_alert_once_per_day": True,

        # --------------------------------------------------
        # OPTIONS MARKET-SESSION GATING
        # --------------------------------------------------

        # Keep the LumiBot process on MARKET=24/7, but only
        # allow stock/options decisions while Alpaca's market
        # clock says the regular market is open.
        "options_session_gate_enabled": True,

        # Avoid the first five minutes after 9:30 ET.
        "options_session_open_buffer_minutes": 5,

        # Stop five minutes before Alpaca's reported close.
        # This also adapts to early-close sessions because it
        # uses clock.next_close rather than assuming 4:00 ET.
        "options_session_close_buffer_minutes": 5,

        # When a 24/7 strategy wakes outside the actionable
        # options window, retry cheaply instead of sleeping
        # another full day at the same unusable time.
        "options_closed_retry_sleeptime": "15M",

        # After a valid in-session full scan, return to the
        # original once-per-day cadence.
        "options_active_sleeptime": "1D",

        # --------------------------------------------------
        # OPTION QUOTE FRESHNESS
        # --------------------------------------------------

        # Reject option quotes older than this many seconds.
        "option_quote_max_age_seconds": 120,

        # Allow tiny clock skew between Alpaca and the host.
        "option_quote_future_tolerance_seconds": 5,

        # Missing quote timestamps fail closed.
        "option_quote_require_timestamp": True,
    }

    def initialize(self):
        self.sleeptime = (
            self.parameters[
                "options_active_sleeptime"
            ]
        )

        # Updated from Alpaca's market clock whenever the
        # options-session gate is checked.
        self._option_quote_reference_time = None

        api_key = os.environ["ALPACA_API_KEY"]
        api_secret = os.environ["ALPACA_API_SECRET"]

        paper = (
            os.environ.get(
                "ALPACA_IS_PAPER",
                "true",
            ).lower()
            == "true"
        )

        self.alpaca_trading_client = TradingClient(
            api_key,
            api_secret,
            paper=paper,
        )

        self.alpaca_screener_client = ScreenerClient(
            api_key,
            api_secret,
        )

                # --------------------------------------------------
        # OPTIONS MARKET DATA CLIENT
        # --------------------------------------------------

        self.alpaca_option_data_client = (
            OptionHistoricalDataClient(
                api_key,
                api_secret,
            )
        )

        self.alpaca_stock_data_client = (
            StockHistoricalDataClient(
                api_key,
                api_secret,
            )
        )

        self.alpaca_corporate_actions_client = (
            CorporateActionsClient(
                api_key,
                api_secret,
            )
        )

        # --------------------------------------------------
        # OPTIONAL OPTIONS DATA FEED
        # --------------------------------------------------
        #
        # .env examples:
        #
        # ALPACA_OPTIONS_FEED=indicative
        #
        # or:
        #
        # ALPACA_OPTIONS_FEED=opra
        #
        # If unset, let Alpaca choose the appropriate
        # default for the account.
        # --------------------------------------------------

        feed_name = (
            os.environ.get(
                "ALPACA_OPTIONS_FEED",
                "",
            )
            .strip()
            .lower()
        )

        if feed_name == "opra":

            self.alpaca_options_feed = (
                OptionsFeed.OPRA
            )

        elif feed_name == "indicative":

            self.alpaca_options_feed = (
                OptionsFeed.INDICATIVE
            )

        elif feed_name == "":

            self.alpaca_options_feed = None

        else:

            raise ValueError(
                "ALPACA_OPTIONS_FEED must be "
                "'opra', 'indicative', or unset."
            )

        # --------------------------------------------------
        # ACCOUNT OPTIONS PERMISSION
        # --------------------------------------------------

        # --------------------------------------------------
        # ALERT RUNTIME SETTINGS
        # --------------------------------------------------
        #
        # Console alerts are enabled by default.
        #
        # Optional .env:
        #
        # TRADE_ALERTS_ENABLED=true
        # TRADE_ALERTS_JSONL_PATH=trade_alerts.jsonl
        #
        # JSONL is optional. If unset, alerts are logged only.
        # --------------------------------------------------

        # --------------------------------------------------
        # MICRO-ACCOUNT RUNTIME SETTINGS
        # --------------------------------------------------
        #
        # Real $100 account:
        #   no override needed; equity activates micro mode.
        #
        # Testing with a larger paper account:
        #   MICRO_ACCOUNT_TEST_EQUITY=100
        #
        # Optional forced routing without a test balance:
        #   MICRO_ACCOUNT_FORCE=true
        # --------------------------------------------------

        self.micro_account_force = (
            os.environ.get(
                "MICRO_ACCOUNT_FORCE",
                "false",
            )
            .strip()
            .lower()
            in (
                "1",
                "true",
                "yes",
                "y",
                "on",
            )
        )

        raw_micro_test_equity = (
            os.environ.get(
                "MICRO_ACCOUNT_TEST_EQUITY",
                "",
            )
            .strip()
        )

        self.micro_account_test_equity = None

        if raw_micro_test_equity:

            try:

                parsed_micro_test_equity = float(
                    raw_micro_test_equity
                )

                if parsed_micro_test_equity <= 0:

                    raise ValueError(
                        "must be greater than zero"
                    )

                self.micro_account_test_equity = (
                    parsed_micro_test_equity
                )

            except ValueError as exc:

                raise ValueError(
                    "MICRO_ACCOUNT_TEST_EQUITY "
                    "must be a positive number."
                ) from exc

        self.trade_alerts_enabled = (
            os.environ.get(
                "TRADE_ALERTS_ENABLED",
                "true",
            )
            .strip()
            .lower()
            in (
                "1",
                "true",
                "yes",
                "y",
                "on",
            )
        )

        self.trade_alerts_jsonl_path = (
            os.environ.get(
                "TRADE_ALERTS_JSONL_PATH",
                "",
            )
            .strip()
        )

        # --------------------------------------------------
        # PERSISTENT ALERT DEDUPLICATION
        # --------------------------------------------------
        #
        # Optional .env:
        #
        # TRADE_ALERT_DEDUPE_PATH=.trade_alert_dedupe.json
        #
        # The file stores only the current calendar day's
        # alert keys. It survives process restarts but resets
        # automatically on a new day.
        # --------------------------------------------------

        self.trade_alert_dedupe_path = (
            os.environ.get(
                "TRADE_ALERT_DEDUPE_PATH",
                ".trade_alert_dedupe.json",
            )
            .strip()
        )

        self._sent_trade_alert_keys = set()

        self._load_trade_alert_dedupe_state()

        # --------------------------------------------------
        # PERSISTENT ALERT-TRACKED SETUPS
        # --------------------------------------------------
        #
        # This file is now a persistent lifecycle ledger. New
        # alerts start as ALERTED and are reconciled read-only
        # against Alpaca positions/orders into states such as
        # ENTRY_WORKING, OPEN, PARTIALLY_CLOSED, and CLOSED.
        #
        # Optional .env:
        #
        # TRADE_ALERT_POSITIONS_PATH=.trade_alert_positions.json
        # TRADE_LIFECYCLE_JSONL_PATH=trade_lifecycle.jsonl
        # EXIT_ALERTS_JSONL_PATH=exit_alerts.jsonl
        # --------------------------------------------------

        self.trade_alert_positions_path = (
            os.environ.get(
                "TRADE_ALERT_POSITIONS_PATH",
                ".trade_alert_positions.json",
            )
            .strip()
        )

        self.exit_alerts_jsonl_path = (
            os.environ.get(
                "EXIT_ALERTS_JSONL_PATH",
                "",
            )
            .strip()
        )

        self.trade_lifecycle_jsonl_path = (
            os.environ.get(
                "TRADE_LIFECYCLE_JSONL_PATH",
                "",
            )
            .strip()
        )

        self._tracked_alert_positions = {}

        self._load_trade_alert_positions_state()

        # --------------------------------------------------
        # IV HISTORY + EVENT-RISK RUNTIME STATE
        # --------------------------------------------------
        #
        # Optional .env:
        #
        # OPTION_IV_HISTORY_PATH=.option_iv_history.json
        #
        # Automatic earnings calendar (default provider):
        # EARNINGS_CALENDAR_PROVIDER=ALPHAVANTAGE
        # ALPHAVANTAGE_API_KEY=your_key_here
        # EARNINGS_CALENDAR_CACHE_PATH=.earnings_calendar_cache.json
        # EARNINGS_CALENDAR_HORIZON=3month
        #
        # Optional manual override/fallback. If configured and valid,
        # this takes precedence over automatic provider retrieval:
        # EARNINGS_CALENDAR_PATH=earnings_calendar.csv
        #
        # Earnings CSV columns:
        #   symbol,earnings_date
        # --------------------------------------------------

        self.option_iv_history_path = (
            os.environ.get(
                "OPTION_IV_HISTORY_PATH",
                ".option_iv_history.json",
            )
            .strip()
        )

        self._option_iv_history = {}
        self._load_option_iv_history_state()

        self.earnings_calendar_path = (
            os.environ.get(
                "EARNINGS_CALENDAR_PATH",
                "",
            )
            .strip()
        )

        self.earnings_calendar_provider = (
            os.environ.get(
                "EARNINGS_CALENDAR_PROVIDER",
                "ALPHAVANTAGE",
            )
            .strip()
            .upper()
        )

        self.earnings_calendar_cache_path = (
            os.environ.get(
                "EARNINGS_CALENDAR_CACHE_PATH",
                ".earnings_calendar_cache.json",
            )
            .strip()
        )

        self.earnings_calendar_horizon = (
            os.environ.get(
                "EARNINGS_CALENDAR_HORIZON",
                "3month",
            )
            .strip()
            .lower()
        )

        if self.earnings_calendar_horizon not in {
            "3month",
            "6month",
            "12month",
        }:
            self.log_message(
                "Unsupported EARNINGS_CALENDAR_HORIZON="
                f"{self.earnings_calendar_horizon!r}; using 3month."
            )
            self.earnings_calendar_horizon = "3month"

        self.alphavantage_api_key = (
            os.environ.get(
                "ALPHAVANTAGE_API_KEY",
                "",
            )
            .strip()
        )

        self._event_risk_by_symbol = {}
        self._event_risk_corporate_actions_available = False
        self._event_risk_earnings_available = False
        self._event_risk_earnings_freshness = "UNAVAILABLE"
        self._event_risk_earnings_source = "NONE"
        self._event_risk_earnings_cache_age_hours = None
        self._event_risk_earnings_reason = "not loaded yet"

        self.options_trading_level = None

        try:

            account = (
                self.alpaca_trading_client
                .get_account()
            )

            self.options_trading_level = (
                getattr(
                    account,
                    "options_trading_level",
                    None,
                )
            )

            self.log_message(
                "Alpaca options trading level: "
                f"{self.options_trading_level}"
            )

        except Exception as exc:

            self.log_message(
                "Could not determine Alpaca "
                f"options trading level: {exc}"
            )
    @staticmethod
    def _is_fund_or_leveraged_product(asset):
        """
        Return True for ETFs, ETNs, funds, trusts used as
        exchange-traded products, and leveraged/inverse products.

        Alpaca classifies both common stocks and ETFs as
        US_EQUITY, so we use the official asset name here.
        """

        name = (
            getattr(asset, "name", "")
            or ""
        )

        name = " ".join(
            name.upper().split()
        )

        if not name:
            return False

        # --------------------------------------------------
        # POSITIVE COMMON-EQUITY MARKERS
        # --------------------------------------------------
        #
        # If Alpaca explicitly says this is common stock,
        # don't accidentally reject it because the company
        # happens to contain words like Trust or Fund.
        # --------------------------------------------------

        common_equity_markers = (
            "COMMON STOCK",
            "COMMON SHARES",
            "ORDINARY SHARE",
            "ORDINARY SHARES",
            "SUBORDINATE VOTING SHARES",
            "VOTING COMMON SHARES",
        )

        if any(
            marker in name
            for marker in common_equity_markers
        ):
            return False

        # --------------------------------------------------
        # EXPLICIT FUND / NOTE PRODUCT TERMS
        # --------------------------------------------------

        product_terms = (
            " ETF",
            "ETF ",
            " ETN",
            "ETN ",
            "EXCHANGE TRADED FUND",
            "EXCHANGE-TRADED FUND",
            "EXCHANGE TRADED NOTE",
            "EXCHANGE-TRADED NOTE",
            "INDEX FUND",
            "BOND FUND",
            "TREASURY FUND",
            "MONEY MARKET FUND",
            "CLOSED-END FUND",
            "CLOSED END FUND",
            "STRUCTURED NOTE",
            "NOTES DUE",
            "ULTRAPRO",
            "ULTRASHORT",
            "LEVERAGED",
            "INVERSE",
        )

        padded_name = f" {name} "

        if any(
            term in padded_name
            for term in product_terms
        ):
            return True

        # --------------------------------------------------
        # LEVERAGED / INVERSE PATTERNS
        # --------------------------------------------------
        #
        # Examples:
        #
        # 2X
        # 3X
        # Daily Bull
        # Daily Bear
        # Daily Long
        # Daily Short
        # --------------------------------------------------

        if re.search(
            r"\b[234]X\b",
            name,
        ):
            return True

        if re.search(
            r"\bDAILY\b.*\b"
            r"(BULL|BEAR|LONG|SHORT)\b",
            name,
        ):
            return True

        # --------------------------------------------------
        # COMMON ETF / ETP ISSUERS
        # --------------------------------------------------
        #
        # Common-stock names get through above, so this will
        # not reject an issuer's own public stock if Alpaca
        # labels it "Common Stock".
        # --------------------------------------------------

        fund_issuer_prefixes = (
            "ISHARES ",
            "SPDR ",
            "PROSHARES ",
            "DIREXION ",
            "VANGUARD ",
            "VANECK ",
            "GLOBAL X ",
            "KRANESHARES ",
            "WISDOMTREE ",
            "GRANITESHARES ",
            "ROUNDHILL ",
            "DEFIANCE ",
            "YIELDMAX ",
            "T-REX ",
            "REX ",
            "INNOVATOR ",
            "INVESCO ",
            "PIMCO ",
            "FIRST TRUST ",
            "FRANKLIN ",
        )

        if name.startswith(
            fund_issuer_prefixes
        ):
            return True

        # --------------------------------------------------
        # GENERIC FUND NAMES
        # --------------------------------------------------

        if re.search(
            r"\bFUND\b",
            name,
        ):
            return True

        # --------------------------------------------------
        # INVESTMENT TRUST PRODUCTS
        # --------------------------------------------------
        #
        # Preserve REIT/common-equity style trusts.
        # Remove things like QQQ Trust / commodity trusts.
        # --------------------------------------------------

        if "TRUST" in name:

            reit_markers = (
                "REALTY TRUST",
                "REAL ESTATE INVESTMENT TRUST",
                "REIT",
            )

            if not any(
                marker in name
                for marker in reit_markers
            ):
                return True

        return False

    # ======================================================
    # AUTOMATIC UNIVERSE GENERATOR
    # ======================================================

    def build_universe(self):

        self.log_message(
            "Building automatic stock universe..."
        )

        # --------------------------------------------------
        # GET ALL ACTIVE US EQUITIES
        # --------------------------------------------------

        request = GetAssetsRequest(
            status=AssetStatus.ACTIVE,
            asset_class=AssetClass.US_EQUITY,
        )

        assets = (
            self.alpaca_trading_client
            .get_all_assets(request)
        )

        valid_assets = {}

        excluded_product_symbols = set()

        major_exchange_count = 0

        allowed_exchanges = set(
            self.parameters[
                "allowed_exchanges"
            ]
        )

        # --------------------------------------------------
        # FILTER MASTER ASSET LIST
        # --------------------------------------------------

        for asset in assets:

            # Must be tradable.
            if not asset.tradable:
                continue

            symbol = asset.symbol.upper()

            exchange = getattr(
                asset.exchange,
                "value",
                str(asset.exchange),
            )

            # Only major exchanges.
            if exchange not in allowed_exchanges:
                continue

            major_exchange_count += 1

            name = (
                asset.name.upper()
                if asset.name
                else ""
            )

            # --------------------------------------------------
            # REMOVE NON-COMMON EQUITY SECURITY TYPES
            # --------------------------------------------------

            excluded_name_terms = (
                "PREFERRED",
                "DEPOSITARY SHARES",
                "DEPOSITARY SHARE",
                "DEPOSITARY RECEIPT",
            )

            if any(
                term in name
                for term in excluded_name_terms
            ):
                continue


            # Catch:
            #
            # Warrant / Warrants
            # Right / Rights
            # Unit / Units
            #
            # Use word boundaries so a company name containing
            # something like "United" does not get rejected.
            if re.search(
                r"\b(WARRANTS?|RIGHTS?|UNITS?)\b",
                name,
            ):
                continue

            # --------------------------------------------------
            # REMOVE ETF / ETN / FUND / LEVERAGED PRODUCTS
            # --------------------------------------------------

            if self._is_fund_or_leveraged_product(
                asset
            ):

                excluded_product_symbols.add(
                    symbol
                )

                continue

            # --------------------------------------------------
            # REMOVE UNUSUAL SYMBOL STRUCTURES
            # --------------------------------------------------

            if not re.fullmatch(
                r"[A-Z]{1,6}([.-][A-Z])?",
                symbol,
            ):
                continue

            valid_assets[symbol] = asset

        self.log_message(
            f"Alpaca reports "
            f"{major_exchange_count} active/tradable "
            f"major-exchange US equities."
        )

        self.log_message(
            f"After removing ETF/ETN/fund/"
            f"leveraged products, "
            f"{len(valid_assets)} stock-like "
            f"equities remain."
        )

        # --------------------------------------------------
        # MOST ACTIVE BY VOLUME
        # --------------------------------------------------

        active_volume_response = (
            self.alpaca_screener_client
            .get_most_actives(
                MostActivesRequest(
                    top=self.parameters[
                        "most_active_volume_count"
                    ],
                    by=MostActivesBy.VOLUME,
                )
            )
        )

        active_volume = {
            stock.symbol
            for stock
            in active_volume_response.most_actives
        }

        # --------------------------------------------------
        # MOST ACTIVE BY NUMBER OF TRADES
        # --------------------------------------------------

        active_trades_response = (
            self.alpaca_screener_client
            .get_most_actives(
                MostActivesRequest(
                    top=self.parameters[
                        "most_active_trades_count"
                    ],
                    by=MostActivesBy.TRADES,
                )
            )
        )

        active_trades = {
            stock.symbol
            for stock
            in active_trades_response.most_actives
        }

        # --------------------------------------------------
        # MARKET MOVERS
        # --------------------------------------------------

        movers_response = (
            self.alpaca_screener_client
            .get_market_movers(
                MarketMoversRequest(
                    top=self.parameters[
                        "market_movers_count"
                    ],
                    market_type=MarketType.STOCKS,
                )
            )
        )

        gainers = {
            stock.symbol
            for stock
            in movers_response.gainers
        }

        losers = {
            stock.symbol
            for stock
            in movers_response.losers
        }

        # --------------------------------------------------
        # COMBINE SCREENER RESULTS
        # --------------------------------------------------

        candidate_symbols = (
            active_volume
            | active_trades
            | gainers
            | losers
        )

        self.log_message(
            f"Dynamic screener produced "
            f"{len(candidate_symbols)} unique symbols."
        )

        # --------------------------------------------------
        # SHOW PRODUCTS REMOVED FROM TODAY'S SCREEN
        # --------------------------------------------------

        removed_products = sorted(
            candidate_symbols
            & excluded_product_symbols
        )

        if removed_products:

            self.log_message(
                f"Removed "
                f"{len(removed_products)} ETF/ETN/"
                f"fund/leveraged screener hits: "
                + ", ".join(
                    removed_products
                )
            )

        # --------------------------------------------------
        # KEEP ONLY VALID COMMON-STOCK-LIKE EQUITIES
        # --------------------------------------------------

        universe = sorted(
            symbol
            for symbol in candidate_symbols
            if symbol in valid_assets
        )

        self.log_message(
            f"Automatic stock universe contains "
            f"{len(universe)} valid stocks."
        )

        # Preserve Alpaca Asset objects so micro-account mode
        # can enforce Asset.fractionable without another
        # per-symbol Trading API request.
        self._stock_asset_by_symbol = (
            valid_assets
        )

        return universe

    # ======================================================
    # STOCK / OPTIONS MARKET-SESSION GATE
    # ======================================================

    def _get_options_session_status(
        self,
    ):
        """
        Fail-closed gate for actionable option decisions.

        Alpaca's market clock determines whether the regular
        market is open and supplies the actual next close.
        We then apply configurable opening/closing buffers.
        """

        if not self.parameters[
            "options_session_gate_enabled"
        ]:

            now_utc = datetime.now(
                timezone.utc
            )

            self._option_quote_reference_time = (
                now_utc
            )

            return {
                "allowed":
                    True,

                "reason":
                    "session gate disabled",

                "now":
                    now_utc,

                "actionable_open":
                    None,

                "actionable_close":
                    None,
            }

        try:

            clock = (
                self.alpaca_trading_client
                .get_clock()
            )

        except Exception as exc:

            return {
                "allowed":
                    False,

                "reason":
                    (
                        "could not read Alpaca "
                        f"market clock: {exc}"
                    ),

                "now":
                    None,

                "actionable_open":
                    None,

                "actionable_close":
                    None,
            }

        market_now = getattr(
            clock,
            "timestamp",
            None,
        )

        if market_now is None:

            return {
                "allowed":
                    False,

                "reason":
                    (
                        "Alpaca market clock "
                        "returned no timestamp"
                    ),

                "now":
                    None,

                "actionable_open":
                    None,

                "actionable_close":
                    None,
            }

        if isinstance(
            market_now,
            str,
        ):

            try:

                market_now = (
                    datetime.fromisoformat(
                        market_now.replace(
                            "Z",
                            "+00:00",
                        )
                    )
                )

            except ValueError:

                return {
                    "allowed":
                        False,

                    "reason":
                        (
                            "Alpaca market clock "
                            "timestamp could not "
                            "be parsed"
                        ),

                    "now":
                        None,

                    "actionable_open":
                        None,

                    "actionable_close":
                        None,
                }

        if market_now.tzinfo is None:

            market_now = (
                market_now.replace(
                    tzinfo=timezone.utc
                )
            )

        market_now_utc = (
            market_now.astimezone(
                timezone.utc
            )
        )

        self._option_quote_reference_time = (
            market_now_utc
        )

        eastern = ZoneInfo(
            "America/New_York"
        )

        market_now_et = (
            market_now.astimezone(
                eastern
            )
        )

        is_open = bool(
            getattr(
                clock,
                "is_open",
                False,
            )
        )

        if not is_open:

            next_open = getattr(
                clock,
                "next_open",
                None,
            )

            next_open_text = ""

            if next_open is not None:

                try:

                    if isinstance(
                        next_open,
                        str,
                    ):

                        next_open = (
                            datetime.fromisoformat(
                                next_open.replace(
                                    "Z",
                                    "+00:00",
                                )
                            )
                        )

                    if next_open.tzinfo is None:

                        next_open = (
                            next_open.replace(
                                tzinfo=timezone.utc
                            )
                        )

                    next_open_text = (
                        "; next regular open "
                        + next_open.astimezone(
                            eastern
                        )
                        .strftime(
                            "%Y-%m-%d %I:%M %p ET"
                        )
                    )

                except Exception:

                    next_open_text = ""

            return {
                "allowed":
                    False,

                "reason":
                    (
                        "Alpaca market clock "
                        "reports regular market "
                        "closed"
                        + next_open_text
                    ),

                "now":
                    market_now_et,

                "actionable_open":
                    None,

                "actionable_close":
                    None,
            }

        regular_open_et = (
            datetime.combine(
                market_now_et.date(),
                dt_time(
                    9,
                    30,
                ),
                tzinfo=eastern,
            )
        )

        actionable_open = (
            regular_open_et
            + timedelta(
                minutes=self.parameters[
                    "options_session_open_buffer_minutes"
                ]
            )
        )

        next_close = getattr(
            clock,
            "next_close",
            None,
        )

        if next_close is None:

            return {
                "allowed":
                    False,

                "reason":
                    (
                        "Alpaca market clock "
                        "returned no next_close"
                    ),

                "now":
                    market_now_et,

                "actionable_open":
                    actionable_open,

                "actionable_close":
                    None,
            }

        if isinstance(
            next_close,
            str,
        ):

            try:

                next_close = (
                    datetime.fromisoformat(
                        next_close.replace(
                            "Z",
                            "+00:00",
                        )
                    )
                )

            except ValueError:

                return {
                    "allowed":
                        False,

                    "reason":
                        (
                            "Alpaca next_close "
                            "could not be parsed"
                        ),

                    "now":
                        market_now_et,

                    "actionable_open":
                        actionable_open,

                    "actionable_close":
                        None,
                }

        if next_close.tzinfo is None:

            next_close = (
                next_close.replace(
                    tzinfo=timezone.utc
                )
            )

        actionable_close = (
            next_close.astimezone(
                eastern
            )
            - timedelta(
                minutes=self.parameters[
                    "options_session_close_buffer_minutes"
                ]
            )
        )

        if (
            market_now_et
            < actionable_open
        ):

            return {
                "allowed":
                    False,

                "reason":
                    (
                        "inside regular session "
                        "but still inside opening "
                        "buffer"
                    ),

                "now":
                    market_now_et,

                "actionable_open":
                    actionable_open,

                "actionable_close":
                    actionable_close,
            }

        if (
            market_now_et
            >= actionable_close
        ):

            return {
                "allowed":
                    False,

                "reason":
                    (
                        "inside regular session "
                        "but inside closing buffer"
                    ),

                "now":
                    market_now_et,

                "actionable_open":
                    actionable_open,

                "actionable_close":
                    actionable_close,
            }

        return {
            "allowed":
                True,

            "reason":
                "actionable options window",

            "now":
                market_now_et,

            "actionable_open":
                actionable_open,

            "actionable_close":
                actionable_close,
        }


    # ======================================================
    # OPTION QUOTE FRESHNESS
    # ======================================================

    def _option_quote_age_seconds(
        self,
        quote,
    ):

        if quote is None:
            return None

        quote_timestamp = getattr(
            quote,
            "timestamp",
            None,
        )

        if quote_timestamp is None:
            return None

        if isinstance(
            quote_timestamp,
            str,
        ):

            try:

                quote_timestamp = (
                    datetime.fromisoformat(
                        quote_timestamp.replace(
                            "Z",
                            "+00:00",
                        )
                    )
                )

            except ValueError:

                return None

        if quote_timestamp.tzinfo is None:

            quote_timestamp = (
                quote_timestamp.replace(
                    tzinfo=timezone.utc
                )
            )

        reference_time = (
            datetime.now(
                timezone.utc
            )
        )

        return (
            reference_time
            - quote_timestamp.astimezone(
                timezone.utc
            )
        ).total_seconds()


    def _is_option_quote_fresh(
        self,
        quote,
    ):

        age_seconds = (
            self._option_quote_age_seconds(
                quote
            )
        )

        if age_seconds is None:

            return (
                not self.parameters[
                    "option_quote_require_timestamp"
                ],
                None,
            )

        future_tolerance = float(
            self.parameters[
                "option_quote_future_tolerance_seconds"
            ]
        )

        maximum_age = float(
            self.parameters[
                "option_quote_max_age_seconds"
            ]
        )

        if (
            age_seconds
            < -future_tolerance
        ):

            return (
                False,
                age_seconds,
            )

        return (
            age_seconds
            <= maximum_age,
            age_seconds,
        )



    # ======================================================
    # ACTUAL OPTION DAILY ACTIVITY
    # ======================================================

    def _get_option_daily_activity(
        self,
        contract_symbols,
    ):
        """
        Return cumulative current-session option bar volume and
        trade count keyed by option contract symbol.

        Real-time OPRA history is attempted first. If Alpaca rejects
        the request because the account lacks current OPRA access,
        retry through the permitted delayed historical window. This
        preserves actual traded volume while making the data delay
        explicit; open interest and quote size are never substituted.
        """

        activity = {
            str(symbol): {
                "volume": 0.0,
                "trade_count": 0.0,
                "vwap": None,
                "bar_timestamp": None,
                "has_bar": False,
                "data_status": "UNKNOWN",
                "data_delay_minutes": None,
                "data_asof_utc": None,
            }
            for symbol in contract_symbols
        }

        if not contract_symbols:
            return activity

        reference_time = (
            self._option_quote_reference_time
            or datetime.now(timezone.utc)
        )

        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(
                tzinfo=timezone.utc
            )

        reference_time_utc = (
            reference_time
            .astimezone(timezone.utc)
        )

        eastern = ZoneInfo(
            "America/New_York"
        )

        trading_day = (
            reference_time
            .astimezone(eastern)
            .date()
        )

        start_et = datetime.combine(
            trading_day,
            dt_time(0, 0),
            tzinfo=eastern,
        )

        start_utc = start_et.astimezone(
            timezone.utc
        )

        realtime_end_utc = (
            reference_time_utc
            + timedelta(seconds=1)
        )

        fallback_delay_minutes = max(
            15,
            int(
                self.parameters.get(
                    "option_daily_volume_delayed_fallback_minutes",
                    16,
                )
                or 16
            ),
        )

        delayed_end_utc = (
            reference_time_utc
            - timedelta(
                minutes=fallback_delay_minutes
            )
        )

        # Keep multi-symbol requests manageable and consistent
        # with Alpaca's option-data symbol limits.
        chunk_size = 100

        for start in range(
            0,
            len(contract_symbols),
            chunk_size,
        ):

            chunk = [
                str(symbol)
                for symbol in contract_symbols[
                    start:
                    start + chunk_size
                ]
            ]

            data_status = "REALTIME_OPRA"
            data_delay_minutes = 0
            data_asof_utc = realtime_end_utc

            request = OptionBarsRequest(
                symbol_or_symbols=chunk,
                timeframe=TimeFrame.Day,
                start=start_utc,
                end=realtime_end_utc,
            )

            try:
                response = (
                    self.alpaca_option_data_client
                    .get_option_bars(request)
                )

            except Exception as exc:
                message = str(exc).lower()

                opra_entitlement_error = (
                    "opra agreement is not signed"
                    in message
                )

                if not opra_entitlement_error:
                    raise

                if delayed_end_utc <= start_utc:
                    raise

                delayed_request = OptionBarsRequest(
                    symbol_or_symbols=chunk,
                    timeframe=TimeFrame.Day,
                    start=start_utc,
                    end=delayed_end_utc,
                )

                response = (
                    self.alpaca_option_data_client
                    .get_option_bars(delayed_request)
                )

                data_status = (
                    "ACTUAL_OPRA_DELAYED"
                )
                data_delay_minutes = (
                    fallback_delay_minutes
                )
                data_asof_utc = delayed_end_utc

            data = getattr(
                response,
                "data",
                None,
            )

            if data is None:
                data = (
                    response
                    if isinstance(response, dict)
                    else {}
                )

            for symbol in chunk:

                # Even if no bar exists for a symbol, retain the
                # request status so downstream scoring can
                # distinguish a true zero/no-bar from lookup failure.
                activity[symbol][
                    "data_status"
                ] = data_status
                activity[symbol][
                    "data_delay_minutes"
                ] = data_delay_minutes
                activity[symbol][
                    "data_asof_utc"
                ] = data_asof_utc

                bars = data.get(
                    symbol,
                    [],
                )

                if not bars:
                    continue

                bar = bars[-1]

                try:
                    volume = float(
                        getattr(
                            bar,
                            "volume",
                            0,
                        )
                        or 0
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    volume = 0.0

                try:
                    trade_count = float(
                        getattr(
                            bar,
                            "trade_count",
                            0,
                        )
                        or 0
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    trade_count = 0.0

                try:
                    vwap_raw = getattr(
                        bar,
                        "vwap",
                        None,
                    )
                    vwap = (
                        float(vwap_raw)
                        if vwap_raw is not None
                        else None
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    vwap = None

                activity[symbol].update(
                    {
                        "volume": max(
                            0.0,
                            volume,
                        ),
                        "trade_count": max(
                            0.0,
                            trade_count,
                        ),
                        "vwap": vwap,
                        "bar_timestamp": getattr(
                            bar,
                            "timestamp",
                            None,
                        ),
                        "has_bar": True,
                    }
                )

        return activity


    # ======================================================
    # OBSERVED IV HISTORY
    # ======================================================

    def _load_option_iv_history_state(
        self,
    ):

        self._option_iv_history = {}

        path = self.option_iv_history_path

        if not path:
            return

        if not os.path.exists(path):
            return

        try:

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:

                state = json.load(handle)

            raw_series = state.get(
                "series",
                {},
            )

            if not isinstance(
                raw_series,
                dict,
            ):
                raise ValueError(
                    "series must be an object"
                )

            cleaned = {}

            for key, records in (
                raw_series.items()
            ):

                if not isinstance(
                    records,
                    list,
                ):
                    continue

                clean_records = []

                for record in records:

                    if not isinstance(
                        record,
                        dict,
                    ):
                        continue

                    record_date = str(
                        record.get(
                            "date",
                            "",
                        )
                    )

                    try:
                        iv = float(
                            record.get(
                                "iv"
                            )
                        )
                    except (
                        TypeError,
                        ValueError,
                    ):
                        continue

                    if (
                        not record_date
                        or iv <= 0
                    ):
                        continue

                    clean_records.append(
                        {
                            "date": record_date,
                            "iv": iv,
                        }
                    )

                if clean_records:
                    cleaned[str(key)] = (
                        clean_records
                    )

            self._option_iv_history = cleaned

            sample_count = sum(
                len(records)
                for records in cleaned.values()
            )

            if sample_count:
                self.log_message(
                    "Loaded observed option IV history: "
                    f"{sample_count} daily sample(s) "
                    f"across {len(cleaned)} series."
                )

        except Exception as exc:

            self._option_iv_history = {}

            self.log_message(
                "Could not load option IV history; "
                f"starting clean: {exc}"
            )


    def _save_option_iv_history_state(
        self,
    ):

        path = self.option_iv_history_path

        if not path:
            return

        directory = os.path.dirname(
            os.path.abspath(path)
        )

        if directory:
            os.makedirs(
                directory,
                exist_ok=True,
            )

        temporary_path = path + ".tmp"

        state = {
            "version": 1,
            "series": self._option_iv_history,
        }

        try:

            with open(
                temporary_path,
                "w",
                encoding="utf-8",
            ) as handle:

                json.dump(
                    state,
                    handle,
                    indent=2,
                    sort_keys=True,
                )

                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())

            os.replace(
                temporary_path,
                path,
            )

        except Exception as exc:

            try:
                if os.path.exists(
                    temporary_path
                ):
                    os.remove(
                        temporary_path
                    )
            except Exception:
                pass

            self.log_message(
                "Could not save option IV history: "
                f"{exc}"
            )


    @staticmethod
    def _option_iv_history_key(
        underlying,
        contract_type,
    ):

        type_text = getattr(
            contract_type,
            "value",
            contract_type,
        )

        return (
            f"{str(underlying).upper()}|"
            f"{str(type_text).lower()}"
        )


    def _annotate_and_update_option_iv_context(
        self,
        results,
        contract_type,
    ):
        """
        Add empirical IV percentile/rank from prior scanner-observed
        daily samples, then persist today's representative IV.

        The representative sample is the median IV of the shortlisted
        contracts for one underlying and option type. Because the
        scanner already constrains DTE and delta, the history remains
        materially more comparable than mixing the full chain.
        """

        if results.empty:
            return results

        today = str(
            self.get_datetime().date()
        )

        lookback = max(
            1,
            int(
                self.parameters[
                    "option_iv_history_lookback_samples"
                ]
            ),
        )

        minimum_samples = max(
            1,
            int(
                self.parameters[
                    "option_iv_history_min_samples"
                ]
            ),
        )

        percentile_values = []
        rank_values = []
        sample_counts = []
        history_mins = []
        history_maxes = []

        for _, row in results.iterrows():

            key = self._option_iv_history_key(
                row["underlying"],
                contract_type,
            )

            records = (
                self._option_iv_history
                .get(
                    key,
                    [],
                )
            )

            prior_values = []

            for record in records:

                if str(
                    record.get(
                        "date",
                        "",
                    )
                ) == today:
                    continue

                try:
                    value = float(
                        record.get("iv")
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    continue

                if value > 0:
                    prior_values.append(
                        value
                    )

            prior_values = prior_values[
                -lookback:
            ]

            count = len(prior_values)
            sample_counts.append(count)

            if count < minimum_samples:
                percentile_values.append(None)
                rank_values.append(None)
                history_mins.append(None)
                history_maxes.append(None)
                continue

            current_iv = float(
                row["iv"]
            )

            iv_min = min(prior_values)
            iv_max = max(prior_values)

            percentile = (
                sum(
                    value <= current_iv
                    for value in prior_values
                )
                / count
            )

            if iv_max > iv_min:
                iv_rank = (
                    current_iv - iv_min
                ) / (
                    iv_max - iv_min
                )
                iv_rank = max(
                    0.0,
                    min(
                        1.0,
                        iv_rank,
                    ),
                )
            else:
                iv_rank = 0.5

            percentile_values.append(
                percentile
            )
            rank_values.append(
                iv_rank
            )
            history_mins.append(iv_min)
            history_maxes.append(iv_max)

        results = results.copy()

        results["iv_percentile"] = (
            percentile_values
        )
        results["iv_rank"] = rank_values
        results["iv_history_samples"] = (
            sample_counts
        )
        results["iv_history_min"] = history_mins
        results["iv_history_max"] = history_maxes

        # Persist one daily representative value per underlying
        # and option type. Same-day rescans replace the sample.
        for underlying, group in (
            results.groupby("underlying")
        ):

            representative_iv = float(
                pd.to_numeric(
                    group["iv"],
                    errors="coerce",
                )
                .dropna()
                .median()
            )

            if representative_iv <= 0:
                continue

            key = self._option_iv_history_key(
                underlying,
                contract_type,
            )

            records = [
                record
                for record in (
                    self._option_iv_history
                    .get(
                        key,
                        [],
                    )
                )
                if str(
                    record.get(
                        "date",
                        "",
                    )
                ) != today
            ]

            records.append(
                {
                    "date": today,
                    "iv": representative_iv,
                }
            )

            self._option_iv_history[key] = (
                records[-lookback:]
            )

        self._save_option_iv_history_state()

        return results


    # ======================================================
    # EVENT / EARNINGS RISK
    # ======================================================

    @staticmethod
    def _normalize_earnings_calendar_records(
        records,
    ):
        """
        Normalize provider/manual records into:
            {"AAPL": [date(...), ...]}

        Alpha Vantage currently uses camelCase reportDate in its
        CSV calendar. Manual files may use earnings_date/report_date/date.
        """

        calendar = {}

        for record in records or []:

            if not isinstance(
                record,
                dict,
            ):
                continue

            normalized = {
                str(key).strip().lower().replace(
                    "-",
                    "_",
                ): value
                for key, value in record.items()
            }

            symbol = str(
                normalized.get(
                    "symbol",
                    normalized.get(
                        "ticker",
                        "",
                    ),
                )
                or ""
            ).strip().upper()

            raw_date = None

            for field in (
                "earnings_date",
                "earningsdate",
                "report_date",
                "reportdate",
                "date",
            ):

                value = normalized.get(field)

                if value not in (
                    None,
                    "",
                ):
                    raw_date = value
                    break

            if not symbol or raw_date is None:
                continue

            parsed = pd.to_datetime(
                raw_date,
                errors="coerce",
            )

            if pd.isna(parsed):
                continue

            calendar.setdefault(
                symbol,
                [],
            ).append(
                parsed.date()
            )

        for symbol in calendar:
            calendar[symbol] = sorted(
                set(calendar[symbol])
            )

        return calendar


    def _set_earnings_calendar_runtime_state(
        self,
        freshness,
        source,
        cache_age_hours=None,
        reason="",
    ):

        self._event_risk_earnings_freshness = str(
            freshness
            or "UNAVAILABLE"
        ).upper()

        self._event_risk_earnings_source = str(
            source
            or "NONE"
        ).upper()

        self._event_risk_earnings_cache_age_hours = (
            None
            if cache_age_hours is None
            else max(
                0.0,
                float(cache_age_hours),
            )
        )

        self._event_risk_earnings_reason = str(
            reason
            or ""
        )


    def _load_manual_earnings_calendar(
        self,
        path,
    ):

        lower_path = path.lower()
        records = []

        if lower_path.endswith(".csv"):

            frame = pd.read_csv(path)
            records = frame.to_dict(
                orient="records"
            )

        elif lower_path.endswith(".json"):

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:
                payload = json.load(handle)

            if isinstance(payload, list):
                records = payload

            elif isinstance(payload, dict):

                for symbol, raw_dates in (
                    payload.items()
                ):

                    date_values = (
                        raw_dates
                        if isinstance(
                            raw_dates,
                            list,
                        )
                        else [raw_dates]
                    )

                    for raw_date in date_values:
                        records.append(
                            {
                                "symbol": symbol,
                                "earnings_date": raw_date,
                            }
                        )

            else:
                raise ValueError(
                    "JSON must be a list or object"
                )

        else:
            raise ValueError(
                "earnings calendar must be .csv or .json"
            )

        calendar = (
            self._normalize_earnings_calendar_records(
                records
            )
        )

        if not calendar:
            raise ValueError(
                "calendar contained no usable symbol/date rows"
            )

        return calendar


    def _earnings_calendar_now_utc(
        self,
    ):

        now = self.get_datetime()

        if now.tzinfo is None:
            now = now.replace(
                tzinfo=timezone.utc
            )

        return now.astimezone(
            timezone.utc
        )


    def _load_earnings_calendar_cache(
        self,
    ):

        path = self.earnings_calendar_cache_path

        if (
            not path
            or not os.path.exists(path)
        ):
            return None

        try:

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:
                payload = json.load(handle)

            if not isinstance(payload, dict):
                raise ValueError(
                    "cache root must be an object"
                )

            raw_calendar = payload.get(
                "calendar",
                {},
            )

            if not isinstance(
                raw_calendar,
                dict,
            ):
                raise ValueError(
                    "cache calendar must be an object"
                )

            records = []

            for symbol, raw_dates in (
                raw_calendar.items()
            ):

                dates = (
                    raw_dates
                    if isinstance(
                        raw_dates,
                        list,
                    )
                    else [raw_dates]
                )

                for raw_date in dates:
                    records.append(
                        {
                            "symbol": symbol,
                            "earnings_date": raw_date,
                        }
                    )

            calendar = (
                self._normalize_earnings_calendar_records(
                    records
                )
            )

            if not calendar:
                raise ValueError(
                    "cache contained no usable earnings rows"
                )

            fetched_at_raw = payload.get(
                "fetched_at"
            )

            fetched_at = pd.to_datetime(
                fetched_at_raw,
                errors="coerce",
                utc=True,
            )

            if pd.isna(fetched_at):
                raise ValueError(
                    "cache fetched_at is missing/invalid"
                )

            fetched_at_dt = (
                fetched_at.to_pydatetime()
            )

            age_hours = max(
                0.0,
                (
                    self._earnings_calendar_now_utc()
                    - fetched_at_dt
                ).total_seconds()
                / 3600.0,
            )

            return {
                "calendar": calendar,
                "fetched_at": fetched_at_dt,
                "age_hours": age_hours,
                "provider": str(
                    payload.get(
                        "provider",
                        "UNKNOWN",
                    )
                ).upper(),
                "horizon": str(
                    payload.get(
                        "horizon",
                        "",
                    )
                ).lower(),
            }

        except Exception as exc:

            self.log_message(
                "Ignoring unusable earnings cache at "
                f"{path}: {exc}"
            )

            return None


    def _save_earnings_calendar_cache(
        self,
        calendar,
        provider,
    ):

        path = self.earnings_calendar_cache_path

        if not path:
            return

        directory = os.path.dirname(
            os.path.abspath(path)
        )

        if directory:
            os.makedirs(
                directory,
                exist_ok=True,
            )

        payload = {
            "schema_version": 1,
            "provider": str(provider).upper(),
            "horizon": self.earnings_calendar_horizon,
            "fetched_at": (
                self._earnings_calendar_now_utc()
                .isoformat()
            ),
            "calendar": {
                symbol: [
                    event_date.isoformat()
                    for event_date in dates
                ]
                for symbol, dates in sorted(
                    calendar.items()
                )
            },
        }

        temporary_path = path + ".tmp"

        with open(
            temporary_path,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                payload,
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")

        os.replace(
            temporary_path,
            path,
        )


    def _fetch_alphavantage_earnings_calendar(
        self,
    ):

        if not self.alphavantage_api_key:
            raise ValueError(
                "ALPHAVANTAGE_API_KEY is not set"
            )

        query = urllib.parse.urlencode(
            {
                "function": "EARNINGS_CALENDAR",
                "horizon": self.earnings_calendar_horizon,
                "apikey": self.alphavantage_api_key,
            }
        )

        url = (
            "https://www.alphavantage.co/query?"
            + query
        )

        # Alpha Vantage's official example performs a normal GET
        # and the earnings-calendar endpoint may advertise a
        # download-oriented content type rather than text/csv.
        # Use a wildcard Accept header so content negotiation does
        # not reject a perfectly valid CSV payload with HTTP 406.
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "*/*",
                "User-Agent": (
                    "LumiBot-StockSuggestionStrategy/1.0"
                ),
            },
        )

        timeout_seconds = max(
            1.0,
            float(
                self.parameters.get(
                    "earnings_calendar_request_timeout_seconds",
                    15.0,
                )
            ),
        )

        try:

            with urllib.request.urlopen(
                request,
                timeout=timeout_seconds,
            ) as response:
                raw = response.read()

        except urllib.error.HTTPError as exc:

            if int(getattr(exc, "code", 0) or 0) == 406:
                self.log_message(
                    "Alpha Vantage earnings request returned HTTP 406; "
                    "retrying once with official-style default content "
                    "negotiation."
                )

                # Compatibility retry: match the provider's sample as
                # closely as urllib allows by omitting custom headers.
                # This remains the same endpoint/key/horizon and does
                # not weaken cache or fail-closed behavior.
                compatibility_request = (
                    urllib.request.Request(url)
                )

                try:
                    with urllib.request.urlopen(
                        compatibility_request,
                        timeout=timeout_seconds,
                    ) as response:
                        raw = response.read()

                except urllib.error.HTTPError as retry_exc:
                    raise RuntimeError(
                        "Alpha Vantage earnings request returned "
                        f"HTTP {retry_exc.code} after compatibility retry"
                    ) from retry_exc

                except urllib.error.URLError as retry_exc:
                    raise RuntimeError(
                        "Alpha Vantage earnings compatibility retry failed: "
                        f"{retry_exc.reason}"
                    ) from retry_exc

            else:
                raise RuntimeError(
                    "Alpha Vantage earnings request returned "
                    f"HTTP {exc.code}"
                ) from exc

        except urllib.error.URLError as exc:
            raise RuntimeError(
                "Alpha Vantage earnings request failed: "
                f"{exc.reason}"
            ) from exc

        text = raw.decode(
            "utf-8-sig",
            errors="replace",
        ).strip()

        if not text:
            raise ValueError(
                "Alpha Vantage returned an empty earnings response"
            )

        if text.startswith("{"):

            try:
                error_payload = json.loads(text)
            except Exception:
                error_payload = {}

            message = ""

            if isinstance(
                error_payload,
                dict,
            ):
                for key in (
                    "Error Message",
                    "Information",
                    "Note",
                    "message",
                ):
                    if error_payload.get(key):
                        message = str(
                            error_payload[key]
                        )
                        break

            raise RuntimeError(
                "Alpha Vantage did not return an earnings CSV"
                + (
                    f": {message}"
                    if message
                    else ""
                )
            )

        reader = csv.DictReader(
            io.StringIO(text)
        )

        if not reader.fieldnames:
            raise ValueError(
                "Alpha Vantage earnings CSV has no header"
            )

        records = list(reader)

        calendar = (
            self._normalize_earnings_calendar_records(
                records
            )
        )

        # The unfiltered full 3/6/12-month calendar should contain
        # many symbols. A tiny response is more likely an upstream
        # error/format change than a trustworthy market-wide calendar.
        if len(calendar) < 10:
            raise ValueError(
                "Alpha Vantage earnings calendar was suspiciously "
                f"small ({len(calendar)} symbols)"
            )

        return calendar


    def _load_earnings_calendar(
        self,
    ):
        """
        Load earnings dates using this precedence:

        1. Explicit EARNINGS_CALENDAR_PATH manual override.
        2. Fresh persistent provider cache.
        3. Automatic provider refresh (Alpha Vantage by default).
        4. Recent stale cache for diagnostics/known-date blocking.
        5. UNAVAILABLE, which fails closed by default in contract risk.
        """

        self._set_earnings_calendar_runtime_state(
            "UNAVAILABLE",
            "NONE",
            reason="calendar has not been resolved",
        )

        manual_path = self.earnings_calendar_path

        if manual_path:

            if not os.path.exists(manual_path):
                self.log_message(
                    "Earnings manual override does not exist: "
                    f"{manual_path}. Falling back to automatic provider/cache."
                )

            else:

                try:

                    calendar = (
                        self._load_manual_earnings_calendar(
                            manual_path
                        )
                    )

                    self._set_earnings_calendar_runtime_state(
                        "FRESH",
                        "MANUAL_PATH",
                        reason=(
                            "explicit EARNINGS_CALENDAR_PATH override"
                        ),
                    )

                    self.log_message(
                        "Loaded manual earnings calendar override: "
                        f"{sum(len(v) for v in calendar.values())} "
                        f"date(s) across {len(calendar)} symbol(s)."
                    )

                    return calendar, True

                except Exception as exc:

                    self.log_message(
                        "Could not load manual earnings calendar; "
                        "falling back to automatic provider/cache: "
                        f"{exc}"
                    )

        cache = self._load_earnings_calendar_cache()

        refresh_hours = max(
            0.0,
            float(
                self.parameters.get(
                    "earnings_calendar_refresh_hours",
                    6.0,
                )
            ),
        )

        max_stale_hours = max(
            refresh_hours,
            float(
                self.parameters.get(
                    "earnings_calendar_max_stale_hours",
                    72.0,
                )
            ),
        )

        if (
            cache is not None
            and cache.get("age_hours", float("inf"))
            <= refresh_hours
            and cache.get("horizon")
            == self.earnings_calendar_horizon
        ):

            age_hours = float(
                cache["age_hours"]
            )

            self._set_earnings_calendar_runtime_state(
                "FRESH",
                "PROVIDER_CACHE",
                cache_age_hours=age_hours,
                reason="cache is inside refresh TTL",
            )

            self.log_message(
                "Using fresh earnings calendar cache: "
                f"age={age_hours:.2f}h, "
                f"provider={cache.get('provider', 'UNKNOWN')}, "
                f"symbols={len(cache['calendar'])}."
            )

            return cache["calendar"], True

        provider = str(
            self.earnings_calendar_provider
            or "NONE"
        ).upper()

        refresh_error = None

        if provider == "ALPHAVANTAGE":

            try:

                calendar = (
                    self._fetch_alphavantage_earnings_calendar()
                )

                self._save_earnings_calendar_cache(
                    calendar,
                    provider="ALPHAVANTAGE",
                )

                self._set_earnings_calendar_runtime_state(
                    "FRESH",
                    "ALPHAVANTAGE_LIVE",
                    cache_age_hours=0.0,
                    reason="automatic provider refresh succeeded",
                )

                self.log_message(
                    "Refreshed automatic earnings calendar from "
                    "Alpha Vantage: "
                    f"{sum(len(v) for v in calendar.values())} "
                    f"date(s) across {len(calendar)} symbol(s), "
                    f"horizon={self.earnings_calendar_horizon}."
                )

                return calendar, True

            except Exception as exc:
                refresh_error = str(exc)

        elif provider in {
            "",
            "NONE",
            "OFF",
            "DISABLED",
        }:
            refresh_error = (
                "automatic earnings provider is disabled"
            )

        else:
            refresh_error = (
                "unsupported EARNINGS_CALENDAR_PROVIDER="
                f"{provider}"
            )

        if cache is not None:

            age_hours = float(
                cache.get(
                    "age_hours",
                    float("inf"),
                )
            )

            if age_hours <= max_stale_hours:

                self._set_earnings_calendar_runtime_state(
                    "STALE",
                    "PROVIDER_CACHE",
                    cache_age_hours=age_hours,
                    reason=(
                        refresh_error
                        or "automatic refresh did not succeed"
                    ),
                )

                self.log_message(
                    "Automatic earnings refresh failed; using "
                    "STALE cache for known-date diagnostics only: "
                    f"age={age_hours:.2f}h, "
                    f"max_stale={max_stale_hours:.2f}h, "
                    f"reason={refresh_error}."
                )

                return cache["calendar"], True

            refresh_error = (
                f"{refresh_error}; cached calendar age "
                f"{age_hours:.2f}h exceeds "
                f"{max_stale_hours:.2f}h maximum"
            )

        self._set_earnings_calendar_runtime_state(
            "UNAVAILABLE",
            "NONE",
            reason=(
                refresh_error
                or "no manual/provider/cache earnings calendar"
            ),
        )

        self.log_message(
            "Earnings calendar is UNAVAILABLE: "
            f"{self._event_risk_earnings_reason}."
        )

        return {}, False


    @staticmethod
    def _corporate_action_symbols(
        action,
    ):

        symbols = set()

        for field in (
            "symbol",
            "old_symbol",
            "source_symbol",
            "acquiree_symbol",
            "acquirer_symbol",
            "new_symbol",
        ):

            value = getattr(
                action,
                field,
                None,
            )

            if value:
                symbols.add(
                    str(value).upper()
                )

        return symbols


    @staticmethod
    def _corporate_action_event_date(
        category,
        action,
    ):

        if category in (
            "forward_splits",
            "reverse_splits",
            "stock_dividends",
            "cash_dividends",
            "spin_offs",
            "rights_distributions",
        ):
            fields = (
                "ex_date",
                "effective_date",
                "payable_date",
                "process_date",
            )

        elif category in (
            "unit_splits",
            "cash_mergers",
            "stock_mergers",
            "stock_and_cash_mergers",
        ):
            fields = (
                "effective_date",
                "payable_date",
                "process_date",
            )

        else:
            fields = (
                "payable_date",
                "effective_date",
                "ex_date",
                "process_date",
            )

        for field in fields:

            value = getattr(
                action,
                field,
                None,
            )

            if isinstance(value, date):
                return value

            if value:
                parsed = pd.to_datetime(
                    value,
                    errors="coerce",
                )
                if not pd.isna(parsed):
                    return parsed.date()

        return None


    def _refresh_event_risk_context(
        self,
        symbols,
    ):

        symbols = sorted(
            {
                str(symbol).upper()
                for symbol in symbols
                if str(symbol).strip()
            }
        )

        self._event_risk_by_symbol = {
            symbol: []
            for symbol in symbols
        }

        self._event_risk_corporate_actions_available = False
        self._event_risk_earnings_available = False
        self._event_risk_earnings_freshness = "UNAVAILABLE"
        self._event_risk_earnings_source = "NONE"
        self._event_risk_earnings_cache_age_hours = None
        self._event_risk_earnings_reason = "not refreshed in this scan"

        if not symbols:
            return

        today = self.get_datetime().date()
        lookahead_end = (
            today
            + timedelta(
                days=int(
                    self.parameters[
                        "event_risk_lookahead_days"
                    ]
                )
            )
        )

        history_start = (
            today
            - timedelta(
                days=int(
                    self.parameters[
                        "event_risk_history_days"
                    ]
                )
            )
        )

        hard_block_categories = {
            "forward_splits",
            "reverse_splits",
            "unit_splits",
            "stock_dividends",
            "spin_offs",
            "cash_mergers",
            "stock_mergers",
            "stock_and_cash_mergers",
            "redemptions",
            "name_changes",
            "worthless_removals",
            "rights_distributions",
        }

        try:

            request = CorporateActionsRequest(
                symbols=symbols,
                start=history_start,
                end=lookahead_end,
                limit=1000,
            )

            response = (
                self.alpaca_corporate_actions_client
                .get_corporate_actions(request)
            )

            data = getattr(
                response,
                "data",
                None,
            )

            if data is None:
                data = (
                    response
                    if isinstance(response, dict)
                    else {}
                )

            event_count = 0

            for category, actions in data.items():

                category = str(category)

                for action in actions or []:

                    event_date = (
                        self._corporate_action_event_date(
                            category,
                            action,
                        )
                    )

                    if (
                        event_date is None
                        or event_date < today
                        or event_date > lookahead_end
                    ):
                        continue

                    related_symbols = (
                        self._corporate_action_symbols(
                            action
                        )
                    )

                    for symbol in (
                        related_symbols
                        & set(symbols)
                    ):

                        hard_block = (
                            category
                            in hard_block_categories
                        )

                        score_penalty = (
                            float(
                                self.parameters[
                                    "event_risk_cash_dividend_score_penalty"
                                ]
                            )
                            if category
                            == "cash_dividends"
                            else 0.0
                        )

                        self._event_risk_by_symbol[
                            symbol
                        ].append(
                            {
                                "kind": "corporate_action",
                                "category": category,
                                "date": event_date,
                                "hard_block": hard_block,
                                "score_penalty": score_penalty,
                            }
                        )

                        event_count += 1

            self._event_risk_corporate_actions_available = True

            self.log_message(
                "Corporate-action event scan: "
                f"{event_count} upcoming relevant event(s) "
                f"for {len(symbols)} finalist symbol(s)."
            )

        except Exception as exc:

            self.log_message(
                "Corporate-action event scan unavailable; "
                f"event risk is partially UNKNOWN: {exc}"
            )

        earnings_calendar, earnings_available = (
            self._load_earnings_calendar()
        )

        self._event_risk_earnings_available = (
            earnings_available
        )

        earnings_count = 0

        if earnings_available:

            for symbol in symbols:

                for earnings_date in (
                    earnings_calendar.get(
                        symbol,
                        [],
                    )
                ):

                    if (
                        earnings_date < today
                        or earnings_date > lookahead_end
                    ):
                        continue

                    self._event_risk_by_symbol[
                        symbol
                    ].append(
                        {
                            "kind": "earnings",
                            "category": "earnings",
                            "date": earnings_date,
                            "hard_block": bool(
                                self.parameters[
                                    "event_risk_block_earnings_before_expiration"
                                ]
                            ),
                            "score_penalty": 0.0,
                        }
                    )

                    earnings_count += 1

        freshness = str(
            self._event_risk_earnings_freshness
            or "UNAVAILABLE"
        ).upper()

        source = str(
            self._event_risk_earnings_source
            or "NONE"
        ).upper()

        age = (
            self._event_risk_earnings_cache_age_hours
        )

        age_text = (
            "n/a"
            if age is None
            else f"{float(age):.2f}h"
        )

        fail_closed = bool(
            self.parameters.get(
                "earnings_calendar_fail_closed",
                True,
            )
        )

        self.log_message(
            "Earnings event scan: "
            f"{earnings_count} upcoming date(s) inside the "
            "configured horizon. "
            f"freshness={freshness}, source={source}, "
            f"cache_age={age_text}."
        )

        if freshness in {
            "STALE",
            "UNAVAILABLE",
        }:

            self.log_message(
                "Earnings calendar is not fresh. "
                f"Reason={self._event_risk_earnings_reason}. "
                + (
                    "New option entries will FAIL CLOSED on "
                    "earnings uncertainty."
                    if fail_closed
                    else (
                        "Fail-closed guard is disabled; entries may "
                        "continue with explicit earnings uncertainty."
                    )
                )
            )


    def _evaluate_contract_event_risk(
        self,
        underlying,
        expiration,
    ):

        symbol = str(
            underlying
        ).upper()

        today = self.get_datetime().date()

        if isinstance(expiration, datetime):
            expiration = expiration.date()

        relevant = []

        for event in (
            self._event_risk_by_symbol
            .get(
                symbol,
                [],
            )
        ):

            event_date = event.get(
                "date"
            )

            if (
                isinstance(event_date, date)
                and today <= event_date <= expiration
            ):
                relevant.append(event)

        hard_blocks = [
            event
            for event in relevant
            if event.get(
                "hard_block",
                False,
            )
        ]

        score_penalty = sum(
            float(
                event.get(
                    "score_penalty",
                    0.0,
                )
                or 0.0
            )
            for event in relevant
            if not event.get(
                "hard_block",
                False,
            )
        )

        labels = []
        earnings_date = None

        for event in sorted(
            relevant,
            key=lambda item: (
                item.get("date"),
                item.get("category", ""),
            ),
        ):

            category = str(
                event.get(
                    "category",
                    "event",
                )
            ).upper()

            event_date = event.get(
                "date"
            )

            labels.append(
                f"{category}@{event_date}"
            )

            if (
                event.get("kind")
                == "earnings"
                and earnings_date is None
            ):
                earnings_date = event_date

        earnings_freshness = str(
            getattr(
                self,
                "_event_risk_earnings_freshness",
                "UNAVAILABLE",
            )
            or "UNAVAILABLE"
        ).upper()

        earnings_fail_closed = bool(
            self.parameters.get(
                "earnings_calendar_fail_closed",
                True,
            )
        )

        earnings_uncertainty_block = (
            earnings_fail_closed
            and earnings_freshness in {
                "STALE",
                "UNAVAILABLE",
            }
        )

        if earnings_freshness == "STALE":
            labels.append(
                "EARNINGS_STALE"
            )

        elif earnings_freshness == "UNAVAILABLE":
            labels.append(
                "EARNINGS_UNKNOWN"
            )

        if not self._event_risk_corporate_actions_available:
            labels.append(
                "CORPORATE_ACTIONS_UNKNOWN"
            )

        label = (
            ";".join(labels)
            if labels
            else "NONE"
        )

        if earnings_date is not None:
            earnings_status = "BEFORE_EXPIRATION"
        elif earnings_freshness == "STALE":
            earnings_status = "STALE"
        elif earnings_freshness == "UNAVAILABLE":
            earnings_status = "UNKNOWN"
        else:
            earnings_status = "CLEAR"

        return {
            "blocked": (
                bool(hard_blocks)
                or earnings_uncertainty_block
            ),
            "score_penalty": score_penalty,
            "label": label,
            "earnings_date": earnings_date,
            "earnings_status": earnings_status,
            "earnings_freshness": earnings_freshness,
            "earnings_source": getattr(
                self,
                "_event_risk_earnings_source",
                "NONE",
            ),
            "earnings_cache_age_hours": getattr(
                self,
                "_event_risk_earnings_cache_age_hours",
                None,
            ),
        }


        # ======================================================
    # OPTION SNAPSHOT HELPER
    # ======================================================

    def _get_option_snapshots(
        self,
        contract_symbols,
    ):

        snapshots = {}

        # Alpaca's option snapshot endpoint accepts
        # at most 100 contract symbols at once.
        chunk_size = 100

        for start in range(
            0,
            len(contract_symbols),
            chunk_size,
        ):

            chunk = contract_symbols[
                start:
                start + chunk_size
            ]

            request = OptionSnapshotRequest(
                symbol_or_symbols=chunk,
                feed=self.alpaca_options_feed,
            )

            response = (
                self.alpaca_option_data_client
                .get_option_snapshot(request)
            )

            if response:

                snapshots.update(
                    response
                )

        return snapshots


    # ======================================================
    # OPTION CONTRACT RANKING
    # ======================================================

    def rank_option_candidates(
        self,
        stock_candidates,
        contract_type,
    ):

        if stock_candidates.empty:

            return pd.DataFrame()

        today = self.get_datetime().date()

        min_expiration = (
            today
            + timedelta(
                days=self.parameters[
                    "option_min_dte"
                ]
            )
        )

        max_expiration = (
            today
            + timedelta(
                days=self.parameters[
                    "option_max_dte"
                ]
            )
        )

        all_rows = []

        # --------------------------------------------------
        # PROCESS EACH STOCK FINALIST
        # --------------------------------------------------

        for _, stock in (
            stock_candidates.iterrows()
        ):

            underlying = str(
                stock["symbol"]
            )

            underlying_price = float(
                stock["price"]
            )

            stock_score = float(
                stock["score"]
            )

            strike_band = (
                self.parameters[
                    "option_strike_band_pct"
                ]
            )

            minimum_strike = max(
                0.01,
                underlying_price
                * (1 - strike_band),
            )

            maximum_strike = (
                underlying_price
                * (1 + strike_band)
            )

            # --------------------------------------------------
            # GET LISTED OPTION CONTRACTS
            # --------------------------------------------------

            try:

                contract_request = (
                    GetOptionContractsRequest(
                        underlying_symbols=[
                            underlying
                        ],
                        status=AssetStatus.ACTIVE,
                        expiration_date_gte=(
                            min_expiration
                        ),
                        expiration_date_lte=(
                            max_expiration
                        ),
                        type=contract_type,
                        style=(
                            ExerciseStyle.AMERICAN
                        ),
                        strike_price_gte=(
                            f"{minimum_strike:.2f}"
                        ),
                        strike_price_lte=(
                            f"{maximum_strike:.2f}"
                        ),
                        limit=10000,
                    )
                )

                response = (
                    self.alpaca_trading_client
                    .get_option_contracts(
                        contract_request
                    )
                )

                contracts = (
                    response.option_contracts
                    or []
                )

            except Exception as exc:

                self.log_message(
                    f"{underlying}: option "
                    f"contract lookup failed: "
                    f"{exc}"
                )

                continue

            # --------------------------------------------------
            # STATIC ELIGIBILITY
            # --------------------------------------------------

            eligible_contracts = []

            event_block_count = 0

            for contract in contracts:

                if not contract.tradable:
                    continue

                if (
                    contract.type
                    != contract_type
                ):
                    continue

                # ------------------------------------------
                # STANDARD 100-SHARE CONTRACTS ONLY
                # ------------------------------------------

                try:

                    contract_size = int(
                        float(
                            contract.size
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue

                if contract_size != 100:
                    continue

                # ------------------------------------------
                # DTE
                # ------------------------------------------

                dte = (
                    contract.expiration_date
                    - today
                ).days

                if (
                    dte
                    < self.parameters[
                        "option_min_dte"
                    ]
                ):
                    continue

                if (
                    dte
                    > self.parameters[
                        "option_max_dte"
                    ]
                ):
                    continue

                # ------------------------------------------
                # EVENT / EARNINGS RISK
                # ------------------------------------------

                event_risk = (
                    self._evaluate_contract_event_risk(
                        underlying,
                        contract.expiration_date,
                    )
                )

                if event_risk[
                    "blocked"
                ]:

                    event_block_count += 1
                    continue

                # ------------------------------------------
                # OPEN INTEREST
                # ------------------------------------------

                try:

                    open_interest = int(
                        float(
                            contract.open_interest
                            or 0
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    open_interest = 0

                if (
                    open_interest
                    < self.parameters[
                        "option_min_open_interest"
                    ]
                ):

                    continue

                eligible_contracts.append(
                    {
                        "contract": contract,
                        "dte": dte,
                        "open_interest":
                            open_interest,
                        "contract_size":
                            contract_size,
                        "event_risk":
                            event_risk,
                    }
                )

            if not eligible_contracts:

                event_note = (
                    ""
                    if event_block_count == 0
                    else (
                        f" Hard-blocked {event_block_count} "
                        "contract(s) for event risk."
                    )
                )

                self.log_message(
                    f"{underlying}: no "
                    f"{contract_type.value} "
                    "contracts passed static "
                    "options filters."
                    + event_note
                )

                continue

            # --------------------------------------------------
            # GET QUOTES + IV + GREEKS
            # --------------------------------------------------

            contract_symbols = [
                item["contract"].symbol
                for item
                in eligible_contracts
            ]

            try:

                snapshots = (
                    self._get_option_snapshots(
                        contract_symbols
                    )
                )

            except Exception as exc:

                self.log_message(
                    f"{underlying}: option "
                    f"snapshot lookup failed: "
                    f"{exc}"
                )

                continue

            # --------------------------------------------------
            # GET CURRENT-SESSION OPTION VOLUME
            # --------------------------------------------------

            daily_activity_lookup_failed = False

            try:

                daily_activity = (
                    self._get_option_daily_activity(
                        contract_symbols
                    )
                )

            except Exception as exc:

                daily_activity_lookup_failed = True

                self.log_message(
                    f"{underlying}: option daily "
                    f"activity lookup failed: {exc}"
                )

                daily_activity = {
                    str(symbol): {
                        "volume": 0.0,
                        "trade_count": 0.0,
                        "vwap": None,
                        "bar_timestamp": None,
                        "has_bar": False,
                    }
                    for symbol in contract_symbols
                }

            stock_option_rows = []

            stale_quote_count = 0
            daily_volume_filter_count = 0

            # --------------------------------------------------
            # MARKET-DATA ELIGIBILITY
            # --------------------------------------------------

            for item in eligible_contracts:

                contract = item[
                    "contract"
                ]

                activity = (
                    daily_activity.get(
                        contract.symbol,
                        {},
                    )
                )

                try:
                    daily_volume = float(
                        activity.get(
                            "volume",
                            0.0,
                        )
                        or 0.0
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    daily_volume = 0.0

                try:
                    daily_trade_count = float(
                        activity.get(
                            "trade_count",
                            0.0,
                        )
                        or 0.0
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    daily_trade_count = 0.0

                if (
                    daily_volume
                    < float(
                        self.parameters[
                            "option_min_daily_volume"
                        ]
                    )
                ):

                    daily_volume_filter_count += 1
                    continue

                snapshot = snapshots.get(
                    contract.symbol
                )

                if snapshot is None:
                    continue

                quote = getattr(
                    snapshot,
                    "latest_quote",
                    None,
                )

                greeks = getattr(
                    snapshot,
                    "greeks",
                    None,
                )

                if (
                    quote is None
                    or greeks is None
                ):

                    continue

                (
                    quote_is_fresh,
                    quote_age_seconds,
                ) = self._is_option_quote_fresh(
                    quote
                )

                if not quote_is_fresh:

                    stale_quote_count += 1

                    continue

                # ------------------------------------------
                # BID / ASK
                # ------------------------------------------

                try:

                    bid = float(
                        quote.bid_price
                        or 0
                    )

                    ask = float(
                        quote.ask_price
                        or 0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue

                if (
                    bid <= 0
                    or ask <= 0
                    or ask < bid
                ):

                    continue

                midpoint = (
                    bid + ask
                ) / 2

                if (
                    midpoint
                    < self.parameters[
                        "option_min_mid_price"
                    ]
                ):

                    continue

                spread = ask - bid

                spread_pct = (
                    spread / midpoint
                    if midpoint > 0
                    else float("inf")
                )

                if (
                    spread_pct
                    > self.parameters[
                        "option_max_spread_pct"
                    ]
                ):

                    continue

                # ------------------------------------------
                # DELTA
                # ------------------------------------------

                delta_raw = getattr(
                    greeks,
                    "delta",
                    None,
                )

                if delta_raw is None:
                    continue

                try:

                    delta = float(
                        delta_raw
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue

                abs_delta = abs(
                    delta
                )

                if (
                    abs_delta
                    < self.parameters[
                        "option_min_abs_delta"
                    ]
                ):

                    continue

                if (
                    abs_delta
                    > self.parameters[
                        "option_max_abs_delta"
                    ]
                ):

                    continue

                # ------------------------------------------
                # IMPLIED VOLATILITY
                # ------------------------------------------

                iv_raw = getattr(
                    snapshot,
                    "implied_volatility",
                    None,
                )

                if iv_raw is None:
                    continue

                try:

                    iv = float(
                        iv_raw
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue

                if iv <= 0:
                    continue

                # ------------------------------------------
                # QUOTE SIZE
                # ------------------------------------------

                try:

                    bid_size = float(
                        quote.bid_size
                        or 0
                    )

                    ask_size = float(
                        quote.ask_size
                        or 0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    bid_size = 0
                    ask_size = 0

                two_sided_quote_size = min(
                    bid_size,
                    ask_size,
                )

                open_interest = item[
                    "open_interest"
                ]

                dte = item[
                    "dte"
                ]

                # ==========================================
                # OPTION QUALITY SCORE
                # ==========================================

                # ------------------------------------------
                # SPREAD SCORE
                # ------------------------------------------

                spread_score = max(
                    0.0,
                    1.0
                    - (
                        spread_pct
                        / self.parameters[
                            "option_max_spread_pct"
                        ]
                    ),
                )

                # ------------------------------------------
                # DELTA SCORE
                # ------------------------------------------

                target_delta = (
                    self.parameters[
                        "option_target_abs_delta"
                    ]
                )

                delta_distance = max(
                    target_delta
                    - self.parameters[
                        "option_min_abs_delta"
                    ],
                    self.parameters[
                        "option_max_abs_delta"
                    ]
                    - target_delta,
                    0.01,
                )

                delta_score = max(
                    0.0,
                    1.0
                    - (
                        abs(
                            abs_delta
                            - target_delta
                        )
                        / delta_distance
                    ),
                )

                # ------------------------------------------
                # OPEN INTEREST SCORE
                # ------------------------------------------

                oi_full_score = max(
                    1,
                    self.parameters[
                        "option_open_interest_full_score"
                    ],
                )

                oi_score = min(
                    1.0,
                    math.log1p(
                        open_interest
                    )
                    / math.log1p(
                        oi_full_score
                    ),
                )

                # ------------------------------------------
                # DTE SCORE
                # ------------------------------------------

                target_dte = (
                    self.parameters[
                        "option_target_dte"
                    ]
                )

                dte_distance = max(
                    target_dte
                    - self.parameters[
                        "option_min_dte"
                    ],
                    self.parameters[
                        "option_max_dte"
                    ]
                    - target_dte,
                    1,
                )

                dte_score = max(
                    0.0,
                    1.0
                    - (
                        abs(
                            dte
                            - target_dte
                        )
                        / dte_distance
                    ),
                )

                # ------------------------------------------
                # QUOTE SIZE SCORE
                # ------------------------------------------

                quote_full_score = max(
                    1,
                    self.parameters[
                        "option_quote_size_full_score"
                    ],
                )

                quote_size_score = min(
                    1.0,
                    math.log1p(
                        max(
                            two_sided_quote_size,
                            0,
                        )
                    )
                    / math.log1p(
                        quote_full_score
                    ),
                )

                # ------------------------------------------
                # ACTUAL DAILY VOLUME SCORE
                # ------------------------------------------

                volume_full_score = max(
                    1.0,
                    float(
                        self.parameters[
                            "option_daily_volume_full_score"
                        ]
                    ),
                )

                daily_volume_score = min(
                    1.0,
                    math.log1p(
                        max(
                            daily_volume,
                            0.0,
                        )
                    )
                    / math.log1p(
                        volume_full_score
                    ),
                )

                # ------------------------------------------
                # FINAL OPTION SCORE
                # ------------------------------------------

                non_volume_weight = (
                    self.parameters[
                        "option_score_spread_weight"
                    ]
                    + self.parameters[
                        "option_score_delta_weight"
                    ]
                    + self.parameters[
                        "option_score_open_interest_weight"
                    ]
                    + self.parameters[
                        "option_score_dte_weight"
                    ]
                    + self.parameters[
                        "option_score_quote_size_weight"
                    ]
                )

                non_volume_score = (
                    spread_score
                    * self.parameters[
                        "option_score_spread_weight"
                    ]
                    + delta_score
                    * self.parameters[
                        "option_score_delta_weight"
                    ]
                    + oi_score
                    * self.parameters[
                        "option_score_open_interest_weight"
                    ]
                    + dte_score
                    * self.parameters[
                        "option_score_dte_weight"
                    ]
                    + quote_size_score
                    * self.parameters[
                        "option_score_quote_size_weight"
                    ]
                )

                if daily_activity_lookup_failed:
                    # Entitlement/API failure means "unknown", not
                    # literal zero traded volume. Renormalize across
                    # the remaining available quality components so
                    # data-access failure does not create a fake 15-
                    # point liquidity penalty.
                    option_score = 100 * (
                        non_volume_score
                        / max(
                            non_volume_weight,
                            1e-9,
                        )
                    )
                else:
                    option_score = 100 * (
                        non_volume_score
                        + daily_volume_score
                        * self.parameters[
                            "option_score_daily_volume_weight"
                        ]
                    )

                event_risk = item[
                    "event_risk"
                ]

                option_score = max(
                    0.0,
                    option_score
                    - float(
                        event_risk[
                            "score_penalty"
                        ]
                    ),
                )

                estimated_ask_cost = (
                    ask
                    * item[
                        "contract_size"
                    ]
                )

                stock_option_rows.append(
                    {
                        "underlying":
                            underlying,

                        "underlying_price":
                            underlying_price,

                        "stock_score":
                            stock_score,

                        "contract":
                            contract.symbol,

                        "type":
                            contract_type.value,

                        "expiration":
                            contract.expiration_date,

                        "dte":
                            dte,

                        "strike":
                            float(
                                contract.strike_price
                            ),

                        "bid":
                            bid,

                        "ask":
                            ask,

                        "mid":
                            midpoint,

                        "spread_pct":
                            spread_pct,

                        "open_interest":
                            open_interest,

                        "delta":
                            delta,

                        "iv":
                            iv,

                        "quote_age_seconds":
                            quote_age_seconds,

                        "quote_size":
                            two_sided_quote_size,

                        "daily_volume":
                            daily_volume,

                        "daily_trade_count":
                            daily_trade_count,

                        "daily_bar_timestamp":
                            activity.get(
                                "bar_timestamp"
                            ),

                        "daily_bar_available":
                            bool(
                                activity.get(
                                    "has_bar",
                                    False,
                                )
                            ),

                        "daily_activity_status":
                            activity.get(
                                "data_status",
                                "UNKNOWN",
                            ),

                        "daily_activity_delay_minutes":
                            activity.get(
                                "data_delay_minutes"
                            ),

                        "event_risk":
                            event_risk[
                                "label"
                            ],

                        "event_score_penalty":
                            float(
                                event_risk[
                                    "score_penalty"
                                ]
                            ),

                        "earnings_date":
                            event_risk[
                                "earnings_date"
                            ],

                        "earnings_status":
                            event_risk[
                                "earnings_status"
                            ],

                        "estimated_ask_cost":
                            estimated_ask_cost,

                        "option_score":
                            option_score,
                    }
                )

            # --------------------------------------------------
            # KEEP BEST CONTRACTS FOR THIS STOCK
            # --------------------------------------------------

            if stock_option_rows:

                stock_options = (
                    pd.DataFrame(
                        stock_option_rows
                    )
                    .sort_values(
                        [
                            "option_score",
                            "daily_volume",
                            "open_interest",
                        ],
                        ascending=[
                            False,
                            False,
                            False,
                        ],
                    )
                    .head(
                        self.parameters[
                            "option_top_contracts_per_stock"
                        ]
                    )
                )

                all_rows.extend(
                    stock_options
                    .to_dict(
                        orient="records"
                    )
                )

                freshness_note = (
                    ""
                    if stale_quote_count == 0
                    else (
                        f" "
                        f"Rejected {stale_quote_count} "
                        "stale/missing-timestamp "
                        "quote(s)."
                    )
                )

                daily_bar_count = sum(
                    bool(
                        daily_activity.get(
                            symbol,
                            {},
                        ).get(
                            "has_bar",
                            False,
                        )
                    )
                    for symbol in contract_symbols
                )

                activity_statuses = {
                    str(
                        daily_activity.get(
                            symbol,
                            {},
                        ).get(
                            "data_status",
                            "UNKNOWN",
                        )
                    )
                    for symbol in contract_symbols
                }

                delayed_activity = (
                    "ACTUAL_OPRA_DELAYED"
                    in activity_statuses
                )

                activity_note = (
                    " Option daily activity lookup FAILED; "
                    "volume is UNKNOWN and omitted from score weighting."
                    if daily_activity_lookup_failed
                    else (
                        f" Current-session option bars="
                        f"{daily_bar_count}/"
                        f"{len(contract_symbols)}."
                        + (
                            " Actual OPRA volume is delayed "
                            f"~{self.parameters.get('option_daily_volume_delayed_fallback_minutes', 16)}m "
                            "because current OPRA history is not entitled."
                            if delayed_activity
                            else " Real-time OPRA volume."
                        )
                    )
                )

                volume_filter_note = (
                    ""
                    if daily_volume_filter_count == 0
                    else (
                        f" Rejected {daily_volume_filter_count} "
                        "contract(s) below the daily-volume floor."
                    )
                )

                event_note = (
                    ""
                    if event_block_count == 0
                    else (
                        f" Hard-blocked {event_block_count} "
                        "contract(s) for event risk."
                    )
                )

                self.log_message(
                    f"{underlying}: "
                    f"{len(stock_option_rows)} "
                    f"{contract_type.value} "
                    "contracts passed options "
                    "liquidity filters."
                    + freshness_note
                    + activity_note
                    + volume_filter_note
                    + event_note
                )

            else:

                freshness_note = (
                    ""
                    if stale_quote_count == 0
                    else (
                        f" "
                        f"Rejected {stale_quote_count} "
                        "stale/missing-timestamp "
                        "quote(s)."
                    )
                )

                self.log_message(
                    f"{underlying}: no "
                    f"{contract_type.value} "
                    "contracts had acceptable "
                    "quotes/Greeks/liquidity."
                    + freshness_note
                    + (
                        f" Rejected {daily_volume_filter_count} "
                        "below daily-volume floor."
                        if daily_volume_filter_count
                        else ""
                    )
                    + (
                        f" Hard-blocked {event_block_count} "
                        "for event risk."
                        if event_block_count
                        else ""
                    )
                )

        # --------------------------------------------------
        # NO RESULTS
        # --------------------------------------------------

        if not all_rows:

            return pd.DataFrame()

        results = pd.DataFrame(
            all_rows
        )

        results = (
            self._annotate_and_update_option_iv_context(
                results,
                contract_type,
            )
        )

        # ==================================================
        # STOCK + OPTION COMBINED SCORE
        # ==================================================

        results[
            "combined_score"
        ] = (
            results[
                "stock_score"
            ]
            * self.parameters[
                "option_stock_weight"
            ]
            +
            results[
                "option_score"
            ]
            * self.parameters[
                "option_contract_weight"
            ]
        )

        results = (
            results
            .sort_values(
                "combined_score",
                ascending=False,
            )
            .reset_index(
                drop=True
            )
        )

        return results


    # ======================================================
    # OPTION OUTPUT
    # ======================================================

    def log_option_candidates(
        self,
        options,
        title,
    ):

        if options.empty:

            self.log_message(
                f"No eligible {title.lower()}."
            )

            return

        display = (
            options
            .head(
                self.parameters[
                    "option_top_results"
                ]
            )
        )[
            [
                "underlying",
                "contract",
                "expiration",
                "dte",
                "strike",
                "bid",
                "ask",
                "quote_age_seconds",
                "spread_pct",
                "open_interest",
                "daily_volume",
                "daily_trade_count",
                "daily_activity_status",
                "delta",
                "iv",
                "iv_percentile",
                "iv_rank",
                "iv_history_samples",
                "event_risk",
                "estimated_ask_cost",
                "option_score",
                "combined_score",
            ]
        ].copy()

        display[
            "strike"
        ] = (
            display["strike"]
            .round(2)
        )

        display[
            "bid"
        ] = (
            display["bid"]
            .round(2)
        )

        display[
            "ask"
        ] = (
            display["ask"]
            .round(2)
        )

        display[
            "quote_age_seconds"
        ] = (
            pd.to_numeric(
                display[
                    "quote_age_seconds"
                ],
                errors="coerce",
            )
            .round(1)
        )

        display[
            "spread_pct"
        ] = (
            display["spread_pct"]
            * 100
        ).round(1)

        display[
            "delta"
        ] = (
            display["delta"]
            .round(3)
        )

        display[
            "iv"
        ] = (
            display["iv"]
            * 100
        ).round(1)

        for column in (
            "daily_volume",
            "daily_trade_count",
            "iv_history_samples",
        ):

            display[column] = (
                pd.to_numeric(
                    display[column],
                    errors="coerce",
                )
                .round(0)
            )

        for column in (
            "iv_percentile",
            "iv_rank",
        ):

            display[column] = (
                pd.to_numeric(
                    display[column],
                    errors="coerce",
                )
                * 100
            ).round(1)

        display[
            "estimated_ask_cost"
        ] = (
            display[
                "estimated_ask_cost"
            ]
            .round(2)
        )

        display[
            "option_score"
        ] = (
            display[
                "option_score"
            ]
            .round(1)
        )

        display[
            "combined_score"
        ] = (
            display[
                "combined_score"
            ]
            .round(1)
        )

        display = display.rename(
            columns={
                "quote_age_seconds":
                    "quote_age_s",
                "spread_pct":
                    "spread_%",
                "iv":
                    "iv_%",
                "daily_volume":
                    "day_vol",
                "daily_trade_count":
                    "day_trades",
                "daily_activity_status":
                    "day_vol_status",
                "iv_percentile":
                    "iv_pctile_%",
                "iv_rank":
                    "iv_rank_%",
                "iv_history_samples":
                    "iv_hist_n",
                "estimated_ask_cost":
                    "ask_cost",
            }
        )

        self.log_message(
            "\n\n"
            f"===== {title} =====\n"
            + display.to_string(
                index=False
            )
            + "\n"
            + "="
            * (
                len(title)
                + 12
            )
        )


    # ======================================================
    # OPTIONS PERMISSION HELPER
    # ======================================================

    def _options_level_allows(
        self,
        required_level,
    ):
        """
        Return True when the account's known Alpaca options
        level meets the requested level.

        If Alpaca did not return a level, keep analysis
        enabled because this scanner does not place orders.
        """

        level = self.options_trading_level

        if level is None:
            return True

        try:

            return int(level) >= int(
                required_level
            )

        except (
            TypeError,
            ValueError,
        ):

            return True


    # ======================================================
    # LONG OPTION STRUCTURE
    # ======================================================

    def _build_long_structure(
        self,
        option_row,
        contract_type,
    ):

        strike = float(
            option_row["strike"]
        )

        premium = float(
            option_row["ask"]
        )

        max_risk = (
            premium * 100
        )

        if (
            contract_type
            == ContractType.CALL
        ):

            decision = "LONG CALL"

            breakeven = (
                strike + premium
            )

            max_reward = float(
                "inf"
            )

            reward_risk = float(
                "inf"
            )

        else:

            decision = "LONG PUT"

            breakeven = (
                strike - premium
            )

            max_reward = max(
                0.0,
                (
                    strike
                    - premium
                )
                * 100,
            )

            reward_risk = (
                max_reward / max_risk
                if max_risk > 0
                else 0.0
            )

        # --------------------------------------------------
        # IV CONTEXT PENALTY FOR LONG PREMIUM
        # --------------------------------------------------
        #
        # Prefer observed historical percentile after the
        # history has matured. During warm-up, preserve the
        # original absolute-IV heuristic as a fallback.
        # --------------------------------------------------

        iv = float(
            option_row["iv"]
        )

        max_penalty = float(
            self.parameters[
                "long_option_max_iv_penalty"
            ]
        )

        iv_penalty = 0.0
        iv_penalty_basis = "none"

        raw_percentile = option_row.get(
            "iv_percentile",
            None,
        )

        iv_percentile = None

        try:
            if (
                raw_percentile is not None
                and not pd.isna(
                    raw_percentile
                )
            ):
                iv_percentile = float(
                    raw_percentile
                )
        except (
            TypeError,
            ValueError,
        ):
            iv_percentile = None

        percentile_threshold = float(
            self.parameters[
                "long_option_high_iv_percentile_threshold"
            ]
        )

        if (
            iv_percentile is not None
            and percentile_threshold < 1.0
            and iv_percentile
            > percentile_threshold
        ):

            iv_penalty = min(
                max_penalty,
                (
                    (
                        iv_percentile
                        - percentile_threshold
                    )
                    / max(
                        0.01,
                        1.0
                        - percentile_threshold,
                    )
                )
                * max_penalty,
            )

            iv_penalty_basis = (
                "observed IV percentile"
            )

        elif iv_percentile is None:

            threshold = float(
                self.parameters[
                    "long_option_high_iv_threshold"
                ]
            )

            if (
                threshold > 0
                and iv > threshold
            ):

                iv_penalty = min(
                    max_penalty,
                    (
                        (iv - threshold)
                        / threshold
                    )
                    * max_penalty,
                )

                iv_penalty_basis = (
                    "absolute-IV warm-up fallback"
                )

        structure_score = max(
            0.0,
            float(
                option_row[
                    "combined_score"
                ]
            )
            - iv_penalty,
        )

        return {
            "underlying":
                option_row[
                    "underlying"
                ],

            "direction":
                (
                    "BULLISH"
                    if contract_type
                    == ContractType.CALL
                    else "BEARISH"
                ),

            "decision":
                decision,

            "long_contract":
                option_row[
                    "contract"
                ],

            "short_contract":
                "",

            "expiration":
                option_row[
                    "expiration"
                ],

            "long_strike":
                strike,

            "short_strike":
                None,

            "net_debit":
                premium,

            "max_risk":
                max_risk,

            "max_reward":
                max_reward,

            "reward_risk":
                reward_risk,

            "breakeven":
                breakeven,

            "iv":
                iv,

            "iv_percentile":
                option_row.get(
                    "iv_percentile"
                ),

            "iv_rank":
                option_row.get(
                    "iv_rank"
                ),

            "iv_history_samples":
                option_row.get(
                    "iv_history_samples",
                    0,
                ),

            "daily_volume":
                option_row.get(
                    "daily_volume",
                    0.0,
                ),

            "daily_activity_status":
                option_row.get(
                    "daily_activity_status",
                    "UNKNOWN",
                ),

            "event_risk":
                option_row.get(
                    "event_risk",
                    "UNKNOWN",
                ),

            "stock_score":
                float(
                    option_row[
                        "stock_score"
                    ]
                ),

            "option_score":
                float(
                    option_row[
                        "option_score"
                    ]
                ),

            "structure_score":
                structure_score,

            "reason":
                (
                    "Best eligible long option"
                    if iv_penalty == 0
                    else (
                        "Best eligible long option; "
                        f"high-IV premium penalty applied "
                        f"using {iv_penalty_basis}"
                    )
                ),
        }


    # ======================================================
    # VERTICAL SPREAD SEARCH
    # ======================================================

    def _find_best_vertical(
        self,
        long_row,
        contract_type,
    ):

        # Alpaca level 3 adds multi-leg spreads.
        if not self._options_level_allows(
            3
        ):

            return None

        underlying = str(
            long_row["underlying"]
        )

        expiration = (
            long_row["expiration"]
        )

        underlying_price = float(
            long_row.get(
                "underlying_price",
                0,
            )
            or 0
        )

        # rank_option_candidates() carries the actual
        # underlying price into each option row. Keep the
        # strike fallback only as a defensive safeguard for
        # externally supplied/legacy rows.
        if underlying_price <= 0:

            underlying_price = float(
                long_row["strike"]
            )

        long_strike = float(
            long_row["strike"]
        )

        long_ask = float(
            long_row["ask"]
        )

        long_max_risk = (
            long_ask * 100
        )

        minimum_width = max(
            0.01,
            underlying_price
            * self.parameters[
                "vertical_min_width_pct"
            ],
        )

        maximum_width = max(
            minimum_width,
            underlying_price
            * self.parameters[
                "vertical_max_width_pct"
            ],
        )

        # --------------------------------------------------
        # SHORT STRIKE RANGE
        # --------------------------------------------------

        if (
            contract_type
            == ContractType.CALL
        ):

            minimum_strike = (
                long_strike
                + minimum_width
            )

            maximum_strike = (
                long_strike
                + maximum_width
            )

        else:

            minimum_strike = max(
                0.01,
                long_strike
                - maximum_width,
            )

            maximum_strike = max(
                0.01,
                long_strike
                - minimum_width,
            )

        try:

            request = (
                GetOptionContractsRequest(
                    underlying_symbols=[
                        underlying
                    ],
                    status=(
                        AssetStatus.ACTIVE
                    ),
                    expiration_date=(
                        expiration
                    ),
                    type=contract_type,
                    style=(
                        ExerciseStyle.AMERICAN
                    ),
                    strike_price_gte=(
                        f"{minimum_strike:.2f}"
                    ),
                    strike_price_lte=(
                        f"{maximum_strike:.2f}"
                    ),
                    limit=1000,
                )
            )

            response = (
                self.alpaca_trading_client
                .get_option_contracts(
                    request
                )
            )

            contracts = (
                response.option_contracts
                or []
            )

        except Exception as exc:

            self.log_message(
                f"{underlying}: vertical "
                f"contract lookup failed: "
                f"{exc}"
            )

            return None

        eligible = []

        for contract in contracts:

            if not contract.tradable:
                continue

            try:

                contract_size = int(
                    float(
                        contract.size
                    )
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            if contract_size != 100:
                continue

            try:

                short_strike = float(
                    contract.strike_price
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            # Enforce the correct OTM direction.
            if (
                contract_type
                == ContractType.CALL
                and short_strike
                <= long_strike
            ):

                continue

            if (
                contract_type
                == ContractType.PUT
                and short_strike
                >= long_strike
            ):

                continue

            try:

                open_interest = int(
                    float(
                        contract.open_interest
                        or 0
                    )
                )

            except (
                TypeError,
                ValueError,
            ):

                open_interest = 0

            if (
                open_interest
                < self.parameters[
                    "option_min_open_interest"
                ]
            ):

                continue

            eligible.append(
                {
                    "contract":
                        contract,
                    "strike":
                        short_strike,
                    "open_interest":
                        open_interest,
                }
            )

        if not eligible:
            return None

        try:

            snapshots = (
                self._get_option_snapshots(
                    [
                        item[
                            "contract"
                        ].symbol
                        for item
                        in eligible
                    ]
                )
            )

        except Exception as exc:

            self.log_message(
                f"{underlying}: vertical "
                f"snapshot lookup failed: "
                f"{exc}"
            )

            return None

        candidates = []

        for item in eligible:

            contract = item[
                "contract"
            ]

            snapshot = snapshots.get(
                contract.symbol
            )

            if snapshot is None:
                continue

            quote = getattr(
                snapshot,
                "latest_quote",
                None,
            )

            greeks = getattr(
                snapshot,
                "greeks",
                None,
            )

            if (
                quote is None
                or greeks is None
            ):

                continue

            (
                quote_is_fresh,
                _quote_age_seconds,
            ) = self._is_option_quote_fresh(
                quote
            )

            if not quote_is_fresh:

                continue

            try:

                short_bid = float(
                    quote.bid_price
                    or 0
                )

                short_ask = float(
                    quote.ask_price
                    or 0
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            if (
                short_bid <= 0
                or short_ask <= 0
                or short_ask < short_bid
            ):

                continue

            short_mid = (
                short_bid + short_ask
            ) / 2

            short_spread_pct = (
                (
                    short_ask
                    - short_bid
                )
                / short_mid
                if short_mid > 0
                else float("inf")
            )

            if (
                short_spread_pct
                > self.parameters[
                    "option_max_spread_pct"
                ]
            ):

                continue

            delta_raw = getattr(
                greeks,
                "delta",
                None,
            )

            if delta_raw is None:
                continue

            try:

                short_delta = float(
                    delta_raw
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            abs_short_delta = abs(
                short_delta
            )

            if (
                abs_short_delta
                < self.parameters[
                    "vertical_short_min_abs_delta"
                ]
                or abs_short_delta
                > self.parameters[
                    "vertical_short_max_abs_delta"
                ]
            ):

                continue

            short_strike = item[
                "strike"
            ]

            width = abs(
                short_strike
                - long_strike
            )

            # Conservative entry assumption:
            #
            # buy long leg at its ask
            # sell short leg at its bid
            net_debit = (
                long_ask
                - short_bid
            )

            if net_debit <= 0:
                continue

            # A debit spread whose debit is equal to or
            # greater than its width has no positive
            # expiration payoff.
            if net_debit >= width:
                continue

            max_risk = (
                net_debit
                * 100
            )

            max_reward = (
                (
                    width
                    - net_debit
                )
                * 100
            )

            reward_risk = (
                max_reward
                / max_risk
                if max_risk > 0
                else 0.0
            )

            if (
                reward_risk
                < self.parameters[
                    "vertical_min_reward_risk"
                ]
            ):

                continue

            if (
                contract_type
                == ContractType.CALL
            ):

                decision = (
                    "BULL CALL SPREAD"
                )

                breakeven = (
                    long_strike
                    + net_debit
                )

            else:

                decision = (
                    "BEAR PUT SPREAD"
                )

                breakeven = (
                    long_strike
                    - net_debit
                )

            # --------------------------------------------------
            # VERTICAL QUALITY SCORE
            # --------------------------------------------------

            rr_full = max(
                0.01,
                self.parameters[
                    "vertical_full_reward_risk_score"
                ],
            )

            reward_risk_score = min(
                1.0,
                reward_risk
                / rr_full,
            )

            debit_efficiency = max(
                0.0,
                min(
                    1.0,
                    1.0
                    - (
                        net_debit
                        / width
                    ),
                ),
            )

            short_spread_score = max(
                0.0,
                1.0
                - (
                    short_spread_pct
                    / self.parameters[
                        "option_max_spread_pct"
                    ]
                ),
            )

            oi_full_score = max(
                1,
                self.parameters[
                    "option_open_interest_full_score"
                ],
            )

            short_oi_score = min(
                1.0,
                math.log1p(
                    item[
                        "open_interest"
                    ]
                )
                / math.log1p(
                    oi_full_score
                ),
            )

            risk_reduction_score = (
                max(
                    0.0,
                    min(
                        1.0,
                        1.0
                        - (
                            max_risk
                            / long_max_risk
                        ),
                    ),
                )
                if long_max_risk > 0
                else 0.0
            )

            vertical_quality = 100 * (
                reward_risk_score
                * 0.30
                + debit_efficiency
                * 0.20
                + short_spread_score
                * 0.15
                + short_oi_score
                * 0.15
                + risk_reduction_score
                * 0.20
            )

            structure_score = (
                float(
                    long_row[
                        "combined_score"
                    ]
                )
                * 0.65
                + vertical_quality
                * 0.35
            )

            candidates.append(
                {
                    "underlying":
                        underlying,

                    "direction":
                        (
                            "BULLISH"
                            if contract_type
                            == ContractType.CALL
                            else "BEARISH"
                        ),

                    "decision":
                        decision,

                    "long_contract":
                        long_row[
                            "contract"
                        ],

                    "short_contract":
                        contract.symbol,

                    "expiration":
                        expiration,

                    "long_strike":
                        long_strike,

                    "short_strike":
                        short_strike,

                    "net_debit":
                        net_debit,

                    "max_risk":
                        max_risk,

                    "max_reward":
                        max_reward,

                    "reward_risk":
                        reward_risk,

                    "breakeven":
                        breakeven,

                    "iv":
                        float(
                            long_row["iv"]
                        ),

                    "iv_percentile":
                        long_row.get(
                            "iv_percentile"
                        ),

                    "iv_rank":
                        long_row.get(
                            "iv_rank"
                        ),

                    "iv_history_samples":
                        long_row.get(
                            "iv_history_samples",
                            0,
                        ),

                    "daily_volume":
                        long_row.get(
                            "daily_volume",
                            0.0,
                        ),

                    "daily_activity_status":
                        long_row.get(
                            "daily_activity_status",
                            "UNKNOWN",
                        ),

                    "event_risk":
                        long_row.get(
                            "event_risk",
                            "UNKNOWN",
                        ),

                    "stock_score":
                        float(
                            long_row[
                                "stock_score"
                            ]
                        ),

                    "option_score":
                        float(
                            long_row[
                                "option_score"
                            ]
                        ),

                    "structure_score":
                        structure_score,

                    "reason":
                        (
                            "Defined-risk debit spread; "
                            "short premium reduces cost "
                            "and caps reward"
                        ),
                }
            )

        if not candidates:
            return None

        return max(
            candidates,
            key=lambda row: (
                row[
                    "structure_score"
                ],
                row[
                    "reward_risk"
                ],
            ),
        )


    # ======================================================
    # LONG VS VERTICAL VS NO-TRADE
    # ======================================================

    def rank_trade_structures(
        self,
        stock_candidates,
        options,
        contract_type,
    ):

        rows = []

        required_long_level = 2

        for _, stock in (
            stock_candidates.iterrows()
        ):

            underlying = str(
                stock["symbol"]
            )

            direction = (
                "BULLISH"
                if contract_type
                == ContractType.CALL
                else "BEARISH"
            )

            if not self._options_level_allows(
                required_long_level
            ):

                rows.append(
                    {
                        "underlying":
                            underlying,
                        "direction":
                            direction,
                        "decision":
                            "NO TRADE",
                        "long_contract":
                            "",
                        "short_contract":
                            "",
                        "expiration":
                            None,
                        "long_strike":
                            None,
                        "short_strike":
                            None,
                        "net_debit":
                            None,
                        "max_risk":
                            None,
                        "max_reward":
                            None,
                        "reward_risk":
                            None,
                        "breakeven":
                            None,
                        "iv":
                            None,
                        "stock_score":
                            float(
                                stock[
                                    "score"
                                ]
                            ),
                        "option_score":
                            None,
                        "structure_score":
                            0.0,
                        "reason":
                            (
                                "Account options level "
                                "does not permit long "
                                "calls/puts"
                            ),
                    }
                )

                continue

            stock_options = (
                options[
                    options[
                        "underlying"
                    ]
                    == underlying
                ]
                if not options.empty
                else pd.DataFrame()
            )

            if stock_options.empty:

                rows.append(
                    {
                        "underlying":
                            underlying,
                        "direction":
                            direction,
                        "decision":
                            "NO TRADE",
                        "long_contract":
                            "",
                        "short_contract":
                            "",
                        "expiration":
                            None,
                        "long_strike":
                            None,
                        "short_strike":
                            None,
                        "net_debit":
                            None,
                        "max_risk":
                            None,
                        "max_reward":
                            None,
                        "reward_risk":
                            None,
                        "breakeven":
                            None,
                        "iv":
                            None,
                        "stock_score":
                            float(
                                stock[
                                    "score"
                                ]
                            ),
                        "option_score":
                            None,
                        "structure_score":
                            0.0,
                        "reason":
                            (
                                "No eligible long option "
                                "passed liquidity/Greek "
                                "filters"
                            ),
                    }
                )

                continue

            # --------------------------------------------------
            # EVALUATE EVERY SHORTLISTED LONG + ITS BEST VERTICAL
            # --------------------------------------------------
            #
            # rank_option_candidates() keeps the top N contracts
            # for each underlying. Evaluate each one as a long
            # option and also build the best same-expiration debit
            # vertical from that specific long leg.
            # --------------------------------------------------

            candidates = []

            for _, option_row in (
                stock_options.iterrows()
            ):

                long_structure = (
                    self._build_long_structure(
                        option_row,
                        contract_type,
                    )
                )

                candidates.append(
                    long_structure
                )

                vertical_structure = (
                    self._find_best_vertical(
                        option_row,
                        contract_type,
                    )
                )

                if (
                    vertical_structure
                    is not None
                ):

                    candidates.append(
                        vertical_structure
                    )

            best = max(
                candidates,
                key=lambda row: (
                    row[
                        "structure_score"
                    ],
                    # When scores tie, prefer lower defined
                    # capital at risk. Unlimited long-call reward
                    # should not dominate this tie-breaker.
                    -float(
                        row[
                            "max_risk"
                        ]
                        or 0
                    ),
                ),
            )

            minimum_score = (
                self.parameters[
                    "trade_structure_min_score"
                ]
            )

            if (
                best[
                    "structure_score"
                ]
                < minimum_score
            ):

                best = {
                    "underlying":
                        underlying,
                    "direction":
                        direction,
                    "decision":
                        "NO TRADE",
                    "long_contract":
                        best[
                            "long_contract"
                        ],
                    "short_contract":
                        best[
                            "short_contract"
                        ],
                    "expiration":
                        best[
                            "expiration"
                        ],
                    "long_strike":
                        best[
                            "long_strike"
                        ],
                    "short_strike":
                        best[
                            "short_strike"
                        ],
                    "net_debit":
                        best[
                            "net_debit"
                        ],
                    "max_risk":
                        best[
                            "max_risk"
                        ],
                    "max_reward":
                        best[
                            "max_reward"
                        ],
                    "reward_risk":
                        best[
                            "reward_risk"
                        ],
                    "breakeven":
                        best[
                            "breakeven"
                        ],
                    "iv":
                        best[
                            "iv"
                        ],
                    "stock_score":
                        best[
                            "stock_score"
                        ],
                    "option_score":
                        best[
                            "option_score"
                        ],
                    "structure_score":
                        best[
                            "structure_score"
                        ],
                    "reason":
                        (
                            "Best available structure "
                            f"scored below "
                            f"{minimum_score:.1f}"
                        ),
                }

            rows.append(
                best
            )

        return pd.DataFrame(
            rows
        )


    # ======================================================
    # TRADE STRUCTURE OUTPUT
    # ======================================================

    def log_trade_structures(
        self,
        structures,
        title,
    ):

        if structures.empty:

            self.log_message(
                f"No {title.lower()}."
            )

            return

        display = structures[
            [
                "underlying",
                "decision",
                "expiration",
                "long_strike",
                "short_strike",
                "net_debit",
                "max_risk",
                "max_reward",
                "reward_risk",
                "breakeven",
                "iv",
                "structure_score",
                "reason",
            ]
        ].copy()

        # NO TRADE is not an actionable position. Preserve the
        # rejected candidate economics in the internal DataFrame,
        # but blank them from the recommendation table so they
        # cannot be mistaken for an order suggestion.
        no_trade_mask = (
            display["decision"]
            == "NO TRADE"
        )

        display.loc[
            no_trade_mask,
            [
                "expiration",
                "long_strike",
                "short_strike",
                "net_debit",
                "max_risk",
                "max_reward",
                "reward_risk",
                "breakeven",
                "iv",
            ],
        ] = None

        numeric_columns = [
            "long_strike",
            "short_strike",
            "net_debit",
            "max_risk",
            "breakeven",
        ]

        for column in numeric_columns:

            display[
                column
            ] = (
                pd.to_numeric(
                    display[
                        column
                    ],
                    errors="coerce",
                )
                .round(2)
            )

        display[
            "iv"
        ] = (
            pd.to_numeric(
                display[
                    "iv"
                ],
                errors="coerce",
            )
            * 100
        ).round(1)

        display[
            "structure_score"
        ] = (
            pd.to_numeric(
                display[
                    "structure_score"
                ],
                errors="coerce",
            )
            .round(1)
        )

        def format_max_reward(
            value,
        ):

            if value is None:
                return ""

            try:

                numeric = float(
                    value
                )

            except (
                TypeError,
                ValueError,
            ):

                return ""

            if math.isinf(
                numeric
            ):

                return "UNLIMITED"

            return f"${numeric:,.2f}"

        def format_reward_risk(
            value,
        ):

            if value is None:
                return ""

            try:

                numeric = float(
                    value
                )

            except (
                TypeError,
                ValueError,
            ):

                return ""

            if math.isinf(
                numeric
            ):

                return "UNLIMITED"

            return f"{numeric:.2f}"

        display[
            "max_reward"
        ] = display[
            "max_reward"
        ].map(
            format_max_reward
        )

        display[
            "reward_risk"
        ] = display[
            "reward_risk"
        ].map(
            format_reward_risk
        )

        display = display.rename(
            columns={
                "net_debit":
                    "debit",
                "max_risk":
                    "max_risk_$",
                "max_reward":
                    "max_reward_$",
                "reward_risk":
                    "reward/risk",
                "breakeven":
                    "breakeven_$",
                "iv":
                    "iv_%",
                "structure_score":
                    "score",
            }
        )

        # Force non-actionable NO TRADE economics to render
        # as blanks after all numeric/string formatting.
        blank_columns = [
            "expiration",
            "long_strike",
            "short_strike",
            "debit",
            "max_risk_$",
            "max_reward_$",
            "reward/risk",
            "breakeven_$",
            "iv_%",
        ]

        display[
            blank_columns
        ] = (
            display[
                blank_columns
            ]
            .astype(object)
        )

        display.loc[
            no_trade_mask,
            blank_columns,
        ] = ""

        self.log_message(
            "\n\n"
            f"===== {title} =====\n"
            + display.to_string(
                index=False
            )
            + "\n"
            + "="
            * (
                len(title)
                + 12
            )
        )


    # ======================================================
    # TRADE STRUCTURE SCANNER
    # ======================================================

    def scan_trade_structures(
        self,
        bullish_stocks,
        bearish_stocks,
        bullish_options,
        bearish_options,
    ):

        self.log_message(
            "Comparing long options, "
            "vertical spreads, and no-trade..."
        )

        bullish_structures = (
            self.rank_trade_structures(
                bullish_stocks,
                bullish_options,
                ContractType.CALL,
            )
        )

        bearish_structures = (
            self.rank_trade_structures(
                bearish_stocks,
                bearish_options,
                ContractType.PUT,
            )
        )

        self.log_trade_structures(
            bullish_structures,
            "BULLISH TRADE STRUCTURE RANKING",
        )

        self.log_trade_structures(
            bearish_structures,
            "BEARISH TRADE STRUCTURE RANKING",
        )

        return (
            bullish_structures,
            bearish_structures,
        )


    # ======================================================
    # MICRO-ACCOUNT MODE CONTEXT
    # ======================================================

    def _get_micro_account_context(
        self,
    ):

        account = (
            self._get_account_risk_snapshot()
        )

        actual_equity = float(
            account.get(
                "equity",
                0.0,
            )
            or 0.0
        )

        actual_cash = max(
            0.0,
            float(
                account.get(
                    "cash",
                    0.0,
                )
                or 0.0
            ),
        )

        simulated = (
            self.micro_account_test_equity
            is not None
        )

        if simulated:

            effective_equity = float(
                self.micro_account_test_equity
            )

            # A test balance is a sizing sandbox only.
            # It never changes the broker account.
            deployable_cash = (
                effective_equity
            )

        else:

            effective_equity = (
                actual_equity
            )

            # MICRO mode is intentionally cash-only.
            # Do not use margin buying power.
            deployable_cash = min(
                actual_cash,
                effective_equity,
            )

        threshold = float(
            self.parameters[
                "micro_account_equity_threshold"
            ]
        )

        active = (
            effective_equity > 0
            and (
                simulated
                or self.micro_account_force
                or effective_equity
                <= threshold
            )
        )

        if simulated:

            reason = (
                "MICRO_ACCOUNT_TEST_EQUITY "
                "override"
            )

        elif self.micro_account_force:

            reason = (
                "MICRO_ACCOUNT_FORCE enabled"
            )

        elif (
            effective_equity > 0
            and effective_equity <= threshold
        ):

            reason = (
                f"equity <= ${threshold:,.2f} "
                "micro threshold"
            )

        else:

            reason = (
                f"equity > ${threshold:,.2f} "
                "micro threshold"
            )

        return {
            **account,

            "actual_equity":
                actual_equity,

            "effective_equity":
                effective_equity,

            "deployable_cash":
                deployable_cash,

            "simulated":
                simulated,

            "active":
                active,

            "mode_reason":
                reason,
        }


    # ======================================================
    # MICRO STOCK QUOTE HELPER
    # ======================================================

    def _get_micro_stock_quotes(
        self,
        symbols,
    ):

        if not symbols:

            return {}

        request = StockLatestQuoteRequest(
            symbol_or_symbols=list(
                symbols
            ),
        )

        response = (
            self.alpaca_stock_data_client
            .get_stock_latest_quote(
                request
            )
        )

        return response or {}


    def _is_micro_stock_quote_fresh(
        self,
        quote,
    ):

        # Alpaca stock Quote and option Quote objects share
        # the same timestamp field, so reuse the existing
        # timestamp-age parser.
        age_seconds = (
            self._option_quote_age_seconds(
                quote
            )
        )

        if age_seconds is None:

            return (
                False,
                None,
            )

        future_tolerance = float(
            self.parameters[
                "option_quote_future_tolerance_seconds"
            ]
        )

        maximum_age = float(
            self.parameters[
                "micro_stock_quote_max_age_seconds"
            ]
        )

        if (
            age_seconds
            < -future_tolerance
        ):

            return (
                False,
                age_seconds,
            )

        return (
            age_seconds <= maximum_age,
            age_seconds,
        )


    # ======================================================
    # MICRO FRACTIONAL-STOCK SIZING
    # ======================================================

    def size_micro_fractional_candidates(
        self,
        bullish,
        context,
    ):

        if (
            bullish is None
            or bullish.empty
        ):

            return pd.DataFrame()

        effective_equity = float(
            context[
                "effective_equity"
            ]
        )

        deployable_cash = float(
            context[
                "deployable_cash"
            ]
        )

        if (
            context.get(
                "trading_blocked",
                False,
            )
            or context.get(
                "account_blocked",
                False,
            )
            or context.get(
                "trade_suspended_by_user",
                False,
            )
        ):

            self.log_message(
                "Micro-account alerts blocked "
                "because the Alpaca account is "
                "not currently trade-enabled."
            )

            return pd.DataFrame()

        if (
            effective_equity <= 0
            or deployable_cash <= 0
        ):

            self.log_message(
                "Micro-account mode has no "
                "deployable cash."
            )

            return pd.DataFrame()

        broker_stock_symbols = set()
        broker_stock_gross = 0.0
        broker_stock_capacity = float(
            "inf"
        )

        if context.get(
            "simulated",
            False,
        ):

            # MICRO_ACCOUNT_TEST_EQUITY is an isolated sizing
            # sandbox. Do not let unrelated positions in the
            # larger paper account distort the simulated balance.
            context[
                "broker_portfolio_exposure"
            ] = {
                "available": False,
                "skipped_for_simulation": True,
                "stock_gross_market_value": 0.0,
                "stock_gross_pct_equity": 0.0,
                "stock_symbols": [],
            }

        else:

            broker_exposure = (
                self._get_broker_portfolio_exposure(
                    effective_equity
                )
            )

            context[
                "broker_portfolio_exposure"
            ] = broker_exposure

            if (
                self.parameters[
                    "portfolio_require_broker_positions_snapshot"
                ]
                and not broker_exposure[
                    "available"
                ]
            ):

                self.log_message(
                    "Micro-account alerts blocked "
                    "because broker open positions "
                    "could not be read."
                )

                return pd.DataFrame()

            broker_stock_symbols = set(
                broker_exposure.get(
                    "stock_symbols",
                    [],
                )
            )

            broker_stock_gross = float(
                broker_exposure.get(
                    "stock_gross_market_value",
                    0.0,
                )
                or 0.0
            )

            stock_cap_pct = float(
                self.parameters[
                    "micro_max_broker_stock_gross_pct_equity"
                ]
            )

            if stock_cap_pct > 0:

                broker_stock_capacity = max(
                    0.0,
                    effective_equity
                    * stock_cap_pct
                    - broker_stock_gross,
                )

        minimum_score = float(
            self.parameters[
                "micro_min_stock_score"
            ]
        )

        candidates = (
            bullish[
                bullish[
                    "score"
                ]
                >= minimum_score
            ]
            .sort_values(
                "score",
                ascending=False,
            )
            .copy()
        )

        if candidates.empty:

            self.log_message(
                "No bullish stock candidate met "
                f"the micro score floor of "
                f"{minimum_score:.1f}."
            )

            return pd.DataFrame()

        asset_map = getattr(
            self,
            "_stock_asset_by_symbol",
            {},
        )

        eligible_symbols = []

        fractionable_by_symbol = {}

        for symbol in (
            candidates[
                "symbol"
            ]
            .astype(str)
        ):

            asset = asset_map.get(
                symbol
            )

            fractionable = bool(
                getattr(
                    asset,
                    "fractionable",
                    False,
                )
            )

            fractionable_by_symbol[
                symbol
            ] = fractionable

            if fractionable:

                eligible_symbols.append(
                    symbol
                )

        if not eligible_symbols:

            self.log_message(
                "No bullish micro candidates "
                "were marked fractionable by "
                "Alpaca."
            )

            return pd.DataFrame()

        quote_symbols = [
            symbol
            for symbol in eligible_symbols
            if not (
                self.parameters[
                    "micro_block_existing_stock_position"
                ]
                and symbol
                in broker_stock_symbols
            )
        ]

        try:

            quotes = (
                self._get_micro_stock_quotes(
                    quote_symbols
                )
            )

        except Exception as exc:

            self.log_message(
                "Micro-account stock quote "
                f"lookup failed: {exc}"
            )

            return pd.DataFrame()

        per_position_budget = min(
            effective_equity
            * float(
                self.parameters[
                    "micro_position_pct_equity"
                ]
            ),
            float(
                self.parameters[
                    "micro_max_position_dollars"
                ]
            ),
        )

        total_budget = min(
            effective_equity
            * float(
                self.parameters[
                    "micro_total_allocation_pct_equity"
                ]
            ),
            deployable_cash,
            broker_stock_capacity,
        )

        minimum_notional = float(
            self.parameters[
                "micro_min_notional_dollars"
            ]
        )

        stock_exposure_cap_exhausted = (
            broker_stock_capacity
            < minimum_notional
        )

        if (
            total_budget < minimum_notional
            and stock_exposure_cap_exhausted
        ):

            self.log_message(
                "Micro-account portfolio stock "
                "exposure cap leaves less than "
                "the minimum notional for a new alert. "
                f"Broker stock gross="
                f"${broker_stock_gross:,.2f}."
            )

        maximum_positions = int(
            self.parameters[
                "micro_max_positions_per_run"
            ]
        )

        stop_loss_pct = float(
            self.parameters[
                "micro_stop_loss_pct"
            ]
        )

        profit_target_pct = float(
            self.parameters[
                "micro_profit_target_pct"
            ]
        )

        remaining_budget = (
            total_budget
        )

        alert_count = 0

        rows = []

        for _, stock in (
            candidates.iterrows()
        ):

            symbol = str(
                stock[
                    "symbol"
                ]
            )

            row = {
                "symbol":
                    symbol,

                "score":
                    float(
                        stock[
                            "score"
                        ]
                    ),

                "fractionable":
                    fractionable_by_symbol.get(
                        symbol,
                        False,
                    ),

                "notional":
                    0.0,

                "approx_shares":
                    0.0,

                "reference_bid":
                    None,

                "reference_ask":
                    None,

                "quote_age_seconds":
                    None,

                "spread_pct":
                    None,

                "stop_price":
                    None,

                "target_price":
                    None,

                "planned_loss":
                    0.0,

                "planned_gain":
                    0.0,

                "planned_risk_pct_equity":
                    0.0,

                "alert_status":
                    "SKIP",

                "reason":
                    "",
            }

            if not row[
                "fractionable"
            ]:

                row[
                    "reason"
                ] = (
                    "Alpaca asset is not "
                    "fractionable"
                )

                rows.append(
                    row
                )

                continue

            if (
                self.parameters[
                    "micro_block_existing_stock_position"
                ]
                and symbol
                in broker_stock_symbols
            ):

                row[
                    "reason"
                ] = (
                    "Broker already holds this "
                    "stock; micro stacking blocked"
                )

                rows.append(
                    row
                )

                continue

            if (
                alert_count
                >= maximum_positions
            ):

                row[
                    "reason"
                ] = (
                    "Micro max positions/run "
                    "reached"
                )

                rows.append(
                    row
                )

                continue

            if (
                remaining_budget
                < minimum_notional
            ):

                row[
                    "reason"
                ] = (
                    (
                        "Broker stock exposure cap "
                        "leaves no micro allocation"
                    )
                    if stock_exposure_cap_exhausted
                    else (
                        "Remaining micro allocation "
                        "is below minimum notional"
                    )
                )

                rows.append(
                    row
                )

                continue

            quote = quotes.get(
                symbol
            )

            if quote is None:

                row[
                    "reason"
                ] = (
                    "No current Alpaca stock "
                    "quote"
                )

                rows.append(
                    row
                )

                continue

            (
                quote_is_fresh,
                quote_age_seconds,
            ) = self._is_micro_stock_quote_fresh(
                quote
            )

            row[
                "quote_age_seconds"
            ] = quote_age_seconds

            if not quote_is_fresh:

                row[
                    "reason"
                ] = (
                    "Stock quote stale or "
                    "timestamp unavailable"
                )

                rows.append(
                    row
                )

                continue

            try:

                bid = float(
                    quote.bid_price
                    or 0
                )

                ask = float(
                    quote.ask_price
                    or 0
                )

            except (
                TypeError,
                ValueError,
            ):

                row[
                    "reason"
                ] = (
                    "Invalid stock bid/ask"
                )

                rows.append(
                    row
                )

                continue

            row[
                "reference_bid"
            ] = bid

            row[
                "reference_ask"
            ] = ask

            if (
                bid <= 0
                or ask <= 0
                or ask < bid
            ):

                row[
                    "reason"
                ] = (
                    "Invalid stock bid/ask"
                )

                rows.append(
                    row
                )

                continue

            midpoint = (
                bid + ask
            ) / 2

            spread_pct = (
                (
                    ask - bid
                )
                / midpoint
                if midpoint > 0
                else float(
                    "inf"
                )
            )

            row[
                "spread_pct"
            ] = spread_pct

            if (
                spread_pct
                > float(
                    self.parameters[
                        "micro_stock_max_spread_pct"
                    ]
                )
            ):

                row[
                    "reason"
                ] = (
                    "Stock quote spread too wide"
                )

                rows.append(
                    row
                )

                continue

            notional = min(
                per_position_budget,
                remaining_budget,
            )

            if (
                notional
                < minimum_notional
            ):

                row[
                    "reason"
                ] = (
                    "Position budget below "
                    "minimum notional"
                )

                rows.append(
                    row
                )

                continue

            # Use the ask as the conservative reference
            # entry for a long-only alert.
            reference_entry = ask

            approximate_shares = (
                notional
                / reference_entry
            )

            planned_loss = (
                notional
                * stop_loss_pct
            )

            planned_gain = (
                notional
                * profit_target_pct
            )

            row.update(
                {
                    "notional":
                        notional,

                    "approx_shares":
                        approximate_shares,

                    "stop_price":
                        reference_entry
                        * (
                            1
                            - stop_loss_pct
                        ),

                    "target_price":
                        reference_entry
                        * (
                            1
                            + profit_target_pct
                        ),

                    "planned_loss":
                        planned_loss,

                    "planned_gain":
                        planned_gain,

                    "planned_risk_pct_equity":
                        (
                            planned_loss
                            / effective_equity
                            if effective_equity > 0
                            else 0.0
                        ),

                    "alert_status":
                        "ALERT",

                    "reason":
                        (
                            "Fractionable bullish "
                            "candidate fits micro "
                            "cash/allocation limits"
                        ),
                }
            )

            rows.append(
                row
            )

            remaining_budget -= (
                notional
            )

            alert_count += 1

        return pd.DataFrame(
            rows
        )


    # ======================================================
    # MICRO FRACTIONAL-STOCK OUTPUT
    # ======================================================

    def log_micro_fractional_sizing(
        self,
        sized,
        context,
    ):

        self.log_message(
            "MICRO ACCOUNT MODE ACTIVE: "
            f"effective equity="
            f"${context['effective_equity']:,.2f}, "
            f"deployable cash="
            f"${context['deployable_cash']:,.2f}, "
            f"basis={context['mode_reason']}."
        )

        if context.get(
            "simulated",
            False,
        ):

            self.log_message(
                "MICRO TEST BALANCE ONLY: "
                f"actual Alpaca equity is "
                f"${context['actual_equity']:,.2f}; "
                "no broker balance was changed."
            )

        broker_exposure = context.get(
            "broker_portfolio_exposure",
            {},
        )

        if broker_exposure.get(
            "skipped_for_simulation",
            False,
        ):

            self.log_message(
                "MICRO PORTFOLIO GUARD: broker "
                "stock holdings ignored for the "
                "simulated test balance."
            )

        elif broker_exposure.get(
            "available",
            False,
        ):

            self.log_message(
                "MICRO PORTFOLIO GUARD: broker "
                f"stock gross="
                f"${broker_exposure.get('stock_gross_market_value', 0.0):,.2f} "
                f"({broker_exposure.get('stock_gross_pct_equity', 0.0) * 100:.2f}% "
                "of effective equity)."
            )

        if sized.empty:

            self.log_message(
                "No micro fractional-stock "
                "alerts were sizeable."
            )

            return

        display = sized[
            [
                "symbol",
                "score",
                "fractionable",
                "reference_bid",
                "reference_ask",
                "quote_age_seconds",
                "spread_pct",
                "notional",
                "approx_shares",
                "stop_price",
                "target_price",
                "planned_loss",
                "planned_gain",
                "planned_risk_pct_equity",
                "alert_status",
                "reason",
            ]
        ].copy()

        for column in (
            "score",
            "reference_bid",
            "reference_ask",
            "notional",
            "stop_price",
            "target_price",
            "planned_loss",
            "planned_gain",
        ):

            display[
                column
            ] = (
                pd.to_numeric(
                    display[
                        column
                    ],
                    errors="coerce",
                )
                .round(2)
            )

        display[
            "approx_shares"
        ] = (
            pd.to_numeric(
                display[
                    "approx_shares"
                ],
                errors="coerce",
            )
            .round(6)
        )

        display[
            "quote_age_seconds"
        ] = (
            pd.to_numeric(
                display[
                    "quote_age_seconds"
                ],
                errors="coerce",
            )
            .round(1)
        )

        display[
            "spread_pct"
        ] = (
            pd.to_numeric(
                display[
                    "spread_pct"
                ],
                errors="coerce",
            )
            * 100
        ).round(2)

        display[
            "planned_risk_pct_equity"
        ] = (
            pd.to_numeric(
                display[
                    "planned_risk_pct_equity"
                ],
                errors="coerce",
            )
            * 100
        ).round(3)

        display = display.rename(
            columns={
                "reference_bid":
                    "bid",
                "reference_ask":
                    "ask",
                "quote_age_seconds":
                    "quote_age_s",
                "spread_pct":
                    "spread_%",
                "notional":
                    "notional_$",
                "stop_price":
                    "stop_$",
                "target_price":
                    "target_$",
                "planned_loss":
                    "planned_loss_$",
                "planned_gain":
                    "planned_gain_$",
                "planned_risk_pct_equity":
                    "planned_risk_%",
            }
        )

        self.log_message(
            "\n\n"
            "===== MICRO FRACTIONAL STOCK SIZING =====\n"
            + display.to_string(
                index=False
            )
            + "\n"
            "========================================="
        )


    # ======================================================
    # MICRO FRACTIONAL-STOCK ALERTS
    # ======================================================

    def generate_micro_fractional_alerts(
        self,
        sized,
        context,
    ):

        if not self.trade_alerts_enabled:

            self.log_message(
                "Trade alert generation is "
                "disabled."
            )

            return []

        if sized.empty:

            return []

        actionable = sized[
            sized[
                "alert_status"
            ]
            == "ALERT"
        ]

        if actionable.empty:

            self.log_message(
                "No actionable micro "
                "fractional-stock alerts."
            )

            return []

        today = (
            self.get_datetime().date()
        )

        alerts = []

        for _, row in (
            actionable.iterrows()
        ):

            notional = float(
                row[
                    "notional"
                ]
            )

            # Existing persistent dedupe state stores
            # six-part keys. Use notional cents as the final
            # component so micro alerts survive restarts
            # without changing the file format.
            key = (
                str(
                    today
                ),
                str(
                    row[
                        "symbol"
                    ]
                ),
                "MICRO FRACTIONAL LONG",
                "STOCK",
                "",
                int(
                    round(
                        notional
                        * 100
                    )
                ),
            )

            lifecycle_id = (
                self._alert_position_id(
                    key
                )
            )

            if (
                self.parameters[
                    "alert_once_per_day"
                ]
                and key
                in self._sent_trade_alert_keys
            ):

                self._register_micro_alert_tracked_setup(
                    row,
                    key,
                    "RECONSTRUCTED_SAME_DAY",
                    simulated=bool(
                        context.get(
                            "simulated",
                            False,
                        )
                    ),
                )

                continue

            payload = {
                "timestamp":
                    self.get_datetime()
                    .isoformat(),

                "lifecycle_id":
                    lifecycle_id,

                "lifecycle_status":
                    "ALERTED",

                "asset_type":
                    "FRACTIONAL_STOCK",

                "underlying":
                    str(
                        row[
                            "symbol"
                        ]
                    ),

                "direction":
                    "BULLISH",

                "decision":
                    "MICRO FRACTIONAL LONG",

                "target_notional":
                    notional,

                "approx_shares":
                    float(
                        row[
                            "approx_shares"
                        ]
                    ),

                "reference_bid":
                    float(
                        row[
                            "reference_bid"
                        ]
                    ),

                "reference_ask":
                    float(
                        row[
                            "reference_ask"
                        ]
                    ),

                "quote_age_seconds":
                    float(
                        row[
                            "quote_age_seconds"
                        ]
                    ),

                "stop_price":
                    float(
                        row[
                            "stop_price"
                        ]
                    ),

                "profit_target_price":
                    float(
                        row[
                            "target_price"
                        ]
                    ),

                "planned_loss":
                    float(
                        row[
                            "planned_loss"
                        ]
                    ),

                "planned_gain":
                    float(
                        row[
                            "planned_gain"
                        ]
                    ),

                "planned_risk_pct_equity":
                    float(
                        row[
                            "planned_risk_pct_equity"
                        ]
                    ),

                "stock_score":
                    float(
                        row[
                            "score"
                        ]
                    ),

                "effective_equity":
                    float(
                        context[
                            "effective_equity"
                        ]
                    ),

                "sizing_basis":
                    (
                        "SIMULATED_MICRO_EQUITY"
                        if context.get(
                            "simulated",
                            False,
                        )
                        else
                        "ALPACA_ACCOUNT_EQUITY"
                    ),

                "broker_stock_gross_market_value":
                    float(
                        context.get(
                            "broker_portfolio_exposure",
                            {},
                        ).get(
                            "stock_gross_market_value",
                            0.0,
                        )
                        or 0.0
                    ),

                "broker_stock_gross_pct_equity":
                    float(
                        context.get(
                            "broker_portfolio_exposure",
                            {},
                        ).get(
                            "stock_gross_pct_equity",
                            0.0,
                        )
                        or 0.0
                    ),

                "mode":
                    "ALERT_ONLY_NO_ORDER",
            }

            self.log_message(
                "\n\n"
                "====== MICRO TRADE ALERT ======\n"
                f"BULLISH | "
                f"{payload['underlying']} | "
                "FRACTIONAL STOCK\n"
                f"Lifecycle: {payload['lifecycle_id']} "
                f"[{payload['lifecycle_status']}]\n"
                f"Target notional: "
                f"${payload['target_notional']:.2f}\n"
                f"Approx shares @ ask: "
                f"{payload['approx_shares']:.6f}\n"
                f"Reference bid/ask: "
                f"${payload['reference_bid']:.2f} / "
                f"${payload['reference_ask']:.2f}\n"
                f"Quote age: "
                f"{payload['quote_age_seconds']:.1f}s\n"
                f"Planning stop: "
                f"${payload['stop_price']:.2f}\n"
                f"Planning target: "
                f"${payload['profit_target_price']:.2f}\n"
                f"Planned loss at stop: "
                f"${payload['planned_loss']:.2f} "
                f"("
                f"{payload['planned_risk_pct_equity'] * 100:.3f}% "
                "of effective equity)\n"
                f"Broker stock gross: "
                f"${payload['broker_stock_gross_market_value']:,.2f} "
                f"({payload['broker_stock_gross_pct_equity'] * 100:.2f}% "
                "of effective equity)\n"
                f"Planned gain at target: "
                f"${payload['planned_gain']:.2f}\n"
                f"Stock score: "
                f"{payload['stock_score']:.1f}\n"
                "MODE: ALERT ONLY - NO ORDER SUBMITTED\n"
                "================================"
            )

            self._append_trade_alert_jsonl(
                payload
            )

            if self.parameters[
                "alert_once_per_day"
            ]:

                self._sent_trade_alert_keys.add(
                    key
                )

                self._save_trade_alert_dedupe_state()

            self._register_micro_alert_tracked_setup(
                row,
                key,
                "ALERT_ESTIMATE",
                simulated=bool(
                    context.get(
                        "simulated",
                        False,
                    )
                ),
            )

            alerts.append(
                payload
            )

        self.log_message(
            f"Generated {len(alerts)} "
            "micro fractional-stock alert(s)."
        )

        return alerts


    # ======================================================
    # MICRO ACCOUNT PIPELINE
    # ======================================================

    def run_micro_account_mode(
        self,
        bullish,
        context,
    ):

        self.log_message(
            "Routing this account through "
            "long-only fractional-stock mode. "
            "Options entries are disabled while "
            "micro mode is active."
        )

        sized = (
            self.size_micro_fractional_candidates(
                bullish,
                context,
            )
        )

        self.log_micro_fractional_sizing(
            sized,
            context,
        )

        alerts = (
            self.generate_micro_fractional_alerts(
                sized,
                context,
            )
        )

        return (
            sized,
            alerts,
        )


    # ======================================================
    # ACCOUNT RISK SNAPSHOT
    # ======================================================

    def _get_account_risk_snapshot(
        self,
    ):

        account = (
            self.alpaca_trading_client
            .get_account()
        )

        def safe_float(
            value,
        ):

            try:

                return float(
                    value
                    or 0
                )

            except (
                TypeError,
                ValueError,
            ):

                return 0.0

        return {
            "equity":
                safe_float(
                    getattr(
                        account,
                        "equity",
                        None,
                    )
                ),

            "cash":
                safe_float(
                    getattr(
                        account,
                        "cash",
                        None,
                    )
                ),

            "buying_power":
                safe_float(
                    getattr(
                        account,
                        "buying_power",
                        None,
                    )
                ),

            "options_buying_power":
                safe_float(
                    getattr(
                        account,
                        "options_buying_power",
                        None,
                    )
                ),

            "trading_blocked":
                bool(
                    getattr(
                        account,
                        "trading_blocked",
                        False,
                    )
                ),

            "account_blocked":
                bool(
                    getattr(
                        account,
                        "account_blocked",
                        False,
                    )
                ),

            "trade_suspended_by_user":
                bool(
                    getattr(
                        account,
                        "trade_suspended_by_user",
                        False,
                    )
                ),
        }


    # ======================================================
    # PORTFOLIO EXPOSURE SNAPSHOT
    # ======================================================

    @staticmethod
    def _option_underlying_from_occ_symbol(
        symbol,
    ):
        """
        Extract the underlying from a standard OCC option symbol.

        Alpaca option position symbols use the standard compact
        format such as AAPL260918C00200000. Return an empty string
        when a symbol does not match that format.
        """

        match = re.fullmatch(
            r"([A-Z]{1,6})"
            r"(\d{6})"
            r"([CP])"
            r"(\d{8})",
            str(symbol or "").upper(),
        )

        if match is None:
            return ""

        return match.group(1)


    # ======================================================
    # PERSISTENT TRADE LIFECYCLE + BROKER RECONCILIATION
    # ======================================================

    @staticmethod
    def _broker_field(
        item,
        name,
        default=None,
    ):

        if isinstance(
            item,
            dict,
        ):

            return item.get(
                name,
                default,
            )

        return getattr(
            item,
            name,
            default,
        )


    @staticmethod
    def _enum_text(
        value,
    ):

        if value is None:
            return ""

        return str(
            getattr(
                value,
                "value",
                value,
            )
            or ""
        ).strip().lower()


    @staticmethod
    def _parse_lifecycle_datetime(
        value,
    ):

        if value is None:
            return None

        if isinstance(
            value,
            datetime,
        ):

            parsed = value

        else:

            try:

                parsed = datetime.fromisoformat(
                    str(value).replace(
                        "Z",
                        "+00:00",
                    )
                )

            except ValueError:

                return None

        if parsed.tzinfo is None:

            parsed = parsed.replace(
                tzinfo=ZoneInfo(
                    "America/New_York"
                )
            )

        return parsed


    @staticmethod
    def _lifecycle_terminal_statuses():

        return {
            "CLOSED",
            "CANCELED",
            "REJECTED",
            "EXPIRED",
            "STALE_UNCONFIRMED",
        }


    @staticmethod
    def _lifecycle_active_statuses():

        return {
            "ALERTED",
            "ENTRY_WORKING",
            "PARTIALLY_OPEN",
            "OPEN",
            "CLOSE_ALERTED",
            "CLOSE_WORKING",
            "PARTIALLY_CLOSED",
            "ORPHANED",
        }


    @staticmethod
    def _lifecycle_broker_reserved_statuses():

        # These states have broker/order evidence or represent
        # unresolved broker exposure. Their risk reservation never
        # expires merely because the original alert is old.
        return {
            "ENTRY_WORKING",
            "PARTIALLY_OPEN",
            "OPEN",
            "CLOSE_ALERTED",
            "CLOSE_WORKING",
            "PARTIALLY_CLOSED",
            "ORPHANED",
        }


    @staticmethod
    def _lifecycle_exit_managed_statuses():

        return {
            "PARTIALLY_OPEN",
            "OPEN",
            "CLOSE_ALERTED",
            "CLOSE_WORKING",
            "PARTIALLY_CLOSED",
        }


    @staticmethod
    def _normalize_lifecycle_status(
        status,
    ):

        text = str(
            status
            or "ALERTED"
        ).strip().upper()

        # Backward compatibility with the pre-reconciliation
        # ledger. TRACKING meant "alert emitted and monitored",
        # but it did not prove that a broker fill existed.
        if text == "TRACKING":
            return "ALERTED"

        return text


    def _refresh_position_risk_reservation(
        self,
        position,
        broker_evidence_complete=None,
        now=None,
        record_event=True,
    ):
        """
        Refresh the independent portfolio-risk reservation state.

        Lifecycle status and risk reservation are deliberately
        separate. An unmatched ALERTED record may remain in the
        persistent audit ledger after its short reservation window
        expires. Broker/order-confirmed states always reserve risk.
        """

        asset_type = str(
            position.get(
                "asset_type",
                "OPTION",
            )
            or "OPTION"
        ).upper()

        status = self._normalize_lifecycle_status(
            position.get(
                "status",
                "ALERTED",
            )
        )

        if now is None:
            now = self.get_datetime()

        if now.tzinfo is None:
            now = now.replace(
                tzinfo=ZoneInfo(
                    "America/New_York"
                )
            )

        prior_active_raw = position.get(
            "risk_reservation_active",
            None,
        )
        prior_status = str(
            position.get(
                "risk_reservation_status",
                "",
            )
            or ""
        ).upper()

        if asset_type != "OPTION":
            active = False
            reservation_status = "NOT_APPLICABLE"
            reason = (
                "Option portfolio-risk reservation does not apply "
                "to this lifecycle asset type"
            )
            deadline = None

        else:
            deadline = self._parse_lifecycle_datetime(
                position.get(
                    "risk_reservation_expires_at",
                    None,
                )
            )

            if deadline is None:
                entry_time = self._parse_lifecycle_datetime(
                    position.get(
                        "entry_timestamp",
                        None,
                    )
                )

                if entry_time is not None:
                    try:
                        minutes = max(
                            0.0,
                            float(
                                self.parameters.get(
                                    "lifecycle_alert_risk_reservation_minutes",
                                    60.0,
                                )
                            ),
                        )
                    except (
                        TypeError,
                        ValueError,
                    ):
                        minutes = 60.0

                    deadline = (
                        entry_time
                        + timedelta(
                            minutes=minutes
                        )
                    )

                    position[
                        "risk_reservation_expires_at"
                    ] = deadline.isoformat()

            if broker_evidence_complete is not None:
                position[
                    "risk_reservation_evidence_complete"
                ] = bool(
                    broker_evidence_complete
                )

            evidence_complete = bool(
                position.get(
                    "risk_reservation_evidence_complete",
                    False,
                )
            )

            if status in self._lifecycle_terminal_statuses():
                active = False
                reservation_status = "RELEASED_TERMINAL"
                reason = (
                    "Lifecycle is terminal; portfolio-risk "
                    "reservation is released"
                )

            elif status in self._lifecycle_broker_reserved_statuses():
                active = True
                reservation_status = "RESERVED_BROKER_EVIDENCE"
                reason = (
                    "Broker/order-confirmed lifecycle state keeps "
                    "portfolio-risk reservation active"
                )

            elif status == "ALERTED":
                # Once a fully reconciled unmatched alert has been
                # released, a later data outage must not re-reserve it.
                # Actual broker/order evidence will first move the
                # lifecycle into a broker-reserved state above.
                already_released = (
                    prior_status
                    == "RELEASED_ALERT_TIMEOUT"
                    and prior_active_raw is False
                )

                if already_released:
                    active = False
                    reservation_status = "RELEASED_ALERT_TIMEOUT"
                    reason = (
                        "Unmatched alert reservation previously "
                        "expired after complete broker reconciliation"
                    )

                elif deadline is None:
                    active = True
                    reservation_status = "RESERVED_NO_DEADLINE"
                    reason = (
                        "Alert reservation deadline is unavailable; "
                        "holding risk conservatively"
                    )

                elif now < deadline:
                    active = True
                    reservation_status = "RESERVED_ALERT_WINDOW"
                    reason = (
                        "Alert is still inside the configured "
                        "broker-match risk-reservation window"
                    )

                elif evidence_complete:
                    active = False
                    reservation_status = "RELEASED_ALERT_TIMEOUT"
                    reason = (
                        "Alert reservation window expired with no "
                        "matching broker position or order evidence"
                    )

                else:
                    active = True
                    reservation_status = "RESERVED_EVIDENCE_UNCERTAIN"
                    reason = (
                        "Alert reservation window expired, but broker "
                        "position/order evidence is incomplete; risk "
                        "remains reserved conservatively"
                    )

            else:
                # Unknown non-terminal lifecycle states fail safe.
                active = True
                reservation_status = "RESERVED_UNKNOWN_STATE"
                reason = (
                    "Unknown non-terminal lifecycle state; holding "
                    "portfolio-risk reservation conservatively"
                )

        prior_active = (
            bool(prior_active_raw)
            if prior_active_raw is not None
            else None
        )

        changed = (
            prior_active is None
            or prior_active != bool(active)
            or prior_status != reservation_status
        )

        position[
            "risk_reservation_active"
        ] = bool(active)
        position[
            "risk_reservation_status"
        ] = reservation_status
        position[
            "risk_reservation_reason"
        ] = reason
        position[
            "risk_reservation_updated_at"
        ] = now.isoformat()

        if deadline is not None:
            position[
                "risk_reservation_expires_at"
            ] = deadline.isoformat()

        if changed:
            if not active:
                position[
                    "risk_reservation_released_at"
                ] = now.isoformat()
            elif prior_active is False:
                position[
                    "risk_reservation_reactivated_at"
                ] = now.isoformat()

            if record_event:
                if prior_active is True and not active:
                    event_type = "RISK_RESERVATION_RELEASED"
                elif prior_active is False and active:
                    event_type = "RISK_RESERVATION_REACTIVATED"
                else:
                    event_type = "RISK_RESERVATION_UPDATED"

                self._record_lifecycle_event(
                    position,
                    event_type,
                    reason,
                    details={
                        "reservation_status": reservation_status,
                        "reservation_active": bool(active),
                        "reservation_expires_at": (
                            deadline.isoformat()
                            if deadline is not None
                            else ""
                        ),
                        "broker_evidence_complete": (
                            broker_evidence_complete
                        ),
                    },
                )

        return {
            "active": bool(active),
            "status": reservation_status,
            "reason": reason,
            "expires_at": (
                deadline.isoformat()
                if deadline is not None
                else ""
            ),
            "changed": changed,
        }


    def _append_trade_lifecycle_jsonl(
        self,
        payload,
    ):

        path = getattr(
            self,
            "trade_lifecycle_jsonl_path",
            "",
        )

        if not path:
            return

        try:

            with open(
                path,
                "a",
                encoding="utf-8",
            ) as handle:

                handle.write(
                    json.dumps(
                        payload,
                        default=str,
                        sort_keys=True,
                    )
                    + "\n"
                )

        except Exception as exc:

            self.log_message(
                "Could not write trade lifecycle "
                f"JSONL file: {exc}"
            )


    def _record_lifecycle_event(
        self,
        position,
        event_type,
        reason,
        details=None,
    ):

        now_text = (
            self.get_datetime()
            .isoformat()
        )

        history = position.get(
            "lifecycle_history",
            [],
        )

        if not isinstance(
            history,
            list,
        ):

            history = []

        event = {
            "timestamp": now_text,
            "event": str(
                event_type
            ),
            "status": self._normalize_lifecycle_status(
                position.get(
                    "status",
                    "ALERTED",
                )
            ),
            "reason": str(
                reason
            ),
        }

        if details:
            event["details"] = details

        history.append(
            event
        )

        maximum = max(
            1,
            int(
                self.parameters.get(
                    "lifecycle_history_max_events",
                    100,
                )
            ),
        )

        if len(history) > maximum:
            history = history[-maximum:]

        position[
            "lifecycle_history"
        ] = history

        self._append_trade_lifecycle_jsonl(
            {
                "position_id": position.get(
                    "id",
                    "",
                ),
                "underlying": position.get(
                    "underlying",
                    "",
                ),
                "asset_type": position.get(
                    "asset_type",
                    "OPTION",
                ),
                **event,
            }
        )


    def _transition_trade_lifecycle(
        self,
        position,
        new_status,
        reason,
        details=None,
    ):

        old_status = (
            self._normalize_lifecycle_status(
                position.get(
                    "status",
                    "ALERTED",
                )
            )
        )

        new_status = (
            self._normalize_lifecycle_status(
                new_status
            )
        )

        position[
            "lifecycle_version"
        ] = 1

        if old_status == new_status:
            position["status"] = new_status
            return False

        position["status"] = new_status
        position[
            "status_updated_at"
        ] = (
            self.get_datetime()
            .isoformat()
        )

        if (
            new_status
            in self._lifecycle_terminal_statuses()
        ):

            position[
                "terminal_at"
            ] = position[
                "status_updated_at"
            ]

        history = position.get(
            "lifecycle_history",
            [],
        )

        if not isinstance(
            history,
            list,
        ):

            history = []

        event = {
            "timestamp": position[
                "status_updated_at"
            ],
            "event": "STATUS_TRANSITION",
            "from_status": old_status,
            "to_status": new_status,
            "status": new_status,
            "reason": str(
                reason
            ),
        }

        if details:
            event["details"] = details

        history.append(
            event
        )

        maximum = max(
            1,
            int(
                self.parameters.get(
                    "lifecycle_history_max_events",
                    100,
                )
            ),
        )

        if len(history) > maximum:
            history = history[-maximum:]

        position[
            "lifecycle_history"
        ] = history

        self._append_trade_lifecycle_jsonl(
            {
                "position_id": position.get(
                    "id",
                    "",
                ),
                "underlying": position.get(
                    "underlying",
                    "",
                ),
                "asset_type": position.get(
                    "asset_type",
                    "OPTION",
                ),
                **event,
            }
        )

        return True


    def _position_signed_quantity_map(
        self,
        positions,
    ):

        signed_by_symbol = {}
        details = {}

        for position in (
            positions
            or []
        ):

            symbol = str(
                self._broker_field(
                    position,
                    "symbol",
                    "",
                )
                or ""
            ).upper()

            if not symbol:
                continue

            try:

                raw_qty = float(
                    self._broker_field(
                        position,
                        "qty",
                        0.0,
                    )
                    or 0.0
                )

            except (
                TypeError,
                ValueError,
            ):

                raw_qty = 0.0

            side = self._enum_text(
                self._broker_field(
                    position,
                    "side",
                    "",
                )
            )

            absolute_qty = abs(
                raw_qty
            )

            if side == "short":
                signed_qty = -absolute_qty
            elif side == "long":
                signed_qty = absolute_qty
            else:
                signed_qty = raw_qty

            signed_by_symbol[
                symbol
            ] = (
                signed_by_symbol.get(
                    symbol,
                    0.0,
                )
                + signed_qty
            )

            details[symbol] = {
                "signed_qty": signed_by_symbol[
                    symbol
                ],
                "side": side,
                "avg_entry_price": self._broker_field(
                    position,
                    "avg_entry_price",
                    None,
                ),
                "market_value": self._broker_field(
                    position,
                    "market_value",
                    None,
                ),
            }

        return (
            signed_by_symbol,
            details,
        )


    def _normalize_broker_orders(
        self,
        orders,
    ):

        normalized = []

        for order in (
            orders
            or []
        ):

            top_status = self._enum_text(
                self._broker_field(
                    order,
                    "status",
                    "",
                )
            )

            raw_legs = self._broker_field(
                order,
                "legs",
                None,
            )

            if raw_legs:
                leg_items = list(
                    raw_legs
                )
            else:
                leg_items = [
                    order
                ]

            legs = []

            for leg in leg_items:

                symbol = str(
                    self._broker_field(
                        leg,
                        "symbol",
                        "",
                    )
                    or ""
                ).upper()

                if not symbol:
                    continue

                try:

                    qty = float(
                        self._broker_field(
                            leg,
                            "qty",
                            0.0,
                        )
                        or 0.0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    qty = 0.0

                try:

                    filled_qty = float(
                        self._broker_field(
                            leg,
                            "filled_qty",
                            0.0,
                        )
                        or 0.0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    filled_qty = 0.0

                legs.append(
                    {
                        "symbol": symbol,
                        "side": self._enum_text(
                            self._broker_field(
                                leg,
                                "side",
                                "",
                            )
                        ),
                        "position_intent": self._enum_text(
                            self._broker_field(
                                leg,
                                "position_intent",
                                "",
                            )
                        ),
                        "qty": qty,
                        "filled_qty": filled_qty,
                        "filled_avg_price": self._broker_field(
                            leg,
                            "filled_avg_price",
                            None,
                        ),
                        "status": (
                            self._enum_text(
                                self._broker_field(
                                    leg,
                                    "status",
                                    "",
                                )
                            )
                            or top_status
                        ),
                    }
                )

            try:

                top_filled_qty = float(
                    self._broker_field(
                        order,
                        "filled_qty",
                        0.0,
                    )
                    or 0.0
                )

            except (
                TypeError,
                ValueError,
            ):

                top_filled_qty = 0.0

            normalized.append(
                {
                    "id": str(
                        self._broker_field(
                            order,
                            "id",
                            "",
                        )
                        or ""
                    ),
                    "client_order_id": str(
                        self._broker_field(
                            order,
                            "client_order_id",
                            "",
                        )
                        or ""
                    ),
                    "status": top_status,
                    "submitted_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "submitted_at",
                            None,
                        )
                    ),
                    "filled_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "filled_at",
                            None,
                        )
                    ),
                    "filled_qty": top_filled_qty,
                    "filled_avg_price": self._broker_field(
                        order,
                        "filled_avg_price",
                        None,
                    ),
                    "legs": legs,
                    "symbols": sorted(
                        {
                            leg[
                                "symbol"
                            ]
                            for leg in legs
                        }
                    ),
                }
            )

        return normalized


    def _expected_lifecycle_legs(
        self,
        position,
        close=False,
    ):

        asset_type = str(
            position.get(
                "asset_type",
                "OPTION",
            )
            or "OPTION"
        ).upper()

        if asset_type == "FRACTIONAL_STOCK":

            symbol = str(
                position.get(
                    "underlying",
                    "",
                )
                or ""
            ).upper()

            return [
                {
                    "symbol": symbol,
                    "side": (
                        "sell"
                        if close
                        else "buy"
                    ),
                    "intent": "",
                }
            ] if symbol else []

        long_symbol = str(
            position.get(
                "long_contract",
                "",
            )
            or ""
        ).upper()

        short_symbol = str(
            position.get(
                "short_contract",
                "",
            )
            or ""
        ).upper()

        legs = []

        if long_symbol:

            legs.append(
                {
                    "symbol": long_symbol,
                    "side": (
                        "sell"
                        if close
                        else "buy"
                    ),
                    "intent": (
                        "sell_to_close"
                        if close
                        else "buy_to_open"
                    ),
                }
            )

        if short_symbol:

            legs.append(
                {
                    "symbol": short_symbol,
                    "side": (
                        "buy"
                        if close
                        else "sell"
                    ),
                    "intent": (
                        "buy_to_close"
                        if close
                        else "sell_to_open"
                    ),
                }
            )

        return legs


    def _order_matches_lifecycle_legs(
        self,
        order,
        expected_legs,
    ):

        expected_symbols = {
            leg[
                "symbol"
            ]
            for leg in expected_legs
        }

        order_symbols = set(
            order.get(
                "symbols",
                [],
            )
        )

        if (
            not expected_symbols
            or order_symbols
            != expected_symbols
        ):

            return False

        order_legs = order.get(
            "legs",
            [],
        )

        for expected in expected_legs:

            matches = [
                leg
                for leg in order_legs
                if leg.get(
                    "symbol"
                )
                == expected[
                    "symbol"
                ]
            ]

            if not matches:
                return False

            leg_match = False

            for actual in matches:

                intent = str(
                    actual.get(
                        "position_intent",
                        "",
                    )
                    or ""
                ).lower()

                side = str(
                    actual.get(
                        "side",
                        "",
                    )
                    or ""
                ).lower()

                expected_intent = str(
                    expected.get(
                        "intent",
                        "",
                    )
                    or ""
                ).lower()

                if (
                    expected_intent
                    and intent
                ):

                    leg_match = (
                        intent
                        == expected_intent
                    )

                else:

                    leg_match = (
                        side
                        == expected[
                            "side"
                        ]
                    )

                if leg_match:
                    break

            if not leg_match:
                return False

        return True


    def _matching_lifecycle_orders(
        self,
        position,
        normalized_orders,
        close=False,
    ):

        expected_legs = (
            self._expected_lifecycle_legs(
                position,
                close=close,
            )
        )

        entry_time = (
            self._parse_lifecycle_datetime(
                position.get(
                    "entry_timestamp",
                    None,
                )
            )
        )

        if entry_time is not None:

            earliest = (
                entry_time
                - timedelta(
                    minutes=10
                )
            )

        else:
            earliest = None

        matches = []

        for order in normalized_orders:

            submitted_at = order.get(
                "submitted_at"
            )

            if (
                earliest is not None
                and submitted_at is not None
                and submitted_at < earliest
            ):

                continue

            if self._order_matches_lifecycle_legs(
                order,
                expected_legs,
            ):

                matches.append(
                    order
                )

        matches.sort(
            key=lambda row: (
                row.get(
                    "submitted_at"
                )
                or datetime.min.replace(
                    tzinfo=timezone.utc
                )
            )
        )

        return matches


    @staticmethod
    def _select_lifecycle_order_evidence(
        orders,
    ):

        working_statuses = {
            "new",
            "accepted",
            "pending_new",
            "accepted_for_bidding",
            "pending_review",
            "held",
            "pending_replace",
            "pending_cancel",
        }

        terminal_statuses = {
            "canceled",
            "expired",
            "rejected",
            "done_for_day",
            "stopped",
            "suspended",
            "replaced",
        }

        result = {
            "latest": None,
            "filled": None,
            "partial": None,
            "working": None,
            "terminal": None,
        }

        for order in orders:

            result[
                "latest"
            ] = order

            status = str(
                order.get(
                    "status",
                    "",
                )
                or ""
            ).lower()

            filled_qty = float(
                order.get(
                    "filled_qty",
                    0.0,
                )
                or 0.0
            )

            if status == "filled":
                result["filled"] = order

            elif (
                status
                == "partially_filled"
                or filled_qty > 0
            ):

                result["partial"] = order

            if status in working_statuses:
                result["working"] = order

            if status in terminal_statuses:
                result["terminal"] = order

        return result


    def _get_broker_lifecycle_snapshot(
        self,
    ):

        snapshot = {
            "positions_available": False,
            "orders_available": False,
            "orders_truncated": False,
            "positions_error": "",
            "orders_error": "",
            "positions": [],
            "orders": [],
            "signed_qty_by_symbol": {},
            "position_details": {},
            "normalized_orders": [],
        }

        try:

            positions = (
                self.alpaca_trading_client
                .get_all_positions()
            )

            if positions is None:
                positions = []

            if isinstance(
                positions,
                dict,
            ):

                positions = (
                    positions.get(
                        "positions"
                    )
                    or positions.get(
                        "data"
                    )
                    or []
                )

            if not isinstance(
                positions,
                (list, tuple),
            ):

                raise TypeError(
                    "Unexpected broker positions response type"
                )

            snapshot[
                "positions"
            ] = list(
                positions
            )

            (
                signed_qty_by_symbol,
                position_details,
            ) = self._position_signed_quantity_map(
                snapshot[
                    "positions"
                ]
            )

            snapshot[
                "signed_qty_by_symbol"
            ] = signed_qty_by_symbol

            snapshot[
                "position_details"
            ] = position_details

            snapshot[
                "positions_available"
            ] = True

        except Exception as exc:

            snapshot[
                "positions_error"
            ] = str(exc)

        if not self._tracked_alert_positions:

            snapshot[
                "orders_available"
            ] = True

            return snapshot

        try:

            now = self.get_datetime()

            if now.tzinfo is None:

                now = now.replace(
                    tzinfo=ZoneInfo(
                        "America/New_York"
                    )
                )

            now_utc = now.astimezone(
                timezone.utc
            )

            lookback_days = max(
                1,
                int(
                    self.parameters.get(
                        "lifecycle_order_lookback_days",
                        90,
                    )
                ),
            )

            after = (
                now_utc
                - timedelta(
                    days=lookback_days
                )
            )

            active_entry_times = []

            for position in (
                self._tracked_alert_positions
                .values()
            ):

                status = (
                    self._normalize_lifecycle_status(
                        position.get(
                            "status",
                            "ALERTED",
                        )
                    )
                )

                if (
                    status
                    in self._lifecycle_terminal_statuses()
                ):

                    continue

                parsed = (
                    self._parse_lifecycle_datetime(
                        position.get(
                            "entry_timestamp",
                            None,
                        )
                    )
                )

                if parsed is not None:

                    active_entry_times.append(
                        parsed.astimezone(
                            timezone.utc
                        )
                    )

            if active_entry_times:

                oldest_active = min(
                    active_entry_times
                )

                after = max(
                    after,
                    oldest_active
                    - timedelta(
                        days=1
                    ),
                )

            limit = min(
                500,
                max(
                    1,
                    int(
                        self.parameters.get(
                            "lifecycle_order_limit",
                            500,
                        )
                    ),
                ),
            )

            request = GetOrdersRequest(
                status=QueryOrderStatus.ALL,
                limit=limit,
                after=after,
                nested=True,
            )

            orders = (
                self.alpaca_trading_client
                .get_orders(
                    filter=request
                )
            )

            if orders is None:
                orders = []

            if isinstance(
                orders,
                dict,
            ):

                orders = (
                    orders.get(
                        "orders"
                    )
                    or orders.get(
                        "data"
                    )
                    or []
                )

            if not isinstance(
                orders,
                (list, tuple),
            ):

                raise TypeError(
                    "Unexpected broker orders response type"
                )

            snapshot[
                "orders"
            ] = list(
                orders
            )

            snapshot[
                "normalized_orders"
            ] = self._normalize_broker_orders(
                snapshot[
                    "orders"
                ]
            )

            snapshot[
                "orders_truncated"
            ] = (
                len(
                    snapshot[
                        "orders"
                    ]
                )
                >= limit
            )

            snapshot[
                "orders_available"
            ] = True

        except Exception as exc:

            snapshot[
                "orders_error"
            ] = str(exc)

        return snapshot


    def _reconcile_single_trade_lifecycle(
        self,
        position,
        snapshot,
    ):

        prior_status = (
            self._normalize_lifecycle_status(
                position.get(
                    "status",
                    "ALERTED",
                )
            )
        )

        position[
            "status"
        ] = prior_status

        position.setdefault(
            "asset_type",
            "OPTION",
        )

        position.setdefault(
            "lifecycle_version",
            1,
        )

        now = self.get_datetime()

        if (
            str(
                position.get(
                    "asset_type",
                    "OPTION",
                )
                or "OPTION"
            ).upper()
            == "FRACTIONAL_STOCK"
            and str(
                position.get(
                    "sizing_basis",
                    "",
                )
                or ""
            ).upper()
            == "SIMULATED_MICRO_EQUITY"
        ):

            position[
                "last_reconciliation_at"
            ] = now.isoformat()

            position[
                "broker_reconciliation_state"
            ] = "SIMULATION_SKIPPED"

            position[
                "broker_reconciliation_note"
            ] = (
                "Simulated micro lifecycle is intentionally "
                "not matched to the real/paper broker book"
            )

            return {
                "position_id": position.get(
                    "id",
                    "",
                ),
                "asset_type": "FRACTIONAL_STOCK",
                "underlying": position.get(
                    "underlying",
                    "",
                ),
                "prior_status": prior_status,
                "status": prior_status,
                "broker_open_qty": 0.0,
                "entry_order_status": "SKIPPED",
                "close_order_status": "SKIPPED",
                "match_confidence": "SIMULATION_SKIPPED",
                "reason": position[
                    "broker_reconciliation_note"
                ],
            }

        if (
            prior_status
            in self._lifecycle_terminal_statuses()
        ):

            position[
                "last_reconciliation_at"
            ] = now.isoformat()

            position[
                "broker_reconciliation_state"
            ] = "TERMINAL_LOCKED"

            position[
                "broker_reconciliation_note"
            ] = (
                "Terminal lifecycle states are immutable; "
                "new broker exposure requires a new alert lifecycle"
            )

            return {
                "position_id": position.get(
                    "id",
                    "",
                ),
                "asset_type": position.get(
                    "asset_type",
                    "OPTION",
                ),
                "underlying": position.get(
                    "underlying",
                    "",
                ),
                "prior_status": prior_status,
                "status": prior_status,
                "broker_open_qty": position.get(
                    "broker_open_quantity",
                    0.0,
                ),
                "entry_order_status": "LOCKED",
                "close_order_status": "LOCKED",
                "match_confidence": "TERMINAL_LOCKED",
                "reason": position[
                    "broker_reconciliation_note"
                ],
            }

        position[
            "last_reconciliation_at"
        ] = now.isoformat()

        if (
            self.parameters.get(
                "lifecycle_require_positions_snapshot",
                True,
            )
            and not snapshot[
                "positions_available"
            ]
        ):

            position[
                "broker_reconciliation_state"
            ] = "POSITIONS_UNKNOWN"

            position[
                "broker_reconciliation_note"
            ] = (
                "Broker positions unavailable; "
                "prior lifecycle status retained"
            )

            return {
                "position_id": position.get(
                    "id",
                    "",
                ),
                "asset_type": position.get(
                    "asset_type",
                    "OPTION",
                ),
                "underlying": position.get(
                    "underlying",
                    "",
                ),
                "prior_status": prior_status,
                "status": prior_status,
                "broker_open_qty": position.get(
                    "broker_open_quantity",
                    0.0,
                ),
                "entry_order_status": "UNKNOWN",
                "close_order_status": "UNKNOWN",
                "match_confidence": "UNKNOWN",
                "reason": position[
                    "broker_reconciliation_note"
                ],
            }

        signed_qty = snapshot[
            "signed_qty_by_symbol"
        ]

        expected_legs = (
            self._expected_lifecycle_legs(
                position,
                close=False,
            )
        )

        asset_type = str(
            position.get(
                "asset_type",
                "OPTION",
            )
            or "OPTION"
        ).upper()

        if asset_type == "FRACTIONAL_STOCK":

            expected_quantity = float(
                position.get(
                    "approx_shares",
                    0.0,
                )
                or 0.0
            )

        else:

            expected_quantity = float(
                position.get(
                    "quantity",
                    0.0,
                )
                or 0.0
            )

        tolerance = max(
            0.0,
            float(
                self.parameters.get(
                    "lifecycle_quantity_tolerance",
                    1e-6,
                )
            ),
        )

        leg_quantities = {}
        correct_quantities = []
        any_expected_symbol_position = False
        wrong_side = False

        for leg in expected_legs:

            symbol = leg[
                "symbol"
            ]

            actual = float(
                signed_qty.get(
                    symbol,
                    0.0,
                )
                or 0.0
            )

            desired_sign = (
                1.0
                if leg[
                    "side"
                ] == "buy"
                else -1.0
            )

            if abs(actual) > tolerance:
                any_expected_symbol_position = True

            if desired_sign > 0:
                correct = max(
                    actual,
                    0.0,
                )
            else:
                correct = max(
                    -actual,
                    0.0,
                )

            if (
                abs(actual) > tolerance
                and correct <= tolerance
            ):
                wrong_side = True

            leg_quantities[
                symbol
            ] = actual

            correct_quantities.append(
                correct
            )

        if correct_quantities:

            broker_open_quantity = min(
                correct_quantities
            )

            maximum_correct_quantity = max(
                correct_quantities
            )

        else:

            broker_open_quantity = 0.0
            maximum_correct_quantity = 0.0

        partial_leg_exposure = (
            maximum_correct_quantity
            > tolerance
            and broker_open_quantity
            <= tolerance
        )

        prior_peak = float(
            position.get(
                "broker_peak_open_quantity",
                0.0,
            )
            or 0.0
        )

        broker_peak = max(
            prior_peak,
            broker_open_quantity,
        )

        position[
            "broker_open_quantity"
        ] = broker_open_quantity

        position[
            "broker_peak_open_quantity"
        ] = broker_peak

        position[
            "broker_leg_signed_quantities"
        ] = leg_quantities

        if asset_type == "FRACTIONAL_STOCK":

            full_threshold = (
                expected_quantity
                * 0.90
            )

        else:
            full_threshold = expected_quantity

        full_position_match = (
            expected_quantity > tolerance
            and broker_open_quantity
            + tolerance
            >= full_threshold
        )

        entry_orders = []
        close_orders = []

        if snapshot[
            "orders_available"
        ]:

            entry_orders = (
                self._matching_lifecycle_orders(
                    position,
                    snapshot[
                        "normalized_orders"
                    ],
                    close=False,
                )
            )

            close_orders = (
                self._matching_lifecycle_orders(
                    position,
                    snapshot[
                        "normalized_orders"
                    ],
                    close=True,
                )
            )

        entry_evidence = (
            self._select_lifecycle_order_evidence(
                entry_orders
            )
        )

        close_evidence = (
            self._select_lifecycle_order_evidence(
                close_orders
            )
        )

        def evidence_status(
            evidence,
        ):

            latest = evidence.get(
                "latest"
            )

            return (
                str(
                    latest.get(
                        "status",
                        "",
                    )
                    or ""
                ).upper()
                if latest
                else "NONE"
            )

        def persist_order_evidence(
            prefix,
            evidence,
        ):

            latest = evidence.get(
                "latest"
            )

            if latest is None:
                return

            position[
                f"broker_{prefix}_order_id"
            ] = latest.get(
                "id",
                "",
            )

            position[
                f"broker_{prefix}_client_order_id"
            ] = latest.get(
                "client_order_id",
                "",
            )

            position[
                f"broker_{prefix}_order_status"
            ] = latest.get(
                "status",
                "",
            )

            position[
                f"broker_{prefix}_filled_qty"
            ] = latest.get(
                "filled_qty",
                0.0,
            )

            position[
                f"broker_{prefix}_filled_avg_price"
            ] = latest.get(
                "filled_avg_price",
                None,
            )

        persist_order_evidence(
            "entry",
            entry_evidence,
        )

        persist_order_evidence(
            "close",
            close_evidence,
        )

        previously_open = (
            prior_peak > tolerance
            or prior_status
            in {
                "OPEN",
                "CLOSE_ALERTED",
                "CLOSE_WORKING",
                "PARTIALLY_CLOSED",
            }
        )

        new_status = prior_status
        reason = "No lifecycle change"
        match_confidence = "NONE"

        if wrong_side:

            new_status = "ORPHANED"
            reason = (
                "Broker holds an expected symbol on the "
                "opposite side"
            )
            match_confidence = "POSITION_CONFLICT"

        elif partial_leg_exposure:

            new_status = (
                "ORPHANED"
                if previously_open
                else "PARTIALLY_OPEN"
            )

            reason = (
                "Only part of the expected multi-leg "
                "broker position is present"
            )
            match_confidence = "POSITION_PARTIAL_LEGS"

        elif broker_open_quantity > tolerance:

            match_confidence = (
                "POSITION_FULL"
                if full_position_match
                else "POSITION_PARTIAL"
            )

            close_working = (
                close_evidence.get(
                    "working"
                )
                is not None
            )

            close_filled = (
                close_evidence.get(
                    "filled"
                )
                is not None
            )

            if full_position_match:

                if (
                    close_filled
                    and previously_open
                ):

                    new_status = "ORPHANED"
                    reason = (
                        "Matching close order reports FILLED but "
                        "the full broker position is still present"
                    )
                    match_confidence = (
                        "ORDER_POSITION_CONFLICT"
                    )

                elif (
                    close_working
                    and previously_open
                ):

                    new_status = "CLOSE_WORKING"
                    reason = (
                        "Broker position remains open and a "
                        "matching close order is working"
                    )

                elif (
                    prior_status
                    in {
                        "CLOSE_ALERTED",
                        "CLOSE_WORKING",
                    }
                ):

                    new_status = "CLOSE_ALERTED"
                    reason = (
                        "Broker position remains open after "
                        "a close alert"
                    )

                else:
                    new_status = "OPEN"
                    reason = (
                        "Expected broker position is present"
                    )

            else:

                if previously_open:
                    new_status = "PARTIALLY_CLOSED"
                    reason = (
                        "Broker position quantity is below the "
                        "previously open quantity"
                    )
                else:
                    new_status = "PARTIALLY_OPEN"
                    reason = (
                        "Broker position is present below the "
                        "expected alert quantity"
                    )

        elif any_expected_symbol_position:

            new_status = "ORPHANED"
            reason = (
                "Expected broker symbols exist but do not form "
                "the intended position"
            )
            match_confidence = "POSITION_CONFLICT"

        elif previously_open:

            new_status = "CLOSED"
            reason = (
                "Previously broker-matched exposure is no longer "
                "present in the current position snapshot"
            )
            match_confidence = "POSITION_CLOSED"

        else:

            entry_filled = entry_evidence.get(
                "filled"
            )

            entry_partial = entry_evidence.get(
                "partial"
            )

            entry_working = entry_evidence.get(
                "working"
            )

            entry_terminal = entry_evidence.get(
                "terminal"
            )

            close_filled = close_evidence.get(
                "filled"
            )

            if (
                entry_filled is not None
                and close_filled is not None
            ):

                new_status = "CLOSED"
                reason = (
                    "Matching entry and close orders are filled "
                    "and no broker position remains"
                )
                match_confidence = "ORDER_ROUND_TRIP"

            elif entry_filled is not None:

                new_status = "ORPHANED"
                reason = (
                    "Matching entry order reports FILLED but no "
                    "current broker position is present"
                )
                match_confidence = "ORDER_POSITION_CONFLICT"

            elif entry_partial is not None:

                new_status = "ORPHANED"
                reason = (
                    "Matching entry order reports a partial fill "
                    "but no current broker position is present"
                )
                match_confidence = "ORDER_POSITION_CONFLICT"

            elif entry_working is not None:

                new_status = "ENTRY_WORKING"
                reason = (
                    "Matching broker entry order is working"
                )
                match_confidence = "ORDER_ONLY"

            elif entry_terminal is not None:

                status = str(
                    entry_terminal.get(
                        "status",
                        "",
                    )
                    or ""
                ).lower()

                filled_qty = float(
                    entry_terminal.get(
                        "filled_qty",
                        0.0,
                    )
                    or 0.0
                )

                if filled_qty <= tolerance:

                    if status == "rejected":
                        new_status = "REJECTED"
                    elif status == "expired":
                        new_status = "EXPIRED"
                    else:
                        new_status = "CANCELED"

                    reason = (
                        "Matching broker entry order reached "
                        f"terminal status {status or 'unknown'} "
                        "without a fill"
                    )
                    match_confidence = "ORDER_ONLY"

            if new_status == prior_status:

                expiration_text = str(
                    position.get(
                        "expiration",
                        "",
                    )
                    or ""
                )

                expiration_date = None

                if expiration_text:

                    try:

                        expiration_date = (
                            date.fromisoformat(
                                expiration_text
                            )
                        )

                    except ValueError:
                        expiration_date = None

                if (
                    asset_type == "OPTION"
                    and expiration_date is not None
                    and expiration_date
                    < now.date()
                ):

                    new_status = "EXPIRED"
                    reason = (
                        "Option expiration has passed and no "
                        "matching option position remains"
                    )
                    match_confidence = "EXPIRATION_PLUS_POSITION"

                else:

                    entry_time = (
                        self._parse_lifecycle_datetime(
                            position.get(
                                "entry_timestamp",
                                None,
                            )
                        )
                    )

                    age_days = 0

                    if entry_time is not None:

                        age_days = max(
                            0,
                            (
                                now.date()
                                - entry_time.date()
                            ).days,
                        )

                    grace_days = max(
                        0,
                        int(
                            self.parameters.get(
                                "lifecycle_alert_match_grace_days",
                                2,
                            )
                        ),
                    )

                    order_history_safe = (
                        snapshot[
                            "orders_available"
                        ]
                        and not snapshot[
                            "orders_truncated"
                        ]
                    )

                    require_orders = bool(
                        self.parameters.get(
                            "lifecycle_require_orders_for_terminal_inference",
                            True,
                        )
                    )

                    if (
                        age_days
                        > grace_days
                        and (
                            order_history_safe
                            or not require_orders
                        )
                    ):

                        new_status = (
                            "STALE_UNCONFIRMED"
                        )
                        reason = (
                            "Alert remained unmatched beyond the "
                            "broker-match grace period"
                        )
                        match_confidence = "NO_BROKER_MATCH"

                    else:

                        new_status = "ALERTED"
                        reason = (
                            "No broker position/order match yet; "
                            "alert remains inside reconciliation "
                            "grace or order evidence is incomplete"
                        )

        position[
            "broker_reconciliation_state"
        ] = match_confidence

        position[
            "broker_reconciliation_note"
        ] = reason

        position[
            "broker_orders_snapshot_available"
        ] = bool(
            snapshot[
                "orders_available"
            ]
        )

        position[
            "broker_orders_snapshot_truncated"
        ] = bool(
            snapshot[
                "orders_truncated"
            ]
        )

        self._transition_trade_lifecycle(
            position,
            new_status,
            reason,
            details={
                "broker_open_quantity": (
                    broker_open_quantity
                ),
                "expected_quantity": (
                    expected_quantity
                ),
                "entry_order_status": (
                    evidence_status(
                        entry_evidence
                    )
                ),
                "close_order_status": (
                    evidence_status(
                        close_evidence
                    )
                ),
                "match_confidence": (
                    match_confidence
                ),
            },
        )

        return {
            "position_id": position.get(
                "id",
                "",
            ),
            "asset_type": asset_type,
            "underlying": position.get(
                "underlying",
                "",
            ),
            "prior_status": prior_status,
            "status": position[
                "status"
            ],
            "broker_open_qty": (
                broker_open_quantity
            ),
            "entry_order_status": (
                evidence_status(
                    entry_evidence
                )
            ),
            "close_order_status": (
                evidence_status(
                    close_evidence
                )
            ),
            "match_confidence": (
                match_confidence
            ),
            "reason": reason,
        }


    def reconcile_trade_lifecycle_states(
        self,
    ):

        if not self.parameters.get(
            "lifecycle_reconciliation_enabled",
            True,
        ):

            return pd.DataFrame(), {
                "positions_available": False,
                "orders_available": False,
                "disabled": True,
            }

        if not self._tracked_alert_positions:

            return pd.DataFrame(), {
                "positions_available": True,
                "orders_available": True,
                "empty": True,
            }

        snapshot = (
            self._get_broker_lifecycle_snapshot()
        )

        rows = []

        changed = False

        for position_id, position in list(
            self._tracked_alert_positions.items()
        ):

            old_status = (
                self._normalize_lifecycle_status(
                    position.get(
                        "status",
                        "ALERTED",
                    )
                )
            )

            row = (
                self._reconcile_single_trade_lifecycle(
                    position,
                    snapshot,
                )
            )

            broker_evidence_complete = (
                bool(
                    snapshot.get(
                        "positions_available",
                        False,
                    )
                )
                and bool(
                    snapshot.get(
                        "orders_available",
                        False,
                    )
                )
                and not bool(
                    snapshot.get(
                        "orders_truncated",
                        False,
                    )
                )
            )

            reservation = (
                self._refresh_position_risk_reservation(
                    position,
                    broker_evidence_complete=(
                        broker_evidence_complete
                    ),
                    record_event=True,
                )
            )

            row[
                "risk_reserved"
            ] = reservation[
                "active"
            ]
            row[
                "reservation_status"
            ] = reservation[
                "status"
            ]
            row[
                "reservation_until"
            ] = reservation[
                "expires_at"
            ]

            rows.append(
                row
            )

            self._tracked_alert_positions[
                position_id
            ] = position

            if row[
                "status"
            ] != old_status:
                changed = True

        # Reconciliation metadata itself is worth persisting even
        # when no state transition occurred.
        if rows:
            self._save_trade_alert_positions_state()

        return (
            pd.DataFrame(
                rows
            ),
            snapshot,
        )


    def log_trade_lifecycle_reconciliation(
        self,
        results,
        snapshot,
    ):

        if snapshot.get(
            "disabled",
            False,
        ):

            self.log_message(
                "Trade lifecycle reconciliation disabled."
            )
            return

        if snapshot.get(
            "empty",
            False,
        ):

            self.log_message(
                "No persistent trade lifecycles to reconcile."
            )
            return

        if not snapshot.get(
            "positions_available",
            False,
        ):

            self.log_message(
                "TRADE LIFECYCLE RECONCILIATION: "
                "broker positions unavailable; "
                "prior states retained. "
                f"Reason={snapshot.get('positions_error', '')}"
            )

        if not snapshot.get(
            "orders_available",
            False,
        ):

            self.log_message(
                "TRADE LIFECYCLE RECONCILIATION: "
                "broker order history unavailable; "
                "position-based reconciliation continues, "
                "but terminal order inference is disabled. "
                f"Reason={snapshot.get('orders_error', '')}"
            )

        elif snapshot.get(
            "orders_truncated",
            False,
        ):

            self.log_message(
                "TRADE LIFECYCLE RECONCILIATION: "
                "order-history response hit the configured "
                "limit; unmatched alerts will not be "
                "terminalized from missing order evidence."
            )

        if results.empty:
            return

        display = results[
            [
                "asset_type",
                "underlying",
                "prior_status",
                "status",
                "risk_reserved",
                "reservation_status",
                "broker_open_qty",
                "entry_order_status",
                "close_order_status",
                "match_confidence",
                "reason",
            ]
        ].copy()

        display[
            "broker_open_qty"
        ] = (
            pd.to_numeric(
                display[
                    "broker_open_qty"
                ],
                errors="coerce",
            )
            .round(6)
        )

        self.log_message(
            "\n\n"
            "===== TRADE LIFECYCLE RECONCILIATION =====\n"
            + display.to_string(
                index=False
            )
            + "\n"
            "=========================================="
        )


    def _get_broker_portfolio_exposure(
        self,
        equity,
    ):
        """
        Read current Alpaca positions without placing orders.

        Gross option market value is used only as a conservative
        concentration guard. It is not treated as option max loss,
        especially for spreads whose long/short legs are reported
        as separate positions.
        """

        try:

            raw_positions = (
                self.alpaca_trading_client
                .get_all_positions()
            )

        except Exception as exc:

            return {
                "available": False,
                "error": str(exc),
                "position_count": 0,
                "option_position_count": 0,
                "option_gross_market_value": 0.0,
                "option_gross_pct_equity": 0.0,
                "option_underlyings": [],
                "option_symbols": [],
                "stock_position_count": 0,
                "stock_gross_market_value": 0.0,
                "stock_gross_pct_equity": 0.0,
                "stock_symbols": [],
            }

        positions = raw_positions

        if isinstance(
            raw_positions,
            dict,
        ):

            candidate = (
                raw_positions.get(
                    "positions"
                )
                or raw_positions.get(
                    "data"
                )
            )

            if isinstance(
                candidate,
                list,
            ):

                positions = candidate

            else:

                return {
                    "available": False,
                    "error": (
                        "Unexpected broker positions "
                        "response shape"
                    ),
                    "position_count": 0,
                    "option_position_count": 0,
                    "option_gross_market_value": 0.0,
                    "option_gross_pct_equity": 0.0,
                    "option_underlyings": [],
                    "option_symbols": [],
                "stock_position_count": 0,
                "stock_gross_market_value": 0.0,
                "stock_gross_pct_equity": 0.0,
                "stock_symbols": [],
                }

        if positions is None:
            positions = []

        if not isinstance(
            positions,
            (list, tuple),
        ):

            return {
                "available": False,
                "error": (
                    "Unexpected broker positions "
                    "response type"
                ),
                "position_count": 0,
                "option_position_count": 0,
                "option_gross_market_value": 0.0,
                "option_gross_pct_equity": 0.0,
                "option_underlyings": [],
                "option_symbols": [],
                "stock_position_count": 0,
                "stock_gross_market_value": 0.0,
                "stock_gross_pct_equity": 0.0,
                "stock_symbols": [],
            }

        option_gross_market_value = 0.0
        option_underlyings = set()
        option_symbols = set()
        option_position_count = 0

        stock_gross_market_value = 0.0
        stock_symbols = set()
        stock_position_count = 0

        for position in positions:

            symbol = str(
                getattr(
                    position,
                    "symbol",
                    "",
                )
                or ""
            ).upper()

            raw_asset_class = getattr(
                position,
                "asset_class",
                "",
            )

            asset_class_text = str(
                getattr(
                    raw_asset_class,
                    "value",
                    raw_asset_class,
                )
                or ""
            ).lower()

            underlying = (
                self
                ._option_underlying_from_occ_symbol(
                    symbol
                )
            )

            is_option = (
                "option" in asset_class_text
                or bool(underlying)
            )

            is_stock = (
                not is_option
                and "equity" in asset_class_text
            )

            if (
                not is_option
                and not is_stock
            ):

                continue

            raw_market_value = getattr(
                position,
                "market_value",
                None,
            )

            raw_cost_basis = getattr(
                position,
                "cost_basis",
                None,
            )

            value_source = (
                raw_market_value
                if raw_market_value is not None
                else raw_cost_basis
            )

            try:

                numeric_value = float(
                    value_source
                    or 0.0
                )

            except (
                TypeError,
                ValueError,
            ):

                numeric_value = 0.0

            gross_value = (
                abs(numeric_value)
                if math.isfinite(
                    numeric_value
                )
                else 0.0
            )

            if is_option:

                option_position_count += 1
                option_gross_market_value += (
                    gross_value
                )

                if symbol:
                    option_symbols.add(
                        symbol
                    )

                if underlying:
                    option_underlyings.add(
                        underlying
                    )

            elif is_stock:

                stock_position_count += 1
                stock_gross_market_value += (
                    gross_value
                )

                if symbol:
                    stock_symbols.add(
                        symbol
                    )

        equity = float(
            equity
            or 0.0
        )

        option_gross_pct_equity = (
            option_gross_market_value
            / equity
            if equity > 0
            else 0.0
        )

        stock_gross_pct_equity = (
            stock_gross_market_value
            / equity
            if equity > 0
            else 0.0
        )

        return {
            "available": True,
            "error": "",
            "position_count": len(
                positions
            ),
            "option_position_count": (
                option_position_count
            ),
            "option_gross_market_value": (
                option_gross_market_value
            ),
            "option_gross_pct_equity": (
                option_gross_pct_equity
            ),
            "option_underlyings": sorted(
                option_underlyings
            ),
            "option_symbols": sorted(
                option_symbols
            ),
            "stock_position_count": (
                stock_position_count
            ),
            "stock_gross_market_value": (
                stock_gross_market_value
            ),
            "stock_gross_pct_equity": (
                stock_gross_pct_equity
            ),
            "stock_symbols": sorted(
                stock_symbols
            ),
        }


    def _get_tracked_option_exposure(
        self,
        equity,
    ):
        """
        Summarize active alert-estimated option max risk.

        Risk-reserved lifecycle states consume portfolio capacity.
        ALERTED records reserve only during the configured broker-match
        window (or while reconciliation evidence is incomplete).
        Broker-confirmed states remain reserved; terminal states release.
        Partial broker-confirmed positions reserve risk in proportion
        to reconciled quantity; ORPHANED states reserve full risk.
        """

        today = self.get_datetime().date()

        total_risk = 0.0
        active_setup_count = 0
        expired_tracking_count = 0
        released_alert_reservation_count = 0
        released_alert_nominal_risk = 0.0

        risk_by_direction = {}
        risk_by_underlying = {}
        risk_by_expiration = {}
        risk_by_status = {}
        setups_by_underlying = {}

        for position in (
            self._tracked_alert_positions
            .values()
        ):

            asset_type = str(
                position.get(
                    "asset_type",
                    "OPTION",
                )
                or "OPTION"
            ).upper()

            if asset_type != "OPTION":
                continue

            status = (
                self._normalize_lifecycle_status(
                    position.get(
                        "status",
                        "ALERTED",
                    )
                )
            )

            if (
                status
                not in self._lifecycle_active_statuses()
            ):

                continue

            expiration_text = str(
                position.get(
                    "expiration",
                    "UNKNOWN",
                )
                or "UNKNOWN"
            )

            try:

                expiration_date = (
                    date.fromisoformat(
                        expiration_text
                    )
                )

            except ValueError:

                expiration_date = None

            if (
                expiration_date is not None
                and expiration_date < today
            ):

                expired_tracking_count += 1
                continue

            try:

                max_risk = float(
                    position.get(
                        "entry_max_risk",
                        0.0,
                    )
                    or 0.0
                )

            except (
                TypeError,
                ValueError,
            ):

                max_risk = 0.0

            if (
                not math.isfinite(
                    max_risk
                )
                or max_risk <= 0
            ):

                continue

            reservation = (
                self._refresh_position_risk_reservation(
                    position,
                    broker_evidence_complete=None,
                    record_event=False,
                )
            )

            if not reservation[
                "active"
            ]:

                if status == "ALERTED":
                    released_alert_reservation_count += 1
                    released_alert_nominal_risk += max_risk

                continue

            if status in {
                "PARTIALLY_OPEN",
                "PARTIALLY_CLOSED",
            }:

                try:

                    expected_quantity = float(
                        position.get(
                            "quantity",
                            0.0,
                        )
                        or 0.0
                    )

                    broker_open_quantity = float(
                        position.get(
                            "broker_open_quantity",
                            0.0,
                        )
                        or 0.0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    expected_quantity = 0.0
                    broker_open_quantity = 0.0

                if (
                    expected_quantity > 0
                    and broker_open_quantity > 0
                ):

                    max_risk *= min(
                        1.0,
                        broker_open_quantity
                        / expected_quantity,
                    )

            direction = str(
                position.get(
                    "direction",
                    "UNKNOWN",
                )
                or "UNKNOWN"
            ).upper()

            underlying = str(
                position.get(
                    "underlying",
                    "UNKNOWN",
                )
                or "UNKNOWN"
            ).upper()

            total_risk += max_risk
            active_setup_count += 1

            risk_by_direction[
                direction
            ] = (
                risk_by_direction.get(
                    direction,
                    0.0,
                )
                + max_risk
            )

            risk_by_underlying[
                underlying
            ] = (
                risk_by_underlying.get(
                    underlying,
                    0.0,
                )
                + max_risk
            )

            risk_by_expiration[
                expiration_text
            ] = (
                risk_by_expiration.get(
                    expiration_text,
                    0.0,
                )
                + max_risk
            )

            risk_by_status[
                status
            ] = (
                risk_by_status.get(
                    status,
                    0.0,
                )
                + max_risk
            )

            setups_by_underlying[
                underlying
            ] = (
                setups_by_underlying.get(
                    underlying,
                    0,
                )
                + 1
            )

        equity = float(
            equity
            or 0.0
        )

        return {
            "active_setup_count": (
                active_setup_count
            ),
            "expired_tracking_count": (
                expired_tracking_count
            ),
            "released_alert_reservation_count": (
                released_alert_reservation_count
            ),
            "released_alert_nominal_risk": (
                released_alert_nominal_risk
            ),
            "total_risk": total_risk,
            "total_risk_pct_equity": (
                total_risk / equity
                if equity > 0
                else 0.0
            ),
            "risk_by_direction": (
                risk_by_direction
            ),
            "risk_by_underlying": (
                risk_by_underlying
            ),
            "risk_by_expiration": (
                risk_by_expiration
            ),
            "risk_by_status": (
                risk_by_status
            ),
            "setups_by_underlying": (
                setups_by_underlying
            ),
        }


    # ======================================================
    # POSITION SIZING
    # ======================================================

    def size_trade_structures(
        self,
        bullish_structures,
        bearish_structures,
    ):

        frames = []

        if (
            bullish_structures is not None
            and not bullish_structures.empty
        ):

            frames.append(
                bullish_structures.copy()
            )

        if (
            bearish_structures is not None
            and not bearish_structures.empty
        ):

            frames.append(
                bearish_structures.copy()
            )

        if not frames:

            return (
                pd.DataFrame(),
                {},
            )

        structures = pd.concat(
            frames,
            ignore_index=True,
        )

        try:

            account = (
                self._get_account_risk_snapshot()
            )

        except Exception as exc:

            self.log_message(
                "Position sizing could not read "
                f"the Alpaca account: {exc}"
            )

            structures[
                "quantity"
            ] = 0

            structures[
                "alert_status"
            ] = "SKIP"

            structures[
                "sizing_reason"
            ] = (
                "Could not read account equity/"
                "options buying power"
            )

            return (
                structures,
                {},
            )

        equity = float(
            account.get(
                "equity",
                0,
            )
            or 0
        )

        options_bp = float(
            account.get(
                "options_buying_power",
                0,
            )
            or 0
        )

        account_blocked = any(
            [
                account.get(
                    "trading_blocked",
                    False,
                ),
                account.get(
                    "account_blocked",
                    False,
                ),
                account.get(
                    "trade_suspended_by_user",
                    False,
                ),
            ]
        )

        self.log_message(
            "Risk account snapshot: "
            f"equity=${equity:,.2f}, "
            f"options buying power="
            f"${options_bp:,.2f}."
        )

        # --------------------------------------------------
        # HARD ACCOUNT SAFETY CHECKS
        # --------------------------------------------------

        if (
            account_blocked
            or equity <= 0
            or options_bp <= 0
        ):

            structures[
                "quantity"
            ] = 0

            structures[
                "risk_budget"
            ] = 0.0

            structures[
                "total_debit"
            ] = 0.0

            structures[
                "total_max_risk"
            ] = 0.0

            structures[
                "total_max_reward"
            ] = None

            structures[
                "risk_pct_equity"
            ] = 0.0

            structures[
                "alert_status"
            ] = "SKIP"

            if account_blocked:

                reason = (
                    "Account is blocked or "
                    "trade-suspended"
                )

            elif equity <= 0:

                reason = (
                    "Account equity is not "
                    "positive"
                )

            else:

                reason = (
                    "No options buying power "
                    "available"
                )

            structures[
                "sizing_reason"
            ] = reason

            return (
                structures,
                account,
            )

        # --------------------------------------------------
        # EXISTING PORTFOLIO EXPOSURE
        # --------------------------------------------------

        tracked_exposure = (
            self._get_tracked_option_exposure(
                equity
            )
        )

        broker_exposure = (
            self._get_broker_portfolio_exposure(
                equity
            )
        )

        account[
            "tracked_option_exposure"
        ] = tracked_exposure

        account[
            "broker_option_exposure"
        ] = broker_exposure

        self.log_message(
            "Portfolio exposure snapshot: "
            f"risk-reserved tracked setups="
            f"{tracked_exposure['active_setup_count']}, "
            f"reserved max-risk="
            f"${tracked_exposure['total_risk']:,.2f} "
            f"({tracked_exposure['total_risk_pct_equity'] * 100:.2f}% equity), "
            f"released unmatched alerts="
            f"{tracked_exposure['released_alert_reservation_count']} "
            f"(${tracked_exposure['released_alert_nominal_risk']:,.2f} nominal risk released), "
            f"broker option gross="
            f"${broker_exposure['option_gross_market_value']:,.2f} "
            f"({broker_exposure['option_gross_pct_equity'] * 100:.2f}% equity)."
        )

        if (
            self.parameters[
                "portfolio_require_broker_positions_snapshot"
            ]
            and not broker_exposure[
                "available"
            ]
        ):

            structures[
                "quantity"
            ] = 0

            structures[
                "risk_budget"
            ] = 0.0

            structures[
                "total_debit"
            ] = 0.0

            structures[
                "total_max_risk"
            ] = 0.0

            structures[
                "total_max_reward"
            ] = None

            structures[
                "risk_pct_equity"
            ] = 0.0

            structures[
                "alert_status"
            ] = "SKIP"

            structures[
                "sizing_reason"
            ] = (
                "Could not read broker open "
                "positions; portfolio guard "
                "fails closed"
            )

            return (
                structures,
                account,
            )

        broker_gross_cap_pct = float(
            self.parameters[
                "portfolio_max_broker_options_gross_pct_equity"
            ]
        )

        if (
            broker_exposure[
                "available"
            ]
            and broker_gross_cap_pct > 0
            and broker_exposure[
                "option_gross_pct_equity"
            ]
            >= broker_gross_cap_pct
        ):

            structures[
                "quantity"
            ] = 0

            structures[
                "risk_budget"
            ] = 0.0

            structures[
                "total_debit"
            ] = 0.0

            structures[
                "total_max_risk"
            ] = 0.0

            structures[
                "total_max_reward"
            ] = None

            structures[
                "risk_pct_equity"
            ] = 0.0

            structures[
                "alert_status"
            ] = "SKIP"

            structures[
                "sizing_reason"
            ] = (
                "Broker option gross market "
                "value is at/above portfolio cap"
            )

            return (
                structures,
                account,
            )

        # --------------------------------------------------
        # PER-TRADE RISK BUDGET
        # --------------------------------------------------

        equity_budget = (
            equity
            * self.parameters[
                "position_risk_pct_equity"
            ]
        )

        dollar_cap = float(
            self.parameters[
                "position_max_risk_dollars"
            ]
        )

        bp_trade_budget = (
            options_bp
            * self.parameters[
                "position_max_options_bp_pct_per_trade"
            ]
        )

        per_trade_budget = min(
            equity_budget,
            dollar_cap,
            bp_trade_budget,
        )

        # --------------------------------------------------
        # TOTAL NEW-RISK BUDGET FOR THIS RUN
        # --------------------------------------------------

        total_equity_budget = (
            equity
            * self.parameters[
                "position_total_new_risk_pct_equity"
            ]
        )

        total_bp_budget = (
            options_bp
            * self.parameters[
                "position_max_options_bp_pct_total"
            ]
        )

        total_run_budget = min(
            total_equity_budget,
            total_bp_budget,
        )

        remaining_run_budget = (
            total_run_budget
        )

        total_risk_cap_pct = float(
            self.parameters[
                "portfolio_max_active_tracked_risk_pct_equity"
            ]
        )

        direction_risk_cap_pct = float(
            self.parameters[
                "portfolio_max_directional_tracked_risk_pct_equity"
            ]
        )

        expiration_risk_cap_pct = float(
            self.parameters[
                "portfolio_max_expiration_tracked_risk_pct_equity"
            ]
        )

        portfolio_total_risk_cap = (
            equity * total_risk_cap_pct
            if total_risk_cap_pct > 0
            else float("inf")
        )

        portfolio_direction_risk_cap = (
            equity * direction_risk_cap_pct
            if direction_risk_cap_pct > 0
            else float("inf")
        )

        portfolio_expiration_risk_cap = (
            equity * expiration_risk_cap_pct
            if expiration_risk_cap_pct > 0
            else float("inf")
        )

        current_portfolio_risk = float(
            tracked_exposure[
                "total_risk"
            ]
        )

        current_direction_risk = dict(
            tracked_exposure[
                "risk_by_direction"
            ]
        )

        current_expiration_risk = dict(
            tracked_exposure[
                "risk_by_expiration"
            ]
        )

        current_underlying_setups = dict(
            tracked_exposure[
                "setups_by_underlying"
            ]
        )

        current_active_setups = int(
            tracked_exposure[
                "active_setup_count"
            ]
        )

        broker_option_underlyings = set(
            broker_exposure.get(
                "option_underlyings",
                [],
            )
        )

        max_active_setups = max(
            0,
            int(
                self.parameters[
                    "portfolio_max_active_tracked_setups"
                ]
            ),
        )

        max_setups_per_underlying = max(
            0,
            int(
                self.parameters[
                    "portfolio_max_active_tracked_setups_per_underlying"
                ]
            ),
        )

        max_contracts = max(
            1,
            int(
                self.parameters[
                    "position_max_contracts_per_trade"
                ]
            ),
        )

        max_alerts = max(
            0,
            int(
                self.parameters[
                    "position_max_alerts_per_run"
                ]
            ),
        )

        minimum_alert_score = float(
            self.parameters[
                "alert_min_structure_score"
            ]
        )

        # Allocate higher-scoring structures first.
        structures = (
            structures
            .sort_values(
                [
                    "structure_score",
                    "stock_score",
                ],
                ascending=[
                    False,
                    False,
                ],
            )
            .reset_index(
                drop=True
            )
        )

        sized_rows = []

        alerts_allocated = 0

        for _, row in (
            structures.iterrows()
        ):

            output = row.to_dict()

            output[
                "quantity"
            ] = 0

            output[
                "risk_budget"
            ] = 0.0

            output[
                "total_debit"
            ] = 0.0

            output[
                "total_max_risk"
            ] = 0.0

            output[
                "total_max_reward"
            ] = None

            output[
                "risk_pct_equity"
            ] = 0.0

            output[
                "alert_status"
            ] = "SKIP"

            decision = str(
                row.get(
                    "decision",
                    "",
                )
            )

            score = float(
                row.get(
                    "structure_score",
                    0,
                )
                or 0
            )

            if decision == "NO TRADE":

                output[
                    "sizing_reason"
                ] = (
                    "Structure selector returned "
                    "NO TRADE"
                )

                sized_rows.append(
                    output
                )

                continue

            if score < minimum_alert_score:

                output[
                    "sizing_reason"
                ] = (
                    "Structure score below "
                    f"alert threshold "
                    f"{minimum_alert_score:.1f}"
                )

                sized_rows.append(
                    output
                )

                continue

            if (
                alerts_allocated
                >= max_alerts
            ):

                output[
                    "sizing_reason"
                ] = (
                    "Maximum actionable alerts "
                    "for this run reached"
                )

                sized_rows.append(
                    output
                )

                continue

            try:

                per_contract_risk = float(
                    row.get(
                        "max_risk",
                        0,
                    )
                    or 0
                )

            except (
                TypeError,
                ValueError,
            ):

                per_contract_risk = 0.0

            if (
                not math.isfinite(
                    per_contract_risk
                )
                or per_contract_risk <= 0
            ):

                output[
                    "sizing_reason"
                ] = (
                    "Invalid per-contract "
                    "maximum risk"
                )

                sized_rows.append(
                    output
                )

                continue

            underlying = str(
                row.get(
                    "underlying",
                    "",
                )
                or ""
            ).upper()

            direction = str(
                row.get(
                    "direction",
                    "UNKNOWN",
                )
                or "UNKNOWN"
            ).upper()

            expiration_key = str(
                row.get(
                    "expiration",
                    "UNKNOWN",
                )
                or "UNKNOWN"
            )

            if (
                max_active_setups > 0
                and current_active_setups
                >= max_active_setups
            ):

                output[
                    "sizing_reason"
                ] = (
                    "Maximum active tracked "
                    "option setups reached"
                )

                sized_rows.append(
                    output
                )

                continue

            if (
                max_setups_per_underlying > 0
                and current_underlying_setups.get(
                    underlying,
                    0,
                )
                >= max_setups_per_underlying
            ):

                output[
                    "sizing_reason"
                ] = (
                    "Underlying already has the "
                    "maximum active lifecycle setup count"
                )

                sized_rows.append(
                    output
                )

                continue

            if (
                self.parameters[
                    "portfolio_block_new_same_underlying_broker_option_position"
                ]
                and underlying
                in broker_option_underlyings
            ):

                output[
                    "sizing_reason"
                ] = (
                    "Broker already has an open "
                    "option position on this underlying"
                )

                sized_rows.append(
                    output
                )

                continue

            total_capacity = max(
                0.0,
                portfolio_total_risk_cap
                - current_portfolio_risk,
            )

            direction_capacity = max(
                0.0,
                portfolio_direction_risk_cap
                - current_direction_risk.get(
                    direction,
                    0.0,
                ),
            )

            expiration_capacity = max(
                0.0,
                portfolio_expiration_risk_cap
                - current_expiration_risk.get(
                    expiration_key,
                    0.0,
                ),
            )

            trade_budget = min(
                per_trade_budget,
                remaining_run_budget,
                total_capacity,
                direction_capacity,
                expiration_capacity,
            )

            output[
                "risk_budget"
            ] = trade_budget

            if (
                trade_budget
                < per_contract_risk
            ):

                if (
                    total_capacity
                    < per_contract_risk
                ):

                    reason = (
                        "Portfolio active-risk cap "
                        "leaves less than one contract "
                        "of capacity"
                    )

                elif (
                    direction_capacity
                    < per_contract_risk
                ):

                    reason = (
                        "Directional portfolio-risk cap "
                        "leaves less than one contract "
                        "of capacity"
                    )

                elif (
                    expiration_capacity
                    < per_contract_risk
                ):

                    reason = (
                        "Expiration concentration cap "
                        "leaves less than one contract "
                        "of capacity"
                    )

                elif (
                    remaining_run_budget
                    < per_contract_risk
                ):

                    reason = (
                        "Remaining new-risk budget "
                        "for this run is below one contract"
                    )

                else:

                    reason = (
                        "One contract exceeds the "
                        "current per-trade risk budget"
                    )

                output[
                    "sizing_reason"
                ] = reason

                sized_rows.append(
                    output
                )

                continue

            quantity = math.floor(
                trade_budget
                / per_contract_risk
            )

            quantity = min(
                quantity,
                max_contracts,
            )

            if quantity < 1:

                output[
                    "sizing_reason"
                ] = (
                    "One contract exceeds the "
                    "current risk budget"
                )

                sized_rows.append(
                    output
                )

                continue

            total_max_risk = (
                per_contract_risk
                * quantity
            )

            try:

                debit_per_share = float(
                    row.get(
                        "net_debit",
                        0,
                    )
                    or 0
                )

            except (
                TypeError,
                ValueError,
            ):

                debit_per_share = 0.0

            total_debit = (
                debit_per_share
                * 100
                * quantity
            )

            raw_max_reward = row.get(
                "max_reward",
                None,
            )

            total_max_reward = None

            try:

                per_contract_reward = float(
                    raw_max_reward
                )

                if math.isinf(
                    per_contract_reward
                ):

                    total_max_reward = float(
                        "inf"
                    )

                elif math.isfinite(
                    per_contract_reward
                ):

                    total_max_reward = (
                        per_contract_reward
                        * quantity
                    )

            except (
                TypeError,
                ValueError,
            ):

                total_max_reward = None

            output[
                "quantity"
            ] = int(
                quantity
            )

            output[
                "total_debit"
            ] = total_debit

            output[
                "total_max_risk"
            ] = total_max_risk

            output[
                "total_max_reward"
            ] = total_max_reward

            output[
                "risk_pct_equity"
            ] = (
                total_max_risk
                / equity
            )

            output[
                "alert_status"
            ] = "ALERT"

            output[
                "sizing_reason"
            ] = (
                "Fits per-trade and "
                "portfolio risk budgets"
            )

            remaining_run_budget = max(
                0.0,
                remaining_run_budget
                - total_max_risk,
            )

            current_portfolio_risk += (
                total_max_risk
            )

            current_direction_risk[
                direction
            ] = (
                current_direction_risk.get(
                    direction,
                    0.0,
                )
                + total_max_risk
            )

            current_expiration_risk[
                expiration_key
            ] = (
                current_expiration_risk.get(
                    expiration_key,
                    0.0,
                )
                + total_max_risk
            )

            current_underlying_setups[
                underlying
            ] = (
                current_underlying_setups.get(
                    underlying,
                    0,
                )
                + 1
            )

            current_active_setups += 1

            alerts_allocated += 1

            sized_rows.append(
                output
            )

        sized = pd.DataFrame(
            sized_rows
        )

        account[
            "per_trade_risk_budget"
        ] = per_trade_budget

        account[
            "total_run_risk_budget"
        ] = total_run_budget

        account[
            "remaining_run_risk_budget"
        ] = remaining_run_budget

        account[
            "portfolio_active_risk_cap"
        ] = portfolio_total_risk_cap

        account[
            "portfolio_direction_risk_cap"
        ] = portfolio_direction_risk_cap

        account[
            "portfolio_expiration_risk_cap"
        ] = portfolio_expiration_risk_cap

        account[
            "portfolio_tracked_risk_before"
        ] = tracked_exposure[
            "total_risk"
        ]

        account[
            "portfolio_projected_risk_after"
        ] = current_portfolio_risk

        account[
            "portfolio_active_setups_before"
        ] = tracked_exposure[
            "active_setup_count"
        ]

        account[
            "portfolio_projected_active_setups_after"
        ] = current_active_setups

        return (
            sized,
            account,
        )


    # ======================================================
    # POSITION SIZING OUTPUT
    # ======================================================

    def log_position_sizing(
        self,
        sized,
    ):

        if sized.empty:

            self.log_message(
                "No structures available "
                "for position sizing."
            )

            return

        display = sized[
            [
                "underlying",
                "direction",
                "decision",
                "structure_score",
                "quantity",
                "risk_budget",
                "max_risk",
                "total_max_risk",
                "total_max_reward",
                "risk_pct_equity",
                "alert_status",
                "sizing_reason",
            ]
        ].copy()

        for column in [
            "structure_score",
            "risk_budget",
            "max_risk",
            "total_max_risk",
        ]:

            display[
                column
            ] = (
                pd.to_numeric(
                    display[
                        column
                    ],
                    errors="coerce",
                )
                .round(2)
            )

        display[
            "risk_pct_equity"
        ] = (
            pd.to_numeric(
                display[
                    "risk_pct_equity"
                ],
                errors="coerce",
            )
            * 100
        ).round(3)

        def format_reward(
            value,
        ):

            if value is None:
                return ""

            try:

                numeric = float(
                    value
                )

            except (
                TypeError,
                ValueError,
            ):

                return ""

            if math.isnan(
                numeric
            ):

                return ""

            if math.isinf(
                numeric
            ):

                return "UNLIMITED"

            return f"${numeric:,.2f}"

        display[
            "total_max_reward"
        ] = display[
            "total_max_reward"
        ].map(
            format_reward
        )

        display = display.rename(
            columns={
                "structure_score":
                    "score",
                "risk_budget":
                    "risk_budget_$",
                "max_risk":
                    "risk/contract_$",
                "total_max_risk":
                    "total_risk_$",
                "total_max_reward":
                    "total_reward_$",
                "risk_pct_equity":
                    "risk_%_equity",
            }
        )

        no_trade_mask = (
            display[
                "decision"
            ]
            == "NO TRADE"
        )

        sizing_blank_columns = [
            "quantity",
            "risk_budget_$",
            "risk/contract_$",
            "total_risk_$",
            "total_reward_$",
            "risk_%_equity",
        ]

        display[
            sizing_blank_columns
        ] = (
            display[
                sizing_blank_columns
            ]
            .astype(object)
        )

        display.loc[
            no_trade_mask,
            sizing_blank_columns,
        ] = ""

        self.log_message(
            "\n\n"
            "===== POSITION SIZING =====\n"
            + display.to_string(
                index=False
            )
            + "\n"
            "==========================="
        )


    # ======================================================
    # PERSISTENT ALERT DEDUPLICATION
    # ======================================================

    def _load_trade_alert_dedupe_state(
        self,
    ):

        self._sent_trade_alert_keys = set()

        path = (
            self.trade_alert_dedupe_path
        )

        if not path:
            return

        if not os.path.exists(
            path
        ):
            return

        today = str(
            date.today()
        )

        try:

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:

                state = json.load(
                    handle
                )

            stored_date = str(
                state.get(
                    "date",
                    "",
                )
            )

            # Old state never suppresses alerts on a new
            # calendar day.
            if stored_date != today:

                self._save_trade_alert_dedupe_state()

                return

            keys = state.get(
                "keys",
                [],
            )

            for raw_key in keys:

                if (
                    isinstance(
                        raw_key,
                        list,
                    )
                    and len(
                        raw_key
                    )
                    == 6
                ):

                    self._sent_trade_alert_keys.add(
                        tuple(
                            raw_key
                        )
                    )

            if self._sent_trade_alert_keys:

                self.log_message(
                    "Loaded "
                    f"{len(self._sent_trade_alert_keys)} "
                    "persisted trade alert "
                    "dedupe key(s) for today."
                )

        except Exception as exc:

            # A damaged state file should never stop the
            # scanner. Start clean and overwrite it on the
            # next successful alert.
            self._sent_trade_alert_keys = set()

            self.log_message(
                "Could not load persistent "
                "trade alert dedupe state: "
                f"{exc}"
            )


    def _save_trade_alert_dedupe_state(
        self,
    ):

        path = (
            self.trade_alert_dedupe_path
        )

        if not path:
            return

        today = str(
            date.today()
        )

        # Keep only today's keys so the state file remains
        # tiny and daily dedupe resets automatically.
        today_keys = sorted(
            [
                list(
                    key
                )
                for key
                in self._sent_trade_alert_keys
                if (
                    len(key) >= 1
                    and str(
                        key[0]
                    )
                    == today
                )
            ],
            key=lambda item: tuple(
                str(value)
                for value
                in item
            ),
        )

        state = {
            "date":
                today,

            "keys":
                today_keys,
        }

        directory = os.path.dirname(
            os.path.abspath(
                path
            )
        )

        if directory:

            os.makedirs(
                directory,
                exist_ok=True,
            )

        temporary_path = (
            path + ".tmp"
        )

        try:

            with open(
                temporary_path,
                "w",
                encoding="utf-8",
            ) as handle:

                json.dump(
                    state,
                    handle,
                    indent=2,
                    sort_keys=True,
                )

                handle.write(
                    "\n"
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

            os.replace(
                temporary_path,
                path,
            )

        except Exception as exc:

            try:

                if os.path.exists(
                    temporary_path
                ):

                    os.remove(
                        temporary_path
                    )

            except Exception:

                pass

            self.log_message(
                "Could not save persistent "
                "trade alert dedupe state: "
                f"{exc}"
            )


    # ======================================================
    # PERSISTENT ALERT-TRACKED SETUPS
    # ======================================================

    def _load_trade_alert_positions_state(
        self,
    ):

        self._tracked_alert_positions = {}

        path = (
            self.trade_alert_positions_path
        )

        if (
            not path
            or not os.path.exists(
                path
            )
        ):

            return

        try:

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:

                state = json.load(
                    handle
                )

            positions = state.get(
                "positions",
                [],
            )

            for position in positions:

                if not isinstance(
                    position,
                    dict,
                ):

                    continue

                position_id = str(
                    position.get(
                        "id",
                        "",
                    )
                )

                if not position_id:
                    continue

                raw_status = str(
                    position.get(
                        "status",
                        "ALERTED",
                    )
                    or "ALERTED"
                ).upper()

                position.setdefault(
                    "asset_type",
                    "OPTION",
                )

                position.setdefault(
                    "lifecycle_version",
                    1,
                )

                if raw_status == "TRACKING":

                    position[
                        "status"
                    ] = "ALERTED"

                    history = position.get(
                        "lifecycle_history",
                        [],
                    )

                    if not isinstance(
                        history,
                        list,
                    ):
                        history = []

                    history.append(
                        {
                            "timestamp": (
                                datetime.now(
                                    timezone.utc
                                ).isoformat()
                            ),
                            "event": "MIGRATION",
                            "from_status": "TRACKING",
                            "to_status": "ALERTED",
                            "status": "ALERTED",
                            "reason": (
                                "Legacy TRACKING state migrated; "
                                "broker fill not yet proven"
                            ),
                        }
                    )

                    position[
                        "lifecycle_history"
                    ] = history[-int(
                        self.parameters.get(
                            "lifecycle_history_max_events",
                            100,
                        )
                    ):]

                else:

                    position[
                        "status"
                    ] = self._normalize_lifecycle_status(
                        raw_status
                    )

                position.setdefault(
                    "status_updated_at",
                    position.get(
                        "entry_timestamp",
                        "",
                    ),
                )

                position.setdefault(
                    "broker_peak_open_quantity",
                    0.0,
                )

                position.setdefault(
                    "broker_open_quantity",
                    0.0,
                )

                position.setdefault(
                    "risk_reservation_active",
                    None,
                )

                position.setdefault(
                    "risk_reservation_status",
                    "",
                )

                self._tracked_alert_positions[
                    position_id
                ] = position

            if self._tracked_alert_positions:

                self.log_message(
                    "Loaded "
                    f"{len(self._tracked_alert_positions)} "
                    "persistent trade lifecycle "
                    "record(s)."
                )

        except Exception as exc:

            self._tracked_alert_positions = {}

            self.log_message(
                "Could not load persistent "
                "alert-tracked setup state: "
                f"{exc}"
            )


    def _save_trade_alert_positions_state(
        self,
    ):

        path = (
            self.trade_alert_positions_path
        )

        if not path:
            return

        directory = os.path.dirname(
            os.path.abspath(
                path
            )
        )

        if directory:

            os.makedirs(
                directory,
                exist_ok=True,
            )

        temporary_path = (
            path + ".tmp"
        )

        state = {
            "schema_version": 2,

            "saved_at": (
                self.get_datetime()
                .isoformat()
            ),

            "positions":
                list(
                    self._tracked_alert_positions
                    .values()
                ),
        }

        try:

            with open(
                temporary_path,
                "w",
                encoding="utf-8",
            ) as handle:

                json.dump(
                    state,
                    handle,
                    indent=2,
                    sort_keys=True,
                    default=str,
                )

                handle.write(
                    "\n"
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

            os.replace(
                temporary_path,
                path,
            )

        except Exception as exc:

            try:

                if os.path.exists(
                    temporary_path
                ):

                    os.remove(
                        temporary_path
                    )

            except Exception:

                pass

            self.log_message(
                "Could not save persistent "
                "alert-tracked setup state: "
                f"{exc}"
            )


    @staticmethod
    def _alert_position_id(
        key,
    ):

        return "|".join(
            str(
                part
            )
            for part in key
        )


    def _register_alert_tracked_setup(
        self,
        row,
        key,
        entry_basis,
    ):

        position_id = (
            self._alert_position_id(
                key
            )
        )

        if (
            position_id
            in self._tracked_alert_positions
        ):

            return

        quantity = int(
            row[
                "quantity"
            ]
        )

        entry_debit = float(
            row[
                "net_debit"
            ]
        )

        total_debit = float(
            row[
                "total_debit"
            ]
        )

        now = self.get_datetime()

        try:
            reservation_minutes = max(
                0.0,
                float(
                    self.parameters.get(
                        "lifecycle_alert_risk_reservation_minutes",
                        60.0,
                    )
                ),
            )
        except (
            TypeError,
            ValueError,
        ):
            reservation_minutes = 60.0

        reservation_until = (
            now
            + timedelta(
                minutes=reservation_minutes
            )
        )

        self._tracked_alert_positions[
            position_id
        ] = {
            "id":
                position_id,

            "alert_key":
                list(
                    key
                ),

            "entry_timestamp":
                now.isoformat(),

            "entry_date":
                str(
                    key[0]
                ),

            "entry_basis":
                entry_basis,

            "underlying":
                str(
                    row[
                        "underlying"
                    ]
                ),

            "direction":
                str(
                    row[
                        "direction"
                    ]
                ),

            "decision":
                str(
                    row[
                        "decision"
                    ]
                ),

            "quantity":
                quantity,

            "long_contract":
                str(
                    row[
                        "long_contract"
                    ]
                ),

            "short_contract":
                str(
                    row.get(
                        "short_contract",
                        "",
                    )
                    or ""
                ),

            "expiration":
                str(
                    row[
                        "expiration"
                    ]
                ),

            "entry_debit_per_share":
                entry_debit,

            "entry_total_debit":
                total_debit,

            "entry_max_risk":
                float(
                    row[
                        "total_max_risk"
                    ]
                ),

            "entry_max_reward":
                (
                    "UNLIMITED"
                    if (
                        row.get(
                            "total_max_reward",
                            None,
                        )
                        is not None
                        and math.isinf(
                            float(
                                row[
                                    "total_max_reward"
                                ]
                            )
                        )
                    )
                    else (
                        float(
                            row[
                                "total_max_reward"
                            ]
                        )
                        if row.get(
                            "total_max_reward",
                            None,
                        )
                        is not None
                        and not pd.isna(
                            row[
                                "total_max_reward"
                            ]
                        )
                        else None
                    )
                ),

            "entry_breakeven":
                float(
                    row[
                        "breakeven"
                    ]
                ),

            "entry_structure_score":
                float(
                    row[
                        "structure_score"
                    ]
                ),

            "entry_option_daily_volume":
                (
                    float(
                        row.get(
                            "daily_volume",
                            0.0,
                        )
                        or 0.0
                    )
                ),

            "entry_option_daily_activity_status":
                str(
                    row.get(
                        "daily_activity_status",
                        "UNKNOWN",
                    )
                    or "UNKNOWN"
                ),

            "entry_iv_percentile":
                (
                    None
                    if (
                        row.get(
                            "iv_percentile",
                            None,
                        )
                        is None
                        or pd.isna(
                            row.get(
                                "iv_percentile",
                                None,
                            )
                        )
                    )
                    else float(
                        row.get(
                            "iv_percentile"
                        )
                    )
                ),

            "entry_iv_rank":
                (
                    None
                    if (
                        row.get(
                            "iv_rank",
                            None,
                        )
                        is None
                        or pd.isna(
                            row.get(
                                "iv_rank",
                                None,
                            )
                        )
                    )
                    else float(
                        row.get(
                            "iv_rank"
                        )
                    )
                ),

            "entry_iv_history_samples":
                int(
                    row.get(
                        "iv_history_samples",
                        0,
                    )
                    or 0
                ),

            "entry_event_risk":
                str(
                    row.get(
                        "event_risk",
                        "UNKNOWN",
                    )
                ),

            "asset_type":
                "OPTION",

            "lifecycle_version":
                1,

            "status":
                "ALERTED",

            "status_updated_at":
                now.isoformat(),

            "risk_reservation_active":
                True,

            "risk_reservation_status":
                "RESERVED_ALERT_WINDOW",

            "risk_reservation_expires_at":
                reservation_until.isoformat(),

            "risk_reservation_reason":
                "New alert is inside the configured broker-match risk-reservation window",

            "risk_reservation_updated_at":
                now.isoformat(),

            "risk_reservation_released_at":
                "",

            "risk_reservation_reactivated_at":
                "",

            "risk_reservation_evidence_complete":
                False,

            "broker_open_quantity":
                0.0,

            "broker_peak_open_quantity":
                0.0,

            "broker_reconciliation_state":
                "UNRECONCILED",

            "broker_reconciliation_note":
                "Trade alert emitted; awaiting broker match",

            "lifecycle_history":
                [],

            "last_exit_alert_date":
                "",

            "last_exit_alert_action":
                "",
        }

        position = self._tracked_alert_positions[
            position_id
        ]

        self._record_lifecycle_event(
            position,
            "ALERT_EMITTED",
            "Trade alert emitted; awaiting broker reconciliation",
            details={
                "entry_basis": entry_basis,
                "quantity": quantity,
                "risk_reservation_until": (
                    reservation_until.isoformat()
                ),
            },
        )

        self._tracked_alert_positions[
            position_id
        ] = position

        self._save_trade_alert_positions_state()


    def _register_micro_alert_tracked_setup(
        self,
        row,
        key,
        entry_basis,
        simulated=False,
    ):

        position_id = (
            self._alert_position_id(
                key
            )
        )

        if (
            position_id
            in self._tracked_alert_positions
        ):

            return

        now_text = (
            self.get_datetime()
            .isoformat()
        )

        self._tracked_alert_positions[
            position_id
        ] = {
            "id": position_id,
            "alert_key": list(
                key
            ),
            "entry_timestamp": now_text,
            "entry_date": str(
                key[0]
            ),
            "entry_basis": entry_basis,
            "asset_type": "FRACTIONAL_STOCK",
            "sizing_basis": (
                "SIMULATED_MICRO_EQUITY"
                if simulated
                else "ALPACA_ACCOUNT_EQUITY"
            ),
            "underlying": str(
                row[
                    "symbol"
                ]
            ),
            "direction": "BULLISH",
            "decision": "MICRO FRACTIONAL LONG",
            "target_notional": float(
                row[
                    "notional"
                ]
            ),
            "approx_shares": float(
                row[
                    "approx_shares"
                ]
            ),
            "entry_reference_bid": float(
                row[
                    "reference_bid"
                ]
            ),
            "entry_reference_ask": float(
                row[
                    "reference_ask"
                ]
            ),
            "planning_stop_price": float(
                row[
                    "stop_price"
                ]
            ),
            "planning_target_price": float(
                row[
                    "target_price"
                ]
            ),
            "planned_loss": float(
                row[
                    "planned_loss"
                ]
            ),
            "planned_gain": float(
                row[
                    "planned_gain"
                ]
            ),
            "lifecycle_version": 1,
            "status": "ALERTED",
            "status_updated_at": now_text,
            "broker_open_quantity": 0.0,
            "broker_peak_open_quantity": 0.0,
            "broker_reconciliation_state": "UNRECONCILED",
            "broker_reconciliation_note": (
                "Micro trade alert emitted; awaiting broker match"
            ),
            "lifecycle_history": [],
            "last_exit_alert_date": "",
            "last_exit_alert_action": "",
        }

        position = self._tracked_alert_positions[
            position_id
        ]

        self._record_lifecycle_event(
            position,
            "ALERT_EMITTED",
            "Micro trade alert emitted; awaiting broker reconciliation",
            details={
                "entry_basis": entry_basis,
                "target_notional": position[
                    "target_notional"
                ],
                "approx_shares": position[
                    "approx_shares"
                ],
            },
        )

        self._tracked_alert_positions[
            position_id
        ] = position

        self._save_trade_alert_positions_state()


    # ======================================================
    # EXIT ALERT JSONL
    # ======================================================

    def _append_exit_alert_jsonl(
        self,
        payload,
    ):

        path = (
            self.exit_alerts_jsonl_path
        )

        if not path:
            return

        try:

            with open(
                path,
                "a",
                encoding="utf-8",
            ) as handle:

                handle.write(
                    json.dumps(
                        payload,
                        default=str,
                        sort_keys=True,
                    )
                    + "\n"
                )

        except Exception as exc:

            self.log_message(
                "Could not write exit alert "
                f"JSONL file: {exc}"
            )


    # ======================================================
    # THESIS CHECK
    # ======================================================

    def _evaluate_tracked_thesis(
        self,
        position,
        stock_metrics,
    ):

        if not self.parameters[
            "exit_thesis_invalidation_enabled"
        ]:

            return (
                "DISABLED",
                False,
                False,
                "Thesis invalidation disabled",
            )

        underlying = str(
            position[
                "underlying"
            ]
        )

        metrics = stock_metrics.get(
            underlying
        )

        if metrics is None:

            return (
                "UNKNOWN",
                False,
                False,
                "No current stock metrics available",
            )

        try:

            price = float(
                metrics[
                    "price"
                ]
            )

            sma20 = float(
                metrics[
                    "sma20"
                ]
            )

            momentum20 = float(
                metrics[
                    "momentum20"
                ]
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ):

            return (
                "UNKNOWN",
                False,
                False,
                "Current thesis metrics unavailable",
            )

        direction = str(
            position[
                "direction"
            ]
        )

        if direction == "BULLISH":

            price_break = (
                price < sma20
            )

            momentum_break = (
                momentum20 <= 0
            )

        else:

            price_break = (
                price > sma20
            )

            momentum_break = (
                momentum20 >= 0
            )

        invalid = (
            price_break
            and momentum_break
        )

        warning = (
            not invalid
            and (
                price_break
                or momentum_break
            )
        )

        if invalid:

            state = "INVALID"

            reason = (
                "Price/SMA20 and 20-day "
                "momentum both reversed"
            )

        elif warning:

            state = "WEAKENING"

            reason = (
                "One directional thesis "
                "condition has weakened"
            )

        else:

            state = "VALID"

            reason = (
                "Directional thesis remains intact"
            )

        return (
            state,
            invalid,
            warning,
            reason,
        )


    # ======================================================
    # EXIT MANAGEMENT
    # ======================================================

    def evaluate_exit_management(
        self,
        stock_results,
    ):

        if not self._tracked_alert_positions:

            self.log_message(
                "No alert-tracked setups to "
                "evaluate for exits."
            )

            return pd.DataFrame()

        active_positions = [
            position
            for position
            in self._tracked_alert_positions
            .values()
            if (
                str(
                    position.get(
                        "asset_type",
                        "OPTION",
                    )
                    or "OPTION"
                ).upper()
                == "OPTION"
                and self._normalize_lifecycle_status(
                    position.get(
                        "status",
                        "ALERTED",
                    )
                )
                in self._lifecycle_exit_managed_statuses()
            )
        ]

        if not active_positions:

            self.log_message(
                "No active alert-tracked setups "
                "to evaluate for exits."
            )

            return pd.DataFrame()

        symbols = sorted(
            {
                contract
                for position
                in active_positions
                for contract
                in [
                    str(
                        position.get(
                            "long_contract",
                            "",
                        )
                    ),
                    str(
                        position.get(
                            "short_contract",
                            "",
                        )
                    ),
                ]
                if contract
            }
        )

        try:

            snapshots = (
                self._get_option_snapshots(
                    symbols
                )
            )

        except Exception as exc:

            self.log_message(
                "Exit management option "
                f"snapshot lookup failed: {exc}"
            )

            return pd.DataFrame()

        stock_metrics = {}

        if (
            stock_results is not None
            and not stock_results.empty
        ):

            for _, row in (
                stock_results.iterrows()
            ):

                stock_metrics[
                    str(
                        row[
                            "symbol"
                        ]
                    )
                ] = row

        today = self.get_datetime().date()

        rows = []

        for position in active_positions:

            long_symbol = str(
                position[
                    "long_contract"
                ]
            )

            short_symbol = str(
                position.get(
                    "short_contract",
                    "",
                )
                or ""
            )

            long_snapshot = (
                snapshots.get(
                    long_symbol
                )
            )

            if long_snapshot is None:

                continue

            long_quote = getattr(
                long_snapshot,
                "latest_quote",
                None,
            )

            if long_quote is None:

                self.log_message(
                    f"{position['underlying']}: "
                    "exit management skipped; "
                    "long-leg quote missing."
                )

                continue

            (
                long_quote_fresh,
                long_quote_age_seconds,
            ) = self._is_option_quote_fresh(
                long_quote
            )

            if not long_quote_fresh:

                age_text = (
                    "unknown"
                    if long_quote_age_seconds
                    is None
                    else
                    f"{long_quote_age_seconds:.1f}s"
                )

                self.log_message(
                    f"{position['underlying']}: "
                    "exit management skipped; "
                    "long-leg quote stale "
                    f"(age={age_text})."
                )

                continue

            try:

                long_bid = float(
                    long_quote.bid_price
                    or 0
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            if long_bid < 0:

                continue

            current_value = long_bid

            short_ask = None

            short_quote_age_seconds = None

            if short_symbol:

                short_snapshot = (
                    snapshots.get(
                        short_symbol
                    )
                )

                if short_snapshot is None:

                    continue

                short_quote = getattr(
                    short_snapshot,
                    "latest_quote",
                    None,
                )

                if short_quote is None:

                    self.log_message(
                        f"{position['underlying']}: "
                        "exit management skipped; "
                        "short-leg quote missing."
                    )

                    continue

                (
                    short_quote_fresh,
                    short_quote_age_seconds,
                ) = self._is_option_quote_fresh(
                    short_quote
                )

                if not short_quote_fresh:

                    age_text = (
                        "unknown"
                        if short_quote_age_seconds
                        is None
                        else
                        f"{short_quote_age_seconds:.1f}s"
                    )

                    self.log_message(
                        f"{position['underlying']}: "
                        "exit management skipped; "
                        "short-leg quote stale "
                        f"(age={age_text})."
                    )

                    continue

                try:

                    short_ask = float(
                        short_quote.ask_price
                        or 0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue

                if short_ask < 0:

                    continue

                current_value = max(
                    0.0,
                    long_bid
                    - short_ask,
                )

            entry_debit = float(
                position[
                    "entry_debit_per_share"
                ]
            )

            if entry_debit <= 0:
                continue

            planned_quantity = int(
                position[
                    "quantity"
                ]
            )

            try:

                broker_open_quantity = float(
                    position.get(
                        "broker_open_quantity",
                        0.0,
                    )
                    or 0.0
                )

            except (
                TypeError,
                ValueError,
            ):

                broker_open_quantity = 0.0

            quantity = (
                max(
                    1,
                    int(
                        round(
                            broker_open_quantity
                        )
                    ),
                )
                if broker_open_quantity > 0
                else planned_quantity
            )

            current_total_value = (
                current_value
                * 100
                * quantity
            )

            # Until the future actual-fill P/L phase, keep the
            # alert-estimated debit/share basis but scale it to
            # the broker-reconciled open quantity.
            entry_total_debit = (
                entry_debit
                * 100
                * quantity
            )

            pnl_dollars = (
                current_total_value
                - entry_total_debit
            )

            pnl_pct = (
                current_value
                / entry_debit
                - 1
            )

            try:

                expiration = (
                    date.fromisoformat(
                        str(
                            position[
                                "expiration"
                            ]
                        )
                    )
                )

            except ValueError:

                continue

            try:

                entry_date = (
                    date.fromisoformat(
                        str(
                            position[
                                "entry_date"
                            ]
                        )
                    )
                )

            except ValueError:

                try:

                    entry_date = (
                        datetime.fromisoformat(
                            str(
                                position[
                                    "entry_timestamp"
                                ]
                            )
                        )
                        .date()
                    )

                except ValueError:

                    entry_date = today

            dte = (
                expiration - today
            ).days

            days_held = max(
                0,
                (
                    today - entry_date
                ).days,
            )

            (
                thesis_state,
                thesis_invalid,
                thesis_warning,
                thesis_reason,
            ) = self._evaluate_tracked_thesis(
                position,
                stock_metrics,
            )

            close_reasons = []

            adjust_reasons = []

            if (
                pnl_pct
                >= self.parameters[
                    "exit_profit_target_pct"
                ]
            ):

                close_reasons.append(
                    "profit target reached"
                )

            if (
                pnl_pct
                <= -self.parameters[
                    "exit_max_loss_pct"
                ]
            ):

                close_reasons.append(
                    "maximum loss threshold reached"
                )

            if dte <= self.parameters[
                "exit_dte_days"
            ]:

                close_reasons.append(
                    "hard DTE exit reached"
                )

            if (
                days_held
                >= self.parameters[
                    "exit_max_holding_days"
                ]
            ):

                close_reasons.append(
                    "maximum holding period reached"
                )

            if thesis_invalid:

                close_reasons.append(
                    "directional thesis invalidated"
                )

            if not close_reasons:

                if (
                    dte
                    <= self.parameters[
                        "adjust_dte_days"
                    ]
                ):

                    adjust_reasons.append(
                        "approaching DTE exit; "
                        "review roll/close"
                    )

                if thesis_warning:

                    adjust_reasons.append(
                        "directional thesis weakening"
                    )

            if close_reasons:

                action = "CLOSE"

                reason = "; ".join(
                    close_reasons
                )

            elif adjust_reasons:

                action = "ADJUST"

                reason = "; ".join(
                    adjust_reasons
                )

            else:

                action = "HOLD"

                reason = (
                    "No exit or adjustment "
                    "condition triggered"
                )

            rows.append(
                {
                    "position_id":
                        position[
                            "id"
                        ],

                    "underlying":
                        position[
                            "underlying"
                        ],

                    "direction":
                        position[
                            "direction"
                        ],

                    "decision":
                        position[
                            "decision"
                        ],

                    "quantity":
                        quantity,

                    "long_contract":
                        long_symbol,

                    "short_contract":
                        short_symbol,

                    "entry_debit":
                        entry_debit,

                    "current_value":
                        current_value,

                    "pnl_pct":
                        pnl_pct,

                    "pnl_dollars":
                        pnl_dollars,

                    "dte":
                        dte,

                    "days_held":
                        days_held,

                    "quote_age_seconds":
                        max(
                            [
                                age
                                for age
                                in [
                                    long_quote_age_seconds,
                                    short_quote_age_seconds,
                                ]
                                if age is not None
                            ]
                            or [
                                long_quote_age_seconds
                                or 0.0
                            ]
                        ),

                    "thesis_state":
                        thesis_state,

                    "thesis_reason":
                        thesis_reason,

                    "action":
                        action,

                    "reason":
                        reason,
                }
            )

        return pd.DataFrame(
            rows
        )


    # ======================================================
    # EXIT MANAGEMENT OUTPUT
    # ======================================================

    def log_exit_management(
        self,
        exit_results,
    ):

        if exit_results.empty:

            self.log_message(
                "No exit-management results "
                "were available."
            )

            return

        display = exit_results[
            [
                "underlying",
                "direction",
                "decision",
                "quantity",
                "entry_debit",
                "current_value",
                "pnl_pct",
                "pnl_dollars",
                "dte",
                "days_held",
                "quote_age_seconds",
                "thesis_state",
                "action",
                "reason",
            ]
        ].copy()

        display[
            "entry_debit"
        ] = (
            pd.to_numeric(
                display[
                    "entry_debit"
                ],
                errors="coerce",
            )
            .round(2)
        )

        display[
            "current_value"
        ] = (
            pd.to_numeric(
                display[
                    "current_value"
                ],
                errors="coerce",
            )
            .round(2)
        )

        display[
            "pnl_pct"
        ] = (
            pd.to_numeric(
                display[
                    "pnl_pct"
                ],
                errors="coerce",
            )
            * 100
        ).round(1)

        display[
            "pnl_dollars"
        ] = (
            pd.to_numeric(
                display[
                    "pnl_dollars"
                ],
                errors="coerce",
            )
            .round(2)
        )

        display = display.rename(
            columns={
                "entry_debit":
                    "entry_debit/share",
                "current_value":
                    "exit_value/share",
                "pnl_pct":
                    "pnl_%",
                "pnl_dollars":
                    "pnl_$",
                "days_held":
                    "held_days",
                "quote_age_seconds":
                    "quote_age_s",
            }
        )

        display[
            "quote_age_s"
        ] = (
            pd.to_numeric(
                display[
                    "quote_age_s"
                ],
                errors="coerce",
            )
            .round(1)
        )

        self.log_message(
            "\n\n"
            "===== EXIT MANAGEMENT =====\n"
            + display.to_string(
                index=False
            )
            + "\n"
            "==========================="
        )


    # ======================================================
    # CLOSE / ADJUST ALERTS
    # ======================================================

    def generate_exit_alerts(
        self,
        exit_results,
    ):

        if exit_results.empty:

            return []

        today = str(
            self.get_datetime().date()
        )

        alerts = []

        actionable = exit_results[
            exit_results[
                "action"
            ].isin(
                [
                    "CLOSE",
                    "ADJUST",
                ]
            )
        ]

        for _, row in (
            actionable.iterrows()
        ):

            position_id = str(
                row[
                    "position_id"
                ]
            )

            position = (
                self._tracked_alert_positions
                .get(
                    position_id
                )
            )

            if position is None:
                continue

            action = str(
                row[
                    "action"
                ]
            )

            already_alerted = (
                self.parameters[
                    "exit_alert_once_per_day"
                ]
                and str(
                    position.get(
                        "last_exit_alert_date",
                        "",
                    )
                )
                == today
                and str(
                    position.get(
                        "last_exit_alert_action",
                        "",
                    )
                )
                == action
            )

            if already_alerted:
                continue

            short_contract = str(
                row.get(
                    "short_contract",
                    "",
                )
                or ""
            )

            legs = str(
                row[
                    "long_contract"
                ]
            )

            if short_contract:

                legs += (
                    " / "
                    + short_contract
                )

            payload = {
                "timestamp":
                    self.get_datetime()
                    .isoformat(),

                "lifecycle_id":
                    position_id,

                "lifecycle_status_before":
                    self._normalize_lifecycle_status(
                        position.get(
                            "status",
                            "ALERTED",
                        )
                    ),

                "action":
                    action,

                "underlying":
                    str(
                        row[
                            "underlying"
                        ]
                    ),

                "direction":
                    str(
                        row[
                            "direction"
                        ]
                    ),

                "decision":
                    str(
                        row[
                            "decision"
                        ]
                    ),

                "quantity":
                    int(
                        row[
                            "quantity"
                        ]
                    ),

                "legs":
                    legs,

                "entry_debit_per_share":
                    float(
                        row[
                            "entry_debit"
                        ]
                    ),

                "estimated_exit_value_per_share":
                    float(
                        row[
                            "current_value"
                        ]
                    ),

                "estimated_pnl_pct":
                    float(
                        row[
                            "pnl_pct"
                        ]
                    ),

                "estimated_pnl_dollars":
                    float(
                        row[
                            "pnl_dollars"
                        ]
                    ),

                "dte":
                    int(
                        row[
                            "dte"
                        ]
                    ),

                "days_held":
                    int(
                        row[
                            "days_held"
                        ]
                    ),

                "thesis_state":
                    str(
                        row[
                            "thesis_state"
                        ]
                    ),

                "reason":
                    str(
                        row[
                            "reason"
                        ]
                    ),

                "mode":
                    "ALERT_ONLY_NO_ORDER",
            }

            self.log_message(
                "\n\n"
                "========== EXIT ALERT ==========\n"
                f"{payload['action']} | "
                f"{payload['underlying']} | "
                f"{payload['decision']}\n"
                f"Lifecycle: {payload['lifecycle_id']} "
                f"[{payload['lifecycle_status_before']}]\n"
                f"Contracts: "
                f"{payload['quantity']}\n"
                f"Legs: "
                f"{payload['legs']}\n"
                f"Entry debit/share: "
                f"${payload['entry_debit_per_share']:.2f}\n"
                f"Estimated exit value/share: "
                f"${payload['estimated_exit_value_per_share']:.2f}\n"
                f"Estimated P/L: "
                f"{payload['estimated_pnl_pct'] * 100:.1f}% "
                f"(${payload['estimated_pnl_dollars']:,.2f})\n"
                f"DTE: "
                f"{payload['dte']}\n"
                f"Held: "
                f"{payload['days_held']} day(s)\n"
                f"Thesis: "
                f"{payload['thesis_state']}\n"
                f"Reason: "
                f"{payload['reason']}\n"
                "MODE: ALERT ONLY - NO ORDER SUBMITTED\n"
                "================================"
            )

            self._append_exit_alert_jsonl(
                payload
            )

            position[
                "last_exit_alert_date"
            ] = today

            position[
                "last_exit_alert_action"
            ] = action

            position[
                "pending_management_action"
            ] = action

            if action == "CLOSE":

                self._transition_trade_lifecycle(
                    position,
                    "CLOSE_ALERTED",
                    "Exit-management close alert emitted; "
                    "awaiting broker reconciliation",
                    details={
                        "estimated_pnl_pct": payload[
                            "estimated_pnl_pct"
                        ],
                        "reason": payload[
                            "reason"
                        ],
                    },
                )

            else:

                self._record_lifecycle_event(
                    position,
                    "ADJUST_ALERT_EMITTED",
                    payload[
                        "reason"
                    ],
                    details={
                        "estimated_pnl_pct": payload[
                            "estimated_pnl_pct"
                        ],
                    },
                )

            self._tracked_alert_positions[
                position_id
            ] = position

            self._save_trade_alert_positions_state()

            alerts.append(
                payload
            )

        if alerts:

            self.log_message(
                f"Generated {len(alerts)} "
                "close/adjust alert(s)."
            )

        return alerts


    # ======================================================
    # EXIT MANAGEMENT PIPELINE
    # ======================================================

    def run_exit_management(
        self,
        stock_results,
    ):

        self.log_message(
            "Evaluating profit target, max loss, "
            "DTE/time exits, and thesis state "
            "for alert-tracked setups..."
        )

        exit_results = (
            self.evaluate_exit_management(
                stock_results
            )
        )

        self.log_exit_management(
            exit_results
        )

        exit_alerts = (
            self.generate_exit_alerts(
                exit_results
            )
        )

        return (
            exit_results,
            exit_alerts,
        )


    # ======================================================
    # OPTIONAL JSONL ALERT OUTPUT
    # ======================================================

    def _append_trade_alert_jsonl(
        self,
        payload,
    ):

        if not self.trade_alerts_jsonl_path:
            return

        try:

            with open(
                self.trade_alerts_jsonl_path,
                "a",
                encoding="utf-8",
            ) as handle:

                handle.write(
                    json.dumps(
                        payload,
                        default=str,
                        sort_keys=True,
                    )
                    + "\n"
                )

        except Exception as exc:

            self.log_message(
                "Could not write trade alert "
                f"JSONL file: {exc}"
            )


    # ======================================================
    # ALERT GENERATION
    # ======================================================

    def generate_trade_alerts(
        self,
        sized,
        account,
    ):

        if not self.trade_alerts_enabled:

            self.log_message(
                "Trade alert generation is "
                "disabled."
            )

            return []

        if sized.empty:

            self.log_message(
                "No sized structures available "
                "for alerts."
            )

            return []

        actionable = (
            sized[
                sized[
                    "alert_status"
                ]
                == "ALERT"
            ]
            .sort_values(
                "structure_score",
                ascending=False,
            )
        )

        if actionable.empty:

            self.log_message(
                "No actionable trade alerts "
                "were generated."
            )

            return []

        today = self.get_datetime().date()

        alerts = []

        for _, row in (
            actionable.iterrows()
        ):

            key = (
                str(today),
                str(
                    row[
                        "underlying"
                    ]
                ),
                str(
                    row[
                        "decision"
                    ]
                ),
                str(
                    row[
                        "long_contract"
                    ]
                ),
                str(
                    row.get(
                        "short_contract",
                        "",
                    )
                    or ""
                ),
                int(
                    row[
                        "quantity"
                    ]
                ),
            )

            lifecycle_id = (
                self._alert_position_id(
                    key
                )
            )

            already_sent = (
                self.parameters[
                    "alert_once_per_day"
                ]
                and key
                in self._sent_trade_alert_keys
            )

            if already_sent:

                self._register_alert_tracked_setup(
                    row,
                    key,
                    "RECONSTRUCTED_SAME_DAY",
                )

                continue

            quantity = int(
                row[
                    "quantity"
                ]
            )

            total_risk = float(
                row[
                    "total_max_risk"
                ]
            )

            total_debit = float(
                row[
                    "total_debit"
                ]
            )

            raw_total_reward = row.get(
                "total_max_reward",
                None,
            )

            if raw_total_reward is None:

                reward_text = "N/A"

            else:

                try:

                    reward_number = float(
                        raw_total_reward
                    )

                    if math.isinf(
                        reward_number
                    ):

                        reward_text = (
                            "UNLIMITED"
                        )

                    elif math.isnan(
                        reward_number
                    ):

                        reward_text = "N/A"

                    else:

                        reward_text = (
                            f"${reward_number:,.2f}"
                        )

                except (
                    TypeError,
                    ValueError,
                ):

                    reward_text = "N/A"

            short_contract = str(
                row.get(
                    "short_contract",
                    "",
                )
                or ""
            )

            legs = (
                str(
                    row[
                        "long_contract"
                    ]
                )
            )

            if short_contract:

                legs += (
                    " / "
                    + short_contract
                )

            payload = {
                "timestamp":
                    self.get_datetime()
                    .isoformat(),

                "lifecycle_id":
                    lifecycle_id,

                "lifecycle_status":
                    "ALERTED",

                "underlying":
                    str(
                        row[
                            "underlying"
                        ]
                    ),

                "direction":
                    str(
                        row[
                            "direction"
                        ]
                    ),

                "decision":
                    str(
                        row[
                            "decision"
                        ]
                    ),

                "quantity":
                    quantity,

                "long_contract":
                    str(
                        row[
                            "long_contract"
                        ]
                    ),

                "short_contract":
                    short_contract,

                "expiration":
                    str(
                        row[
                            "expiration"
                        ]
                    ),

                "debit_per_share":
                    float(
                        row[
                            "net_debit"
                        ]
                    ),

                "estimated_total_debit":
                    total_debit,

                "max_risk":
                    total_risk,

                "max_reward":
                    (
                        "UNLIMITED"
                        if reward_text
                        == "UNLIMITED"
                        else raw_total_reward
                    ),

                "breakeven":
                    float(
                        row[
                            "breakeven"
                        ]
                    ),

                "structure_score":
                    float(
                        row[
                            "structure_score"
                        ]
                    ),

                "option_daily_volume":
                    float(
                        row.get(
                            "daily_volume",
                            0.0,
                        )
                        or 0.0
                    ),

                "option_daily_activity_status":
                    str(
                        row.get(
                            "daily_activity_status",
                            "UNKNOWN",
                        )
                        or "UNKNOWN"
                    ),

                "iv_percentile":
                    (
                        None
                        if (
                            row.get(
                                "iv_percentile",
                                None,
                            )
                            is None
                            or pd.isna(
                                row.get(
                                    "iv_percentile",
                                    None,
                                )
                            )
                        )
                        else float(
                            row.get(
                                "iv_percentile"
                            )
                        )
                    ),

                "iv_rank":
                    (
                        None
                        if (
                            row.get(
                                "iv_rank",
                                None,
                            )
                            is None
                            or pd.isna(
                                row.get(
                                    "iv_rank",
                                    None,
                                )
                            )
                        )
                        else float(
                            row.get(
                                "iv_rank"
                            )
                        )
                    ),

                "iv_history_samples":
                    int(
                        row.get(
                            "iv_history_samples",
                            0,
                        )
                        or 0
                    ),

                "event_risk":
                    str(
                        row.get(
                            "event_risk",
                            "UNKNOWN",
                        )
                    ),

                "risk_pct_equity":
                    float(
                        row[
                            "risk_pct_equity"
                        ]
                    ),

                "account_equity":
                    float(
                        account.get(
                            "equity",
                            0,
                        )
                        or 0
                    ),

                "options_buying_power":
                    float(
                        account.get(
                            "options_buying_power",
                            0,
                        )
                        or 0
                    ),

                "portfolio_tracked_risk_before":
                    float(
                        account.get(
                            "portfolio_tracked_risk_before",
                            0,
                        )
                        or 0
                    ),

                "portfolio_projected_risk_after":
                    float(
                        account.get(
                            "portfolio_projected_risk_after",
                            0,
                        )
                        or 0
                    ),

                "portfolio_projected_active_setups_after":
                    int(
                        account.get(
                            "portfolio_projected_active_setups_after",
                            0,
                        )
                        or 0
                    ),

                "portfolio_released_alert_reservations_before":
                    int(
                        account.get(
                            "tracked_option_exposure",
                            {},
                        ).get(
                            "released_alert_reservation_count",
                            0,
                        )
                        or 0
                    ),

                "broker_option_gross_market_value":
                    float(
                        account.get(
                            "broker_option_exposure",
                            {},
                        ).get(
                            "option_gross_market_value",
                            0,
                        )
                        or 0
                    ),

                "broker_option_gross_pct_equity":
                    float(
                        account.get(
                            "broker_option_exposure",
                            {},
                        ).get(
                            "option_gross_pct_equity",
                            0,
                        )
                        or 0
                    ),

                "alert_risk_reservation_minutes":
                    float(
                        self.parameters.get(
                            "lifecycle_alert_risk_reservation_minutes",
                            60.0,
                        )
                    ),

                "mode":
                    "ALERT_ONLY_NO_ORDER",
            }

            alert_message = (
                "\n\n"
                "========== TRADE ALERT ==========\n"
                f"{payload['direction']} | "
                f"{payload['underlying']} | "
                f"{payload['decision']}\n"
                f"Lifecycle: {payload['lifecycle_id']} "
                f"[{payload['lifecycle_status']}]\n"
                f"Contracts: {quantity}\n"
                f"Legs: {legs}\n"
                f"Expiration: "
                f"{payload['expiration']}\n"
                f"Debit/share: "
                f"${payload['debit_per_share']:.2f}\n"
                f"Estimated debit: "
                f"${total_debit:,.2f}\n"
                f"Maximum risk: "
                f"${total_risk:,.2f}\n"
                f"Maximum reward: "
                f"{reward_text}\n"
                f"Breakeven: "
                f"${payload['breakeven']:.2f}\n"
                f"Structure score: "
                f"{payload['structure_score']:.1f}\n"
                f"Option day volume: "
                f"{payload['option_daily_volume']:.0f} "
                f"[{payload['option_daily_activity_status']}]\n"
                + (
                    f"IV percentile/rank: "
                    f"{payload['iv_percentile'] * 100:.1f}% / "
                    f"{payload['iv_rank'] * 100:.1f}% "
                    f"(n={payload['iv_history_samples']})\n"
                    if (
                        payload["iv_percentile"] is not None
                        and payload["iv_rank"] is not None
                    )
                    else (
                        f"IV context: warming up "
                        f"(n={payload['iv_history_samples']})\n"
                    )
                )
                + f"Event risk: {payload['event_risk']}\n"
                + f"Account risk: "
                f"{payload['risk_pct_equity'] * 100:.3f}%\n"
                + f"Projected risk-reserved option risk: "
                f"${payload['portfolio_projected_risk_after']:,.2f} "
                f"across "
                f"{payload['portfolio_projected_active_setups_after']} "
                f"reserved setup(s)\n"
                + f"Released unmatched alert reservations: "
                f"{payload['portfolio_released_alert_reservations_before']}\n"
                + f"Alert risk reservation window: "
                f"{payload['alert_risk_reservation_minutes']:.0f} minutes "
                "unless broker/order evidence appears\n"
                + f"Broker option gross: "
                f"${payload['broker_option_gross_market_value']:,.2f} "
                f"({payload['broker_option_gross_pct_equity'] * 100:.2f}% equity)\n"
                "MODE: ALERT ONLY - NO ORDER SUBMITTED\n"
                "================================="
            )

            self.log_message(
                alert_message
            )

            self._append_trade_alert_jsonl(
                payload
            )

            if self.parameters[
                "alert_once_per_day"
            ]:

                self._sent_trade_alert_keys.add(
                    key
                )

                self._save_trade_alert_dedupe_state()

            self._register_alert_tracked_setup(
                row,
                key,
                "ALERT_ESTIMATE",
            )

            alerts.append(
                payload
            )

        self.log_message(
            f"Generated {len(alerts)} "
            "actionable trade alert(s)."
        )

        return alerts


    # ======================================================
    # POSITION SIZING + ALERT PIPELINE
    # ======================================================

    def run_position_sizing_and_alerts(
        self,
        bullish_structures,
        bearish_structures,
    ):

        self.log_message(
            "Sizing positions and generating "
            "read-only trade alerts..."
        )

        (
            sized,
            account,
        ) = self.size_trade_structures(
            bullish_structures,
            bearish_structures,
        )

        self.log_position_sizing(
            sized
        )

        alerts = self.generate_trade_alerts(
            sized,
            account,
        )

        return (
            sized,
            alerts,
        )


    # ======================================================
    # OPTIONS SCANNER
    # ======================================================

    def scan_options(
        self,
        bullish,
        bearish,
    ):

        self.log_message(
            "Evaluating options for top "
            "stock candidates..."
        )

        event_symbols = set()

        for frame in (
            bullish,
            bearish,
        ):

            if (
                frame is not None
                and not frame.empty
                and "symbol" in frame.columns
            ):

                event_symbols.update(
                    frame["symbol"]
                    .astype(str)
                    .tolist()
                )

        self._refresh_event_risk_context(
            sorted(event_symbols)
        )

        bullish_options = (
            self.rank_option_candidates(
                bullish,
                ContractType.CALL,
            )
        )

        bearish_options = (
            self.rank_option_candidates(
                bearish,
                ContractType.PUT,
            )
        )

        self.log_option_candidates(
            bullish_options,
            "TOP BULLISH CALL CANDIDATES",
        )

        self.log_option_candidates(
            bearish_options,
            "TOP BEARISH PUT CANDIDATES",
        )

        return (
            bullish_options,
            bearish_options,
        )
    # ======================================================
    # MAIN SCANNER
    # ======================================================

    def on_trading_iteration(self):

        # --------------------------------------------------
        # OPTIONS MARKET-SESSION GATE
        # --------------------------------------------------
        #
        # The LumiBot strategy can remain on MARKET=24/7,
        # but no stock/options entry or exit decisions are
        # evaluated outside the actionable options window.
        # --------------------------------------------------

        session_status = (
            self._get_options_session_status()
        )

        if not session_status[
            "allowed"
        ]:

            self.sleeptime = (
                self.parameters[
                    "options_closed_retry_sleeptime"
                ]
            )

            market_now = (
                session_status.get(
                    "now"
                )
            )

            now_text = (
                "unknown"
                if market_now is None
                else market_now.strftime(
                    "%Y-%m-%d %I:%M:%S %p ET"
                )
            )

            actionable_open = (
                session_status.get(
                    "actionable_open"
                )
            )

            actionable_close = (
                session_status.get(
                    "actionable_close"
                )
            )

            if (
                actionable_open is not None
                and actionable_close is not None
            ):

                window_text = (
                    actionable_open.strftime(
                        "%I:%M %p ET"
                    )
                    + " - "
                    + actionable_close.strftime(
                        "%I:%M %p ET"
                    )
                )

            else:

                window_text = "not currently available"

            self.log_message(
                "MARKET SESSION GATE: CLOSED. "
                f"Market time={now_text}. "
                f"Reason={session_status['reason']}. "
                f"Actionable window={window_text}. "
                "Skipping stock/options entries and "
                "exit-management P/L signals. "
                "Retry cadence="
                f"{self.sleeptime}."
            )

            return

        self.sleeptime = (
            self.parameters[
                "options_active_sleeptime"
            ]
        )

        market_now = (
            session_status[
                "now"
            ]
        )

        actionable_open = (
            session_status[
                "actionable_open"
            ]
        )

        actionable_close = (
            session_status[
                "actionable_close"
            ]
        )

        if (
            actionable_open is not None
            and actionable_close is not None
        ):

            actionable_window_text = (
                actionable_open.strftime(
                    "%I:%M %p ET"
                )
                + " - "
                + actionable_close.strftime(
                    "%I:%M %p ET"
                )
            )

        else:

            actionable_window_text = (
                "gate disabled"
            )

        self.log_message(
            "MARKET SESSION GATE: OPEN. "
            f"Market time="
            f"{market_now.strftime('%I:%M:%S %p ET')}. "
            f"Actionable window="
            f"{actionable_window_text}. "
            f"Max option quote age="
            f"{self.parameters['option_quote_max_age_seconds']}s."
        )

        # --------------------------------------------------
        # RECONCILE PERSISTENT TRADE LIFECYCLES
        # --------------------------------------------------

        try:

            (
                lifecycle_results,
                lifecycle_snapshot,
            ) = self.reconcile_trade_lifecycle_states()

            self.log_trade_lifecycle_reconciliation(
                lifecycle_results,
                lifecycle_snapshot,
            )

        except Exception as exc:

            self.log_message(
                "Trade lifecycle reconciliation failed; "
                "prior persistent states are retained. "
                f"Reason={exc}"
            )

        # --------------------------------------------------
        # BUILD THE UNIVERSE AUTOMATICALLY
        # --------------------------------------------------

        try:

            symbols = self.build_universe()

        except Exception as exc:

            self.log_message(
                f"Universe generation failed: {exc}"
            )

            return

        if not symbols:

            self.log_message(
                "Automatic universe was empty."
            )

            return

        self.log_message(
            f"Evaluating {len(symbols)} stocks..."
        )

        # --------------------------------------------------
        # GET HISTORICAL DATA
        # --------------------------------------------------

        histories = (
            self.get_historical_prices_for_assets(
                symbols,
                65,
                timestep="day",

                # Keep requests manageable.
                chunk_size=100,

                # Don't launch hundreds of workers.
                max_workers=10,
            )
        )

        rows = []

        today = self.get_datetime().date()

        for asset, bars in histories.items():

            if bars is None:
                continue

            df = bars.pandas_df.copy()

            # --------------------------------------------------
            # IGNORE TODAY'S INCOMPLETE DAILY CANDLE
            # --------------------------------------------------

            if (
                len(df) > 0
                and df.index[-1].date() == today
            ):
                df = df.iloc[:-1]

            if len(df) < 61:
                continue

            # --------------------------------------------------
            # BASIC DATA
            # --------------------------------------------------

            close = df["close"].astype(float)

            volume = df["volume"].astype(float)

            price = close.iloc[-1]

            # --------------------------------------------------
            # MOVING AVERAGES
            # --------------------------------------------------

            sma20 = close.tail(20).mean()

            sma50 = close.tail(50).mean()

            # --------------------------------------------------
            # MOMENTUM
            # --------------------------------------------------

            momentum20 = (
                price
                / close.iloc[-21]
                - 1
            )

            momentum60 = (
                price
                / close.iloc[-61]
                - 1
            )

            # --------------------------------------------------
            # VOLATILITY
            # --------------------------------------------------

            volatility20 = (
                close
                .pct_change()
                .tail(20)
                .std()
            )

            # --------------------------------------------------
            # VOLUME
            # --------------------------------------------------

            avg_volume20 = (
                volume
                .tail(20)
                .mean()
            )

            avg_dollar_volume20 = (
                close.tail(20)
                * volume.tail(20)
            ).mean()

            relative_volume = (
                volume.iloc[-1]
                / avg_volume20
                if avg_volume20 > 0
                else 0
            )

            # --------------------------------------------------
            # TREND
            # --------------------------------------------------

            bullish_trend = (
                1
                if price > sma20 > sma50
                else 0
            )

            bearish_trend = (
                1
                if price < sma20 < sma50
                else 0
            )

            # --------------------------------------------------
            # SYMBOL
            # --------------------------------------------------

            symbol = (
                asset.symbol
                if hasattr(asset, "symbol")
                else str(asset)
            )

            rows.append({
                "symbol": symbol,
                "price": price,

                "sma20": sma20,
                "sma50": sma50,

                "momentum20": momentum20,
                "momentum60": momentum60,

                "relative_volume": relative_volume,

                "avg_volume20": avg_volume20,

                "avg_dollar_volume20":
                    avg_dollar_volume20,

                "volatility20":
                    volatility20,

                "bullish_trend":
                    bullish_trend,

                "bearish_trend":
                    bearish_trend,
            })

        # --------------------------------------------------
        # CHECK DATA
        # --------------------------------------------------

        if not rows:

            self.log_message(
                "No stocks had enough historical data."
            )

            return

        results = pd.DataFrame(rows)

        self.log_message(
            f"Historical data available for "
            f"{len(results)} stocks."
        )

        # ======================================================
        # BASE ELIGIBILITY
        # ======================================================

        eligible = results[
            (
                results["price"]
                >= self.parameters[
                    "minimum_price"
                ]
            )
            &
            (
                results[
                    "avg_dollar_volume20"
                ]
                >= self.parameters[
                    "minimum_dollar_volume"
                ]
            )
            &
            (
                results[
                    "relative_volume"
                ]
                >= self.parameters[
                    "minimum_relative_volume"
                ]
            )
        ].copy()

        self.log_message(
            f"{len(eligible)} of "
            f"{len(results)} stocks passed "
            f"liquidity/activity filters."
        )

        if eligible.empty:

            self.log_message(
                "No eligible stocks today."
            )

            return

        # ======================================================
        # BULLISH
        # ======================================================

        bullish = eligible[
            eligible["momentum20"]
            >= self.parameters[
                "bullish_momentum_threshold"
            ]
        ].copy()

        bullish_count = len(bullish)

        if not bullish.empty:

            bullish["momentum20_score"] = (
                bullish["momentum20"]
                .rank(pct=True)
            )

            bullish["momentum60_score"] = (
                bullish["momentum60"]
                .rank(pct=True)
            )

            bullish["volume_score"] = (
                bullish["relative_volume"]
                .rank(pct=True)
            )

            bullish["volatility_score"] = (
                1
                - bullish["volatility20"]
                .rank(pct=True)
            )

            bullish["score"] = 100 * (

                bullish[
                    "momentum20_score"
                ] * 0.30

                + bullish[
                    "momentum60_score"
                ] * 0.25

                + bullish[
                    "volume_score"
                ] * 0.20

                + bullish[
                    "volatility_score"
                ] * 0.10

                + bullish[
                    "bullish_trend"
                ] * 0.15
            )

            bullish = (
                bullish
                .sort_values(
                    "score",
                    ascending=False,
                )
                .head(
                    self.parameters[
                        "top_results"
                    ]
                )
            )

        # ======================================================
        # BEARISH
        # ======================================================

        bearish = eligible[
            eligible["momentum20"]
            <= self.parameters[
                "bearish_momentum_threshold"
            ]
        ].copy()

        bearish_count = len(bearish)

        if not bearish.empty:

            bearish["momentum20_score"] = (
                (-bearish["momentum20"])
                .rank(pct=True)
            )

            bearish["momentum60_score"] = (
                (-bearish["momentum60"])
                .rank(pct=True)
            )

            bearish["volume_score"] = (
                bearish["relative_volume"]
                .rank(pct=True)
            )

            bearish["volatility_score"] = (
                1
                - bearish["volatility20"]
                .rank(pct=True)
            )

            bearish["score"] = 100 * (

                bearish[
                    "momentum20_score"
                ] * 0.30

                + bearish[
                    "momentum60_score"
                ] * 0.25

                + bearish[
                    "volume_score"
                ] * 0.20

                + bearish[
                    "volatility_score"
                ] * 0.10

                + bearish[
                    "bearish_trend"
                ] * 0.15
            )

            bearish = (
                bearish
                .sort_values(
                    "score",
                    ascending=False,
                )
                .head(
                    self.parameters[
                        "top_results"
                    ]
                )
            )

        # ======================================================
        # NEUTRAL
        # ======================================================

        neutral = eligible[
            (
                eligible["momentum20"]
                >
                self.parameters[
                    "bearish_momentum_threshold"
                ]
            )
            &
            (
                eligible["momentum20"]
                <
                self.parameters[
                    "bullish_momentum_threshold"
                ]
            )
        ].copy()

        neutral_count = len(neutral)

        # ======================================================
        # SUMMARY
        # ======================================================

        self.log_message(
            f"Classification: "
            f"{bullish_count} bullish, "
            f"{bearish_count} bearish, "
            f"{neutral_count} neutral."
        )

        # ======================================================
        # BULLISH OUTPUT
        # ======================================================

        if not bullish.empty:

            bullish_display = (
                bullish[
                    [
                        "symbol",
                        "price",
                        "score",
                        "momentum20",
                        "momentum60",
                        "relative_volume",
                        "avg_dollar_volume20",
                    ]
                ]
                .copy()
            )

            self.log_message(
                "\n\n"
                "===== TOP BULLISH CANDIDATES =====\n"
                + bullish_display.to_string(
                    index=False
                )
                + "\n"
                "=================================="
            )

        else:

            self.log_message(
                "No bullish candidates today."
            )

        # ======================================================
        # BEARISH OUTPUT
        # ======================================================

        if not bearish.empty:

            bearish_display = (
                bearish[
                    [
                        "symbol",
                        "price",
                        "score",
                        "momentum20",
                        "momentum60",
                        "relative_volume",
                        "avg_dollar_volume20",
                    ]
                ]
                .copy()
            )

            self.log_message(
                "\n\n"
                "===== TOP BEARISH CANDIDATES =====\n"
                + bearish_display.to_string(
                    index=False
                )
                + "\n"
                "=================================="
            )

        else:

            self.log_message(
                "No bearish candidates today."
            )

        # ==================================================
        # ACCOUNT-SIZE ROUTING
        # ==================================================

        try:

            micro_context = (
                self._get_micro_account_context()
            )

        except Exception as exc:

            self.log_message(
                "Could not determine account "
                f"routing mode: {exc}"
            )

            return

        if micro_context[
            "active"
        ]:

            try:

                self.run_micro_account_mode(
                    bullish,
                    micro_context,
                )

                # If this strategy already has older
                # alert-tracked option setups, continue to
                # monitor their exits even while NEW entries
                # route through micro fractional-stock mode.
                if self._tracked_alert_positions:

                    self.run_exit_management(
                        results
                    )

            except Exception as exc:

                self.log_message(
                    "Micro-account fractional "
                    f"stock mode failed: {exc}"
                )

            return

        self.log_message(
            "MICRO ACCOUNT MODE INACTIVE: "
            f"effective equity="
            f"${micro_context['effective_equity']:,.2f}; "
            f"{micro_context['mode_reason']}. "
            "Continuing to options analysis."
        )

        # ==================================================
        # OPTIONS ELIGIBILITY + CONTRACT RANKING
        # ==================================================

        try:

            (
                bullish_options,
                bearish_options,
            ) = self.scan_options(
                bullish,
                bearish,
            )

            (
                bullish_structures,
                bearish_structures,
            ) = self.scan_trade_structures(
                bullish,
                bearish,
                bullish_options,
                bearish_options,
            )

            self.run_position_sizing_and_alerts(
                bullish_structures,
                bearish_structures,
            )

            self.run_exit_management(
                results
            )

        except Exception as exc:

            self.log_message(
                "Options eligibility/ranking "
                f"failed: {exc}"
            )