# strategy.py
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
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
    LimitOrderRequest,
    OptionLegRequest,
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
    OrderClass,
    OrderSide,
    PositionIntent,
    TimeInForce,
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

        # Allocate 10% of account equity to one fractional
        # stock idea to increase capital utilization in micro
        # accounts. With $100, this is $10 per idea.
        "micro_position_pct_equity": 0.10,

        # Hard dollar ceiling per micro position. $200 is 10%
        # of the $2,000 micro-account threshold, so the 10%
        # sizing rule can remain effective across the entire
        # micro-account range while still retaining a hard cap.
        "micro_max_position_dollars": 200.0,

        # Cap all NEW micro alerts in one run at 50% of
        # effective equity. With five 10% positions, up to
        # half the account can be deployed while retaining
        # half as undeployed capacity.
        "micro_total_allocation_pct_equity": 0.50,

        # Cumulative real-account stock exposure guard. New
        # micro alerts stop once broker stock gross market
        # value reaches 70% of effective equity. Set <= 0 to
        # disable this cap. Simulated test equity ignores it.
        "micro_max_broker_stock_gross_pct_equity": 0.70,

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
        # 10% position size * 6% stop ~= 0.6% account risk
        # per idea; five such ideas ~= 3% planned risk. The
        # 12% target preserves an approximate 2:1 target-to-
        # stop relationship.
        "micro_stop_loss_pct": 0.06,
        "micro_profit_target_pct": 0.12,

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
        # OPTIONAL ALPACA PAPER ENTRY EXECUTION
        # --------------------------------------------------

        # PAPER execution is opt-in at runtime and is hard-blocked
        # when ALPACA_IS_PAPER is false. The strategy submits only
        # DAY limit ENTRY orders in this phase; no live-account,
        # replace, cancel, close, or exit-order API is used here.
        "paper_execution_limit_price_round_decimals": 2,

        # Keep a second internal cap even though position sizing
        # already limits actionable alerts per run.
        "paper_execution_max_orders_per_run": 5,

        # --------------------------------------------------
        # OPTIONAL ALPACA PAPER EXIT EXECUTION
        # --------------------------------------------------

        # Exit execution is armed separately from entry execution so
        # upgrading the strategy cannot silently turn prior CLOSE
        # alerts into broker writes. Only PAPER DAY limit exits are
        # supported. A debit spread is closed for a credit using
        # Alpaca's signed MLEG convention (negative limit = credit).
        "paper_exit_execution_limit_price_round_decimals": 2,
        "paper_exit_execution_max_orders_per_run": 5,

        # --------------------------------------------------
        # ORDER LIFECYCLE HARDENING
        # --------------------------------------------------

        # Broker order fills and position snapshots can arrive a few
        # seconds apart. Do not immediately ORPHAN a lifecycle merely
        # because an order reports a fill before the matching position
        # snapshot catches up. After this grace period, a persistent
        # order/position contradiction is treated as an anomaly.
        "lifecycle_fill_position_sync_grace_seconds": 120.0,

        # Partial ENTRY fills are accepted as a smaller position. The
        # unfilled remainder is never topped up automatically after the
        # entry order becomes terminal. A later independent setup must
        # come through the normal scanner/risk pipeline.
        "paper_entry_partial_fill_policy": "KEEP_PARTIAL_NO_TOP_UP",

        # A terminal CLOSE order that leaves broker exposure may be
        # retried only on a later trading date. This deliberately avoids
        # repeated same-day submissions/chasing after reject/cancel/expiry.
        "paper_exit_terminal_retry_policy": "NEXT_TRADING_DATE",

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
        # TRADE JOURNAL + ANALYTICS
        # --------------------------------------------------

        # Journal rows are regenerated/upserted from the persistent lifecycle
        # ledger, so the lifecycle file remains the source of truth.
        "trade_journal_enabled": True,

        # Log a compact analytics summary only when the completed/open trade
        # counts or realized P/L change.
        "trade_journal_log_summary": True,

        # --------------------------------------------------
        # PRODUCTION SAFETY / HARDENING
        # --------------------------------------------------

        # The lifecycle ledger is backed up before material overwrites and
        # validated on every load/save. Corrupt primary state is recovered
        # automatically from the newest valid backup when possible.
        "trade_state_backup_enabled": True,
        "trade_state_backup_max_files": 25,
        "trade_state_backup_min_interval_seconds": 60.0,
        "trade_state_fail_fast_on_unrecoverable": True,

        # Startup health failures block NEW entries but never block lifecycle
        # reconciliation or exits for positions already open.
        "startup_health_block_new_entries": True,

        # Daily circuit breakers are deliberately conservative. They only
        # block NEW entries; existing positions remain managed and closable.
        "circuit_breaker_enabled": True,
        "circuit_breaker_max_daily_realized_loss_pct_equity": 0.01,
        "circuit_breaker_max_daily_equity_drawdown_pct": 0.02,
        "circuit_breaker_max_new_entries_per_day": 5,
        "circuit_breaker_max_consecutive_losses": 3,
        "circuit_breaker_halt_on_orphaned": True,

        # Operational reporting is intentionally read-only. The daily summary
        # is an atomically replaced snapshot; anomalies are append-only JSONL.
        "daily_operational_summary_enabled": True,
        "trading_anomaly_log_enabled": True,

        # --------------------------------------------------
        # EXIT MANAGEMENT
        # --------------------------------------------------

        # Close when the conservative executable mark reaches +50%
        # versus the broker-confirmed actual entry debit.
        "exit_profit_target_pct": 0.50,

        # Close when the conservative executable mark reaches -50%
        # versus the broker-confirmed actual entry debit.
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

        # Logical full-scanner policy retained for compatibility/documentation.
        # This no longer drives LumiBot's framework scheduler; full scanning is
        # throttled internally to once per market date.
        "options_active_sleeptime": "1D",

        # FIXED LumiBot framework driver cadence. Keep this at 1M so the live
        # scheduler cannot strand working orders/open positions until tomorrow.
        # Expensive work is throttled internally; this does NOT mean a full
        # universe/options scan every minute.
        "options_management_sleeptime": "1M",

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
        # LumiBot's live scheduler snapshots sleeptime for the next wakeup
        # before later on_trading_iteration() changes can reliably affect it.
        # Keep the framework driver permanently fast and throttle expensive
        # work inside the strategy instead of dynamically changing sleeptime.
        self.sleeptime = (
            self.parameters[
                "options_management_sleeptime"
            ]
        )

        # Updated from Alpaca's market clock whenever the
        # options-session gate is checked.
        self._option_quote_reference_time = None

        # The expensive stock/options scanner is intentionally separate
        # from broker/order/exit management cadence. A process restart may
        # perform one fresh scan, but once a scan completes for a market
        # date, intraday wakeups are management-only.
        self._last_full_scan_market_date = None
        self._runtime_cadence_label = None
        self._last_closed_market_reconcile_at = None
        self._closed_gate_skip_logged = False

        api_key = os.environ["ALPACA_API_KEY"]
        api_secret = os.environ["ALPACA_API_SECRET"]

        paper = (
            os.environ.get(
                "ALPACA_IS_PAPER",
                "true",
            ).lower()
            == "true"
        )

        self.alpaca_is_paper = bool(paper)

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
        # OPTIONAL PAPER ENTRY EXECUTION
        # --------------------------------------------------
        #
        # Requires BOTH:
        #   PAPER_EXECUTION_ENABLED=true
        #   PAPER_EXECUTION_ARM=PAPER_ONLY
        #
        # ALPACA_IS_PAPER must also be true. This two-key arm
        # prevents an accidental environment edit from enabling
        # broker writes. Only option ENTRY limit orders are submitted
        # in this phase. Micro-account simulation remains alert-only.
        # --------------------------------------------------

        self.paper_execution_enabled = (
            os.environ.get(
                "PAPER_EXECUTION_ENABLED",
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

        self.paper_execution_arm = (
            os.environ.get(
                "PAPER_EXECUTION_ARM",
                "",
            )
            .strip()
            .upper()
        )

        self.paper_execution_armed = (
            self.paper_execution_enabled
            and self.paper_execution_arm == "PAPER_ONLY"
        )

        if self.paper_execution_enabled and not self.alpaca_is_paper:
            raise ValueError(
                "PAPER_EXECUTION_ENABLED=true is forbidden when "
                "ALPACA_IS_PAPER is not true. No live-account "
                "execution is permitted by this strategy build."
            )

        if (
            self.paper_execution_enabled
            and self.paper_execution_arm != "PAPER_ONLY"
        ):
            self.log_message(
                "PAPER EXECUTION requested but NOT ARMED: set "
                "PAPER_EXECUTION_ARM=PAPER_ONLY to allow PAPER "
                "option entry submissions. Remaining alert-only."
            )
        elif self.paper_execution_armed:
            self.log_message(
                "PAPER EXECUTION ARMED: option ENTRY orders may be "
                "submitted to Alpaca PAPER as DAY limit orders. "
                "Close orders remain separately gated by the PAPER EXIT arm; "
                "no live, replace, cancel, or exercise orders are enabled."
            )

        self._paper_execution_orders_submitted_this_run = 0

        # --------------------------------------------------
        # OPTIONAL PAPER EXIT EXECUTION
        # --------------------------------------------------
        #
        # Requires BOTH:
        #   PAPER_EXIT_EXECUTION_ENABLED=true
        #   PAPER_EXIT_EXECUTION_ARM=PAPER_ONLY
        #
        # This arm is intentionally independent from entry execution.
        # Existing broker-confirmed PAPER positions can therefore be
        # managed without enabling new entries, and an existing entry
        # arm does not unexpectedly start sending close orders.
        # --------------------------------------------------

        self.paper_exit_execution_enabled = (
            os.environ.get(
                "PAPER_EXIT_EXECUTION_ENABLED",
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

        self.paper_exit_execution_arm = (
            os.environ.get(
                "PAPER_EXIT_EXECUTION_ARM",
                "",
            )
            .strip()
            .upper()
        )

        self.paper_exit_execution_armed = (
            self.paper_exit_execution_enabled
            and self.paper_exit_execution_arm == "PAPER_ONLY"
        )

        if (
            self.paper_exit_execution_enabled
            and not self.alpaca_is_paper
        ):
            raise ValueError(
                "PAPER_EXIT_EXECUTION_ENABLED=true is forbidden when "
                "ALPACA_IS_PAPER is not true. No live-account exit "
                "execution is permitted by this strategy build."
            )

        if (
            self.paper_exit_execution_enabled
            and self.paper_exit_execution_arm != "PAPER_ONLY"
        ):
            self.log_message(
                "PAPER EXIT EXECUTION requested but NOT ARMED: set "
                "PAPER_EXIT_EXECUTION_ARM=PAPER_ONLY to allow PAPER "
                "option close submissions. Remaining exit-alert only."
            )
        elif self.paper_exit_execution_armed:
            self.log_message(
                "PAPER EXIT EXECUTION ARMED: broker-confirmed option "
                "CLOSE signals may submit Alpaca PAPER DAY limit orders. "
                "No live, replace, cancel, or exercise orders are enabled."
            )

        self._paper_exit_orders_submitted_this_run = 0

        # --------------------------------------------------
        # CONTROLLED PAPER-ONLY EXIT VALIDATION
        # --------------------------------------------------
        #
        # This deliberately reuses the real production CLOSE pipeline.
        # It does not alter profit/loss/DTE/thesis thresholds. Instead, when
        # fully armed during an allowed options session, it converts exactly
        # one broker-confirmed lifecycle for PAPER_EXIT_TEST_SYMBOL into a
        # one-shot CLOSE action. The token is persisted before submission, so
        # restarts cannot repeatedly force the same validation close.
        #
        # Requires the normal PAPER exit arm plus ALL of:
        #   PAPER_EXIT_TEST_ENABLED=true
        #   PAPER_EXIT_TEST_ARM=FORCE_PAPER_CLOSE
        #   PAPER_EXIT_TEST_SYMBOL=OWL
        #   PAPER_EXIT_TEST_TOKEN=OWL_EXIT_TEST_001
        # --------------------------------------------------

        self.paper_exit_test_enabled = (
            os.environ.get(
                "PAPER_EXIT_TEST_ENABLED",
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

        self.paper_exit_test_arm = (
            os.environ.get(
                "PAPER_EXIT_TEST_ARM",
                "",
            )
            .strip()
            .upper()
        )

        self.paper_exit_test_symbol = (
            os.environ.get(
                "PAPER_EXIT_TEST_SYMBOL",
                "",
            )
            .strip()
            .upper()
        )

        self.paper_exit_test_token = (
            os.environ.get(
                "PAPER_EXIT_TEST_TOKEN",
                "",
            )
            .strip()
        )

        self.paper_exit_test_armed = (
            self.paper_exit_test_enabled
            and self.paper_exit_test_arm == "FORCE_PAPER_CLOSE"
            and bool(self.paper_exit_test_symbol)
            and bool(self.paper_exit_test_token)
        )

        if self.paper_exit_test_enabled and not self.alpaca_is_paper:
            raise ValueError(
                "PAPER_EXIT_TEST_ENABLED=true is forbidden when "
                "ALPACA_IS_PAPER is not true. Controlled exit tests "
                "can never run against a live account."
            )

        if self.paper_exit_test_enabled and not self.paper_exit_execution_armed:
            raise ValueError(
                "Controlled PAPER exit validation requires the normal "
                "PAPER_EXIT_EXECUTION_ENABLED=true and "
                "PAPER_EXIT_EXECUTION_ARM=PAPER_ONLY safeguards."
            )

        if self.paper_exit_test_enabled and not self.paper_exit_test_armed:
            self.log_message(
                "CONTROLLED PAPER EXIT TEST requested but NOT ARMED: set "
                "PAPER_EXIT_TEST_ARM=FORCE_PAPER_CLOSE plus an exact "
                "PAPER_EXIT_TEST_SYMBOL and non-empty PAPER_EXIT_TEST_TOKEN."
            )
        elif self.paper_exit_test_armed:
            token_fingerprint = hashlib.sha256(
                self.paper_exit_test_token.encode("utf-8")
            ).hexdigest()[:10]
            self.log_message(
                "CONTROLLED PAPER EXIT TEST ARMED: exact underlying="
                f"{self.paper_exit_test_symbol}; token_sha256={token_fingerprint}; "
                "one broker-confirmed active lifecycle may be forced through "
                "the real Alpaca PAPER close pipeline during an allowed options "
                "session. The token is one-shot and restart-safe."
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

        # --------------------------------------------------
        # PRODUCTION SAFETY / HARDENING RUNTIME PATHS
        # --------------------------------------------------
        self.trade_state_backup_enabled = self._env_bool(
            "TRADE_STATE_BACKUP_ENABLED",
            self.parameters.get("trade_state_backup_enabled", True),
        )
        self.trade_state_backup_dir = os.environ.get(
            "TRADE_STATE_BACKUP_DIR",
            (self.trade_alert_positions_path + ".backups")
            if self.trade_alert_positions_path
            else ".trade_alert_positions.backups",
        ).strip()
        self.trade_state_backup_max_files = max(
            3,
            self._env_int(
                "TRADE_STATE_BACKUP_MAX_FILES",
                self.parameters.get("trade_state_backup_max_files", 25),
            ),
        )
        self.trade_state_backup_min_interval_seconds = max(
            0.0,
            self._env_float(
                "TRADE_STATE_BACKUP_MIN_INTERVAL_SECONDS",
                self.parameters.get("trade_state_backup_min_interval_seconds", 60.0),
            ),
        )
        self.trade_state_fail_fast_on_unrecoverable = self._env_bool(
            "TRADE_STATE_FAIL_FAST_ON_UNRECOVERABLE",
            self.parameters.get("trade_state_fail_fast_on_unrecoverable", True),
        )
        self.startup_health_report_path = os.environ.get(
            "STARTUP_HEALTH_REPORT_PATH",
            "startup_health.json",
        ).strip()
        self.daily_trading_summary_path = os.environ.get(
            "DAILY_TRADING_SUMMARY_PATH",
            "daily_trading_summary.json",
        ).strip()
        self.trading_anomalies_jsonl_path = os.environ.get(
            "TRADING_ANOMALIES_JSONL_PATH",
            "trading_anomalies.jsonl",
        ).strip()
        self.trading_circuit_breaker_state_path = os.environ.get(
            "TRADING_CIRCUIT_BREAKER_STATE_PATH",
            ".trading_circuit_breakers.json",
        ).strip()
        self.trading_kill_switch_file = os.environ.get(
            "TRADING_KILL_SWITCH_FILE",
            ".trading_kill_switch",
        ).strip()
        self._lifecycle_state_integrity_ok = True
        self._lifecycle_state_recovered_from_backup = False
        self._lifecycle_state_recovery_reason = ""
        self._last_trade_state_backup_at = None
        self._startup_health_results = []
        self.startup_health_entries_allowed = False
        self._entry_execution_blocked_reasons = []
        self._last_circuit_breaker_log_signature = None
        self._trading_circuit_breaker_state = {}
        self._circuit_breaker_state_integrity_ok = True
        self._circuit_breaker_state_integrity_reason = ""
        self._anomaly_emitted_keys = set()
        self._last_daily_summary_log_signature = None
        self._load_recent_anomaly_keys()

        self._load_trade_alert_positions_state()

        # --------------------------------------------------
        # TRADE JOURNAL + ANALYTICS OUTPUTS
        # --------------------------------------------------
        # Optional .env:
        #   TRADE_JOURNAL_CSV_PATH=trade_journal.csv
        #   TRADE_ANALYTICS_JSON_PATH=trade_analytics.json
        #
        # The CSV is a canonical one-row-per-executed-lifecycle snapshot.
        # It is rebuilt atomically from the lifecycle ledger, making restart
        # recovery deterministic and avoiding duplicate journal rows.
        # --------------------------------------------------

        self.trade_journal_csv_path = (
            os.environ.get(
                "TRADE_JOURNAL_CSV_PATH",
                "trade_journal.csv",
            ).strip()
        )

        self.trade_analytics_json_path = (
            os.environ.get(
                "TRADE_ANALYTICS_JSON_PATH",
                "trade_analytics.json",
            ).strip()
        )

        self._last_trade_analytics_log_signature = None

        self._load_trading_circuit_breaker_state()

        self.log_message(
            "TRADE JOURNAL ENABLED: canonical lifecycle-derived CSV="
            f"{self.trade_journal_csv_path or 'DISABLED'}; analytics JSON="
            f"{self.trade_analytics_json_path or 'DISABLED'}."
        )

        self.log_message(
            "FRAMEWORK SCHEDULER DRIVER: fixed at "
            f"{self.sleeptime}. Full stock/options scanning is internally "
            "limited to once per market date; closed-market broker "
            f"reconciliation is throttled to "
            f"{self.parameters['options_closed_retry_sleeptime']}."
        )

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

        # Health checks never stop management/exits for existing exposure.
        # Any FAIL only blocks NEW entries.
        self._run_startup_health_check()

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

        If Alpaca did not return a level, keep ANALYSIS enabled.
        The optional PAPER execution layer independently fails closed
        unless a sufficient numeric options level is known.
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

                "mode": (
                    "ALPACA_PAPER_ENTRY_EXECUTION"
                    if self.paper_execution_armed
                    else "ALERT_ONLY_NO_ORDER"
                ),
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
            "SUPERSEDED",
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
                        "ratio_qty": self._broker_field(
                            leg,
                            "ratio_qty",
                            1.0,
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
                    "qty": self._lifecycle_float(
                        self._broker_field(
                            order,
                            "qty",
                            0.0,
                        ),
                        0.0,
                    ) or 0.0,
                    "submitted_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "submitted_at",
                            None,
                        )
                    ),
                    "updated_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "updated_at",
                            None,
                        )
                    ),
                    "expired_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "expired_at",
                            None,
                        )
                    ),
                    "canceled_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "canceled_at",
                            None,
                        )
                    ),
                    "failed_at": self._parse_lifecycle_datetime(
                        self._broker_field(
                            order,
                            "failed_at",
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
        claim_owners=None,
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

        # If this lifecycle has an explicitly linked broker order,
        # exact order/client IDs are authoritative. This avoids
        # accidentally attaching a manual order that happens to use
        # the same option legs. Fall back to leg/time matching only
        # when no explicit linked order is present in the snapshot.
        prefix = (
            "close"
            if close
            else "entry"
        )

        linked_order_id = str(
            position.get(
                f"broker_{prefix}_order_id",
                "",
            )
            or ""
        )

        linked_client_order_id = str(
            position.get(
                f"broker_{prefix}_client_order_id",
                "",
            )
            or ""
        )

        if linked_order_id or linked_client_order_id:
            exact_matches = [
                order
                for order in normalized_orders
                if (
                    linked_order_id
                    and str(order.get("id", "") or "")
                    == linked_order_id
                )
                or (
                    linked_client_order_id
                    and str(
                        order.get(
                            "client_order_id",
                            "",
                        )
                        or ""
                    )
                    == linked_client_order_id
                )
            ]

            exact_matches.sort(
                key=lambda row: (
                    row.get("submitted_at")
                    or datetime.min.replace(
                        tzinfo=timezone.utc
                    )
                )
            )

            # Once an explicit order/client ID is assigned, do not
            # fall back to heuristic leg matching. An empty exact
            # result means the linked order is absent from this
            # snapshot, not that a manual same-leg order should be
            # adopted by this lifecycle.
            return exact_matches

        current_position_id = str(
            position.get(
                "id",
                "",
            )
            or ""
        )

        order_id_owners = {}
        client_id_owners = {}

        if claim_owners:
            prefix_owners = claim_owners.get(
                prefix,
                {},
            )
            order_id_owners = prefix_owners.get(
                "order_id_owner",
                {},
            )
            client_id_owners = prefix_owners.get(
                "client_id_owner",
                {},
            )

        for order in normalized_orders:

            broker_order_id = str(
                order.get(
                    "id",
                    "",
                )
                or ""
            )

            broker_client_order_id = str(
                order.get(
                    "client_order_id",
                    "",
                )
                or ""
            )

            claimed_owner = (
                order_id_owners.get(
                    broker_order_id
                )
                if broker_order_id
                else None
            )

            if claimed_owner is None and broker_client_order_id:
                claimed_owner = client_id_owners.get(
                    broker_client_order_id
                )

            if (
                claimed_owner
                and claimed_owner != current_position_id
            ):
                # Explicit broker links are exclusive. A heuristic
                # same-leg/time match may not steal an order already
                # owned by another lifecycle.
                continue

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
            # Alpaca documents STOPPED as a guaranteed trade that has
            # not executed yet, so it remains broker-working evidence.
            "stopped",
        }

        terminal_statuses = {
            "canceled",
            "expired",
            "rejected",
            "done_for_day",
            "calculated",
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


    def _classify_lifecycle_order(
        self,
        order,
    ):
        """Classify one normalized Alpaca order for lifecycle policy.

        The classification intentionally separates a terminal unfilled
        remainder from the exposure created by any partial fill.
        """

        if not order:
            return {
                "category": "NONE",
                "status": "",
                "requested_qty": 0.0,
                "filled_qty": 0.0,
                "remaining_qty": 0.0,
                "has_fill": False,
                "working": False,
                "terminal": False,
                "retryable_terminal": False,
            }

        status = str(
            order.get(
                "status",
                "",
            )
            or ""
        ).lower()

        requested_qty = self._lifecycle_float(
            order.get(
                "qty",
                0.0,
            ),
            0.0,
        ) or 0.0

        filled_qty = self._lifecycle_float(
            order.get(
                "filled_qty",
                0.0,
            ),
            0.0,
        ) or 0.0

        tolerance = max(
            0.0,
            float(
                self.parameters.get(
                    "lifecycle_quantity_tolerance",
                    1e-6,
                )
            ),
        )

        remaining_qty = max(
            0.0,
            requested_qty - filled_qty,
        )

        has_fill = filled_qty > tolerance

        if (
            status == "filled"
            or (
                requested_qty > tolerance
                and remaining_qty <= tolerance
                and has_fill
            )
        ):
            category = "FILLED"
            working = False
            terminal = True
            retryable_terminal = False

        elif status in {
            "new",
            "accepted",
            "pending_new",
            "accepted_for_bidding",
            "pending_review",
            "held",
            "pending_replace",
            "pending_cancel",
            "stopped",
            "partially_filled",
        }:
            category = (
                "PARTIAL_WORKING"
                if has_fill
                else "WORKING"
            )
            working = True
            terminal = False
            retryable_terminal = False

        elif status == "rejected":
            category = (
                "PARTIAL_REJECTED"
                if has_fill
                else "REJECTED"
            )
            working = False
            terminal = True
            retryable_terminal = True

        elif status == "canceled":
            category = (
                "PARTIAL_CANCELED"
                if has_fill
                else "CANCELED"
            )
            working = False
            terminal = True
            retryable_terminal = True

        elif status in {
            "expired",
            "done_for_day",
            "calculated",
        }:
            category = (
                "PARTIAL_EXPIRED"
                if has_fill
                else "EXPIRED"
            )
            working = False
            terminal = True
            retryable_terminal = True

        elif status == "suspended":
            category = (
                "PARTIAL_SUSPENDED"
                if has_fill
                else "SUSPENDED"
            )
            working = False
            terminal = True
            retryable_terminal = False

        elif status == "replaced":
            category = (
                "PARTIAL_REPLACED"
                if has_fill
                else "REPLACED"
            )
            working = False
            terminal = True
            retryable_terminal = False

        else:
            category = (
                "PARTIAL_UNKNOWN"
                if has_fill
                else "UNKNOWN"
            )
            working = False
            terminal = False
            retryable_terminal = False

        return {
            "category": category,
            "status": status,
            "requested_qty": float(requested_qty),
            "filled_qty": float(filled_qty),
            "remaining_qty": float(remaining_qty),
            "has_fill": bool(has_fill),
            "working": bool(working),
            "terminal": bool(terminal),
            "retryable_terminal": bool(retryable_terminal),
        }


    def _persist_order_outcome_metadata(
        self,
        position,
        prefix,
        evidence,
        expected_quantity,
        now,
    ):
        """Persist requested/filled/remainder policy for entry or close."""

        latest = evidence.get(
            "latest"
        )

        outcome = self._classify_lifecycle_order(
            latest
        )

        requested_qty = outcome[
            "requested_qty"
        ]

        if requested_qty <= 0:
            requested_qty = max(
                0.0,
                float(
                    expected_quantity
                    or 0.0
                ),
            )

        filled_qty = outcome[
            "filled_qty"
        ]
        remaining_qty = max(
            0.0,
            requested_qty - filled_qty,
        )

        position[
            f"broker_{prefix}_order_outcome"
        ] = outcome[
            "category"
        ]
        position[
            f"broker_{prefix}_requested_qty"
        ] = requested_qty
        position[
            f"broker_{prefix}_unfilled_qty"
        ] = remaining_qty

        terminal_remainder = (
            outcome[
                "terminal"
            ]
            and remaining_qty > max(
                0.0,
                float(
                    self.parameters.get(
                        "lifecycle_quantity_tolerance",
                        1e-6,
                    )
                ),
            )
        )

        position[
            f"broker_{prefix}_terminal_remainder"
        ] = bool(
            terminal_remainder
        )

        if prefix == "entry":
            if (
                outcome[
                    "has_fill"
                ]
                and terminal_remainder
            ):
                position[
                    "paper_entry_remainder_policy"
                ] = str(
                    self.parameters.get(
                        "paper_entry_partial_fill_policy",
                        "KEEP_PARTIAL_NO_TOP_UP",
                    )
                )
                position[
                    "paper_entry_top_up_allowed"
                ] = False

        elif prefix == "close":
            if (
                outcome[
                    "terminal"
                ]
                and outcome[
                    "retryable_terminal"
                ]
                and remaining_qty > 0
            ):
                submitted_at = (
                    latest.get(
                        "submitted_at"
                    )
                    if latest
                    else None
                )

                base_date = (
                    submitted_at.date()
                    if isinstance(
                        submitted_at,
                        datetime,
                    )
                    else now.date()
                )

                retry_after = (
                    base_date
                    + timedelta(
                        days=1
                    )
                )

                position[
                    "paper_exit_retry_policy"
                ] = str(
                    self.parameters.get(
                        "paper_exit_terminal_retry_policy",
                        "NEXT_TRADING_DATE",
                    )
                )
                position[
                    "paper_exit_retry_after_date"
                ] = retry_after.isoformat()
                position[
                    "paper_exit_retry_eligible"
                ] = bool(
                    now.date()
                    >= retry_after
                )
                position[
                    "paper_exit_retry_reason"
                ] = outcome[
                    "category"
                ]
            elif outcome[
                "working"
            ] or (
                outcome[
                    "terminal"
                ]
                and remaining_qty <= 0
            ):
                position[
                    "paper_exit_retry_eligible"
                ] = False
                position[
                    "paper_exit_retry_after_date"
                ] = ""
                position[
                    "paper_exit_retry_reason"
                ] = ""

        return outcome


    def _order_position_sync_grace_active(
        self,
        position,
        prefix,
        order,
        now,
    ):
        """Allow short broker order/position eventual-consistency lag."""

        if not order:
            return False

        key = (
            f"broker_{prefix}_fill_position_wait_started_at"
        )

        started = self._parse_lifecycle_datetime(
            position.get(
                key,
                None,
            )
        )

        if started is None:
            started = now
            position[
                key
            ] = started.isoformat()

        grace_seconds = max(
            0.0,
            float(
                self.parameters.get(
                    "lifecycle_fill_position_sync_grace_seconds",
                    120.0,
                )
            ),
        )

        age_seconds = max(
            0.0,
            (
                now - started
            ).total_seconds(),
        )

        return age_seconds <= grace_seconds


    @staticmethod
    def _lifecycle_float(
        value,
        default=None,
    ):
        try:
            if value is None:
                return default
            result = float(value)
            if not math.isfinite(result):
                return default
            return result
        except (
            TypeError,
            ValueError,
        ):
            return default


    def _normalized_order_fill_economics(
        self,
        order,
        position,
        close=False,
    ):
        """Return broker fill quantity/value for a supported option order.

        Entry value is a debit/share. Close value is cash received/share.
        MLEG leg fills are preferred because they make the economics
        unambiguous; Alpaca's signed top-level net fill is a fallback.
        """

        if not order:
            return None

        short_symbol = str(
            position.get(
                "short_contract",
                "",
            )
            or ""
        ).upper()
        is_mleg = bool(short_symbol)

        filled_qty = self._lifecycle_float(
            order.get(
                "filled_qty",
                0.0,
            ),
            0.0,
        ) or 0.0

        top_fill = self._lifecycle_float(
            order.get(
                "filled_avg_price",
                None,
            ),
            None,
        )

        def top_result():
            if (
                filled_qty <= 0
                or top_fill is None
                or abs(top_fill) <= 1e-12
            ):
                return None

            if is_mleg:
                # Alpaca MLEG net prices use signed economics:
                # positive debit, negative credit.
                value = -top_fill if close else top_fill
            else:
                # Single-leg option fills are positive option prices;
                # a sell-to-close fill therefore represents credit received.
                value = top_fill

            if (
                not close
                and value <= 0
            ):
                return None

            return {
                "filled_qty": float(filled_qty),
                "value_per_share": float(value),
                "source": "BROKER_ORDER_TOP_FILL",
                "filled_at": order.get(
                    "filled_at"
                ),
                "order_id": str(
                    order.get(
                        "id",
                        "",
                    )
                    or ""
                ),
                "client_order_id": str(
                    order.get(
                        "client_order_id",
                        "",
                    )
                    or ""
                ),
            }

        if not is_mleg:
            return top_result()

        # Prefer individual MLEG fill prices. This avoids depending on
        # any ambiguity in the sign of a top-level average fill response.
        legs = order.get(
            "legs",
            [],
        ) or []

        if not legs:
            return top_result()

        signed_debit = 0.0
        leg_fill_quantities = []
        usable_legs = 0

        for leg in legs:
            price = self._lifecycle_float(
                leg.get(
                    "filled_avg_price",
                    None,
                ),
                None,
            )

            if (
                price is None
                or price < 0
            ):
                continue

            ratio = self._lifecycle_float(
                leg.get(
                    "ratio_qty",
                    1.0,
                ),
                1.0,
            ) or 1.0

            if ratio <= 0:
                ratio = 1.0

            leg_filled_qty = self._lifecycle_float(
                leg.get(
                    "filled_qty",
                    0.0,
                ),
                0.0,
            ) or 0.0

            side = str(
                leg.get(
                    "side",
                    "",
                )
                or ""
            ).lower()

            if side == "buy":
                signed_debit += price * ratio
            elif side == "sell":
                signed_debit -= price * ratio
            else:
                continue

            usable_legs += 1

            if leg_filled_qty > 0:
                leg_fill_quantities.append(
                    leg_filled_qty / ratio
                )

        if usable_legs != len(legs):
            return top_result()

        effective_qty = filled_qty

        if effective_qty <= 0 and leg_fill_quantities:
            effective_qty = min(
                leg_fill_quantities
            )

        if effective_qty <= 0:
            return top_result()

        value = (
            -signed_debit
            if close
            else signed_debit
        )

        if (
            not close
            and value <= 0
        ):
            return top_result()

        return {
            "filled_qty": float(effective_qty),
            "value_per_share": float(value),
            "source": "BROKER_ORDER_LEG_FILLS",
            "filled_at": order.get(
                "filled_at"
            ),
            "order_id": str(
                order.get(
                    "id",
                    "",
                )
                or ""
            ),
            "client_order_id": str(
                order.get(
                    "client_order_id",
                    "",
                )
                or ""
            ),
        }

    def _broker_position_entry_basis(
        self,
        position,
        snapshot,
    ):
        """Fallback to broker position average prices when order fill is absent."""

        details = snapshot.get(
            "position_details",
            {},
        ) or {}

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

        if not long_symbol:
            return None

        long_detail = details.get(
            long_symbol,
            {},
        ) or {}

        long_price = self._lifecycle_float(
            long_detail.get(
                "avg_entry_price",
                None,
            ),
            None,
        )

        if (
            long_price is None
            or long_price <= 0
        ):
            return None

        basis = long_price

        if short_symbol:
            short_detail = details.get(
                short_symbol,
                {},
            ) or {}

            short_price = self._lifecycle_float(
                short_detail.get(
                    "avg_entry_price",
                    None,
                ),
                None,
            )

            if (
                short_price is None
                or short_price < 0
            ):
                return None

            basis = long_price - short_price

        if basis <= 0:
            return None

        qty = self._lifecycle_float(
            position.get(
                "broker_open_quantity",
                0.0,
            ),
            0.0,
        ) or 0.0

        if qty <= 0:
            return None

        return {
            "filled_qty": float(qty),
            "value_per_share": float(basis),
            "source": "BROKER_POSITION_AVG_ENTRY",
            "filled_at": None,
            "order_id": "",
            "client_order_id": "",
        }


    def _update_lifecycle_fill_accounting(
        self,
        position,
        snapshot,
        entry_evidence,
        close_evidence,
    ):
        """Persist actual broker entry basis and realized close P/L."""

        if str(
            position.get(
                "asset_type",
                "OPTION",
            )
            or "OPTION"
        ).upper() != "OPTION":
            return

        prior_entry_basis = self._lifecycle_float(
            position.get(
                "actual_entry_debit_per_share",
                None,
            ),
            None,
        )

        entry_fill = self._normalized_order_fill_economics(
            entry_evidence.get(
                "latest"
            ),
            position,
            close=False,
        )

        if (
            entry_fill is None
            and prior_entry_basis is None
        ):
            entry_fill = self._broker_position_entry_basis(
                position,
                snapshot,
            )

        if entry_fill is not None:
            entry_basis = float(
                entry_fill[
                    "value_per_share"
                ]
            )
            entry_qty = float(
                entry_fill[
                    "filled_qty"
                ]
            )

            position[
                "actual_entry_debit_per_share"
            ] = entry_basis
            position[
                "actual_entry_filled_qty"
            ] = max(
                entry_qty,
                self._lifecycle_float(
                    position.get(
                        "actual_entry_filled_qty",
                        0.0,
                    ),
                    0.0,
                ) or 0.0,
            )
            position[
                "actual_entry_basis_source"
            ] = entry_fill[
                "source"
            ]
            position[
                "actual_entry_filled_at"
            ] = (
                entry_fill[
                    "filled_at"
                ].isoformat()
                if isinstance(
                    entry_fill.get(
                        "filled_at"
                    ),
                    datetime,
                )
                else str(
                    entry_fill.get(
                        "filled_at",
                        "",
                    )
                    or ""
                )
            )
            position[
                "actual_entry_total_debit"
            ] = (
                entry_basis
                * 100.0
                * float(
                    position[
                        "actual_entry_filled_qty"
                    ]
                )
            )

            if prior_entry_basis is None:
                self._record_lifecycle_event(
                    position,
                    "ACTUAL_ENTRY_BASIS_ESTABLISHED",
                    "Broker fill/position data established the entry basis",
                    details={
                        "debit_per_share": entry_basis,
                        "filled_qty": entry_qty,
                        "source": entry_fill[
                            "source"
                        ],
                    },
                )

        close_fill = self._normalized_order_fill_economics(
            close_evidence.get(
                "latest"
            ),
            position,
            close=True,
        )

        if close_fill is not None:
            ledger = position.setdefault(
                "broker_close_fill_ledger",
                {},
            )

            order_key = (
                close_fill.get(
                    "order_id"
                )
                or close_fill.get(
                    "client_order_id"
                )
                or "UNKNOWN_CLOSE_ORDER"
            )

            old_ledger_row = ledger.get(
                order_key,
                {},
            ) or {}

            ledger[
                order_key
            ] = {
                "filled_qty": float(
                    close_fill[
                        "filled_qty"
                    ]
                ),
                "credit_per_share": float(
                    close_fill[
                        "value_per_share"
                    ]
                ),
                "source": close_fill[
                    "source"
                ],
                "filled_at": (
                    close_fill[
                        "filled_at"
                    ].isoformat()
                    if isinstance(
                        close_fill.get(
                            "filled_at"
                        ),
                        datetime,
                    )
                    else str(
                        close_fill.get(
                            "filled_at",
                            "",
                        )
                        or ""
                    )
                ),
                "client_order_id": close_fill.get(
                    "client_order_id",
                    "",
                ),
            }

            if (
                old_ledger_row.get(
                    "filled_qty"
                )
                != ledger[
                    order_key
                ][
                    "filled_qty"
                ]
            ):
                self._record_lifecycle_event(
                    position,
                    "ACTUAL_CLOSE_FILL_UPDATED",
                    "Broker close fill accounting was updated",
                    details={
                        "order_id": order_key,
                        "filled_qty": ledger[
                            order_key
                        ][
                            "filled_qty"
                        ],
                        "credit_per_share": ledger[
                            order_key
                        ][
                            "credit_per_share"
                        ],
                        "source": ledger[
                            order_key
                        ][
                            "source"
                        ],
                    },
                )

        entry_basis = self._lifecycle_float(
            position.get(
                "actual_entry_debit_per_share",
                None,
            ),
            None,
        )

        ledger = position.get(
            "broker_close_fill_ledger",
            {},
        ) or {}

        total_close_qty = 0.0
        realized_pnl = 0.0
        weighted_close_credit = 0.0

        if entry_basis is not None:
            for fill in ledger.values():
                qty = self._lifecycle_float(
                    fill.get(
                        "filled_qty",
                        0.0,
                    ),
                    0.0,
                ) or 0.0
                credit = self._lifecycle_float(
                    fill.get(
                        "credit_per_share",
                        None,
                    ),
                    None,
                )

                if (
                    qty <= 0
                    or credit is None
                ):
                    continue

                total_close_qty += qty
                weighted_close_credit += credit * qty
                realized_pnl += (
                    credit - entry_basis
                ) * 100.0 * qty

        position[
            "actual_close_filled_qty"
        ] = total_close_qty
        position[
            "actual_realized_pnl_dollars"
        ] = realized_pnl

        if total_close_qty > 0:
            position[
                "actual_close_avg_credit_per_share"
            ] = (
                weighted_close_credit
                / total_close_qty
            )
        else:
            position[
                "actual_close_avg_credit_per_share"
            ] = None


    def log_broker_fill_accounting(
        self,
    ):
        """Log persisted broker fill basis/realized P&L for option lifecycles."""

        rows = []

        for position in self._tracked_alert_positions.values():
            if str(
                position.get(
                    "asset_type",
                    "OPTION",
                )
                or "OPTION"
            ).upper() != "OPTION":
                continue

            entry_basis = self._lifecycle_float(
                position.get(
                    "actual_entry_debit_per_share",
                    None,
                ),
                None,
            )

            if entry_basis is None:
                continue

            rows.append(
                {
                    "underlying": position.get(
                        "underlying",
                        "",
                    ),
                    "status": self._normalize_lifecycle_status(
                        position.get(
                            "status",
                            "ALERTED",
                        )
                    ),
                    "entry_fill/share": round(
                        entry_basis,
                        4,
                    ),
                    "entry_fill_qty": round(
                        self._lifecycle_float(
                            position.get(
                                "actual_entry_filled_qty",
                                0.0,
                            ),
                            0.0,
                        ) or 0.0,
                        4,
                    ),
                    "entry_source": position.get(
                        "actual_entry_basis_source",
                        "",
                    ),
                    "open_qty": round(
                        self._lifecycle_float(
                            position.get(
                                "broker_open_quantity",
                                0.0,
                            ),
                            0.0,
                        ) or 0.0,
                        4,
                    ),
                    "close_fill/share": (
                        None
                        if position.get(
                            "actual_close_avg_credit_per_share",
                            None,
                        ) is None
                        else round(
                            float(
                                position[
                                    "actual_close_avg_credit_per_share"
                                ]
                            ),
                            4,
                        )
                    ),
                    "close_fill_qty": round(
                        self._lifecycle_float(
                            position.get(
                                "actual_close_filled_qty",
                                0.0,
                            ),
                            0.0,
                        ) or 0.0,
                        4,
                    ),
                    "realized_pnl_$": round(
                        self._lifecycle_float(
                            position.get(
                                "actual_realized_pnl_dollars",
                                0.0,
                            ),
                            0.0,
                        ) or 0.0,
                        2,
                    ),
                }
            )

        if rows:
            self.log_message(
                "\n\n===== BROKER FILL ACCOUNTING =====\n"
                + pd.DataFrame(
                    rows
                ).to_string(
                    index=False
                )
                + "\n=================================="
            )


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


    def _lifecycle_position_claim_signature(
        self,
        position,
    ):
        """Stable signature for one intended option/stock position."""

        legs = self._expected_lifecycle_legs(
            position,
            close=False,
        )

        return tuple(
            sorted(
                (
                    str(leg.get("symbol", "") or "").upper(),
                    str(leg.get("side", "") or "").lower(),
                )
                for leg in legs
                if str(leg.get("symbol", "") or "")
            )
        )


    def _broker_claim_owner_priority(
        self,
        position,
        order=None,
        prefix="entry",
    ):
        """Rank duplicate explicit broker claims deterministically.

        Execution-created links outrank links learned heuristically. Exact
        filled-quantity matches and timestamp proximity are secondary
        tie-breakers. The purpose is not to infer a fill; it is only to
        decide which lifecycle owns one already-linked broker order.
        """

        source = str(
            position.get(
                f"broker_{prefix}_link_source",
                "",
            )
            or ""
        ).upper()

        execution_link = int(
            bool(
                position.get(
                    "paper_execution_enabled_at_entry",
                    False,
                )
            )
            or source
            in {
                "SUBMIT_ORDER",
                "EXISTING_CLIENT_ORDER_ID",
            }
        )

        expected_quantity = float(
            position.get(
                "quantity",
                position.get("approx_shares", 0.0),
            )
            or 0.0
        )

        quantity_match = 0
        proximity_score = float("-inf")

        if order is not None:
            try:
                filled_qty = float(
                    order.get(
                        "filled_qty",
                        0.0,
                    )
                    or 0.0
                )
            except (TypeError, ValueError):
                filled_qty = 0.0

            if (
                expected_quantity > 0
                and abs(filled_qty - expected_quantity)
                <= max(
                    1e-6,
                    float(
                        self.parameters.get(
                            "lifecycle_quantity_tolerance",
                            1e-6,
                        )
                    ),
                )
            ):
                quantity_match = 1

            entry_time = self._parse_lifecycle_datetime(
                position.get(
                    "entry_timestamp",
                    None,
                )
            )
            submitted_at = order.get(
                "submitted_at"
            )

            if (
                entry_time is not None
                and submitted_at is not None
            ):
                try:
                    proximity_score = -abs(
                        (
                            submitted_at.astimezone(timezone.utc)
                            - entry_time.astimezone(timezone.utc)
                        ).total_seconds()
                    )
                except Exception:
                    proximity_score = float("-inf")

        linked_at = self._parse_lifecycle_datetime(
            position.get(
                f"broker_{prefix}_linked_at",
                None,
            )
        )

        linked_at_score = (
            linked_at.timestamp()
            if linked_at is not None
            else float("-inf")
        )

        status = self._normalize_lifecycle_status(
            position.get(
                "status",
                "ALERTED",
            )
        )

        status_rank = {
            "ENTRY_WORKING": 4,
            "PARTIALLY_OPEN": 3,
            "OPEN": 3,
            "CLOSE_ALERTED": 2,
            "CLOSE_WORKING": 2,
            "PARTIALLY_CLOSED": 2,
            "ALERTED": 1,
        }.get(status, 0)

        return (
            execution_link,
            quantity_match,
            status_rank,
            proximity_score,
            linked_at_score,
            str(position.get("id", "") or ""),
        )


    def _build_lifecycle_broker_claim_ownership(
        self,
        snapshot,
    ):
        """Build exclusive broker order/position ownership metadata.

        An explicitly linked broker order belongs to exactly one lifecycle.
        Other lifecycle records may not adopt that order through same-leg/time
        heuristics. This also detects and repairs duplicate links created by
        older reconciliation logic.
        """

        claims = {
            "entry": {
                "order_id_owner": {},
                "client_id_owner": {},
            },
            "close": {
                "order_id_owner": {},
                "client_id_owner": {},
            },
            "duplicate_entry_claims": {},
            "entry_signature_owners": {},
        }

        normalized_orders = list(
            snapshot.get(
                "normalized_orders",
                [],
            )
            or []
        )

        order_by_id = {
            str(order.get("id", "") or ""): order
            for order in normalized_orders
            if str(order.get("id", "") or "")
        }
        order_by_client_id = {
            str(order.get("client_order_id", "") or ""): order
            for order in normalized_orders
            if str(order.get("client_order_id", "") or "")
        }

        for prefix in ("entry", "close"):
            groups = {}

            for position_id, position in self._tracked_alert_positions.items():
                asset_type = str(
                    position.get("asset_type", "OPTION")
                    or "OPTION"
                ).upper()

                if (
                    asset_type == "FRACTIONAL_STOCK"
                    and str(
                        position.get("sizing_basis", "")
                        or ""
                    ).upper() == "SIMULATED_MICRO_EQUITY"
                ):
                    continue

                linked_order_id = str(
                    position.get(
                        f"broker_{prefix}_order_id",
                        "",
                    )
                    or ""
                )
                linked_client_id = str(
                    position.get(
                        f"broker_{prefix}_client_order_id",
                        "",
                    )
                    or ""
                )

                if not (linked_order_id or linked_client_id):
                    continue

                order = (
                    order_by_id.get(linked_order_id)
                    if linked_order_id
                    else None
                )
                if order is None and linked_client_id:
                    order = order_by_client_id.get(
                        linked_client_id
                    )

                canonical_order_id = str(
                    (
                        order.get("id", "")
                        if order is not None
                        else linked_order_id
                    )
                    or ""
                )
                canonical_client_id = str(
                    (
                        order.get("client_order_id", "")
                        if order is not None
                        else linked_client_id
                    )
                    or ""
                )

                if canonical_order_id:
                    identity = ("ORDER", canonical_order_id)
                else:
                    identity = ("CLIENT", canonical_client_id)

                groups.setdefault(
                    identity,
                    []
                ).append(
                    {
                        "position_id": str(position_id),
                        "position": position,
                        "order": order,
                        "order_id": canonical_order_id,
                        "client_id": canonical_client_id,
                    }
                )

            for identity, candidates in groups.items():
                owner = max(
                    candidates,
                    key=lambda candidate: self._broker_claim_owner_priority(
                        candidate["position"],
                        order=candidate["order"],
                        prefix=prefix,
                    ),
                )

                owner_id = owner["position_id"]
                order_id = owner["order_id"]
                client_id = owner["client_id"]

                if order_id:
                    claims[prefix]["order_id_owner"][
                        order_id
                    ] = owner_id
                if client_id:
                    claims[prefix]["client_id_owner"][
                        client_id
                    ] = owner_id

                if prefix == "entry":
                    signature = self._lifecycle_position_claim_signature(
                        owner["position"]
                    )
                    if signature:
                        claims["entry_signature_owners"].setdefault(
                            signature,
                            set(),
                        ).add(owner_id)

                    for candidate in candidates:
                        candidate_id = candidate["position_id"]
                        if candidate_id == owner_id:
                            continue

                        claims["duplicate_entry_claims"][
                            candidate_id
                        ] = {
                            "owner_position_id": owner_id,
                            "order_id": order_id,
                            "client_order_id": client_id,
                            "identity": identity,
                        }

        return claims


    def _supersede_duplicate_broker_claim(
        self,
        position,
        duplicate_claim,
    ):
        """Terminalize a lifecycle that duplicated another broker link."""

        prior_status = self._normalize_lifecycle_status(
            position.get(
                "status",
                "ALERTED",
            )
        )

        owner_position_id = str(
            duplicate_claim.get(
                "owner_position_id",
                "",
            )
            or ""
        )

        archived_claim = {
            "broker_entry_order_id": position.get(
                "broker_entry_order_id",
                "",
            ),
            "broker_entry_client_order_id": position.get(
                "broker_entry_client_order_id",
                "",
            ),
            "broker_entry_order_status": position.get(
                "broker_entry_order_status",
                "",
            ),
            "broker_entry_filled_qty": position.get(
                "broker_entry_filled_qty",
                0.0,
            ),
            "broker_entry_filled_avg_price": position.get(
                "broker_entry_filled_avg_price",
                None,
            ),
            "broker_open_quantity": position.get(
                "broker_open_quantity",
                0.0,
            ),
            "broker_peak_open_quantity": position.get(
                "broker_peak_open_quantity",
                0.0,
            ),
        }

        position["superseded_broker_claim"] = archived_claim
        position["superseded_by_lifecycle_id"] = owner_position_id
        position["superseded_at"] = self.get_datetime().isoformat()

        for field, reset_value in {
            "broker_entry_order_id": "",
            "broker_entry_client_order_id": "",
            "broker_entry_order_status": "",
            "broker_entry_filled_qty": 0.0,
            "broker_entry_filled_avg_price": None,
            "broker_open_quantity": 0.0,
            "broker_peak_open_quantity": 0.0,
            "broker_leg_signed_quantities": {},
        }.items():
            position[field] = reset_value

        reason = (
            "Broker entry order/position claim is owned by lifecycle "
            f"{owner_position_id}; this duplicate heuristic claim was "
            "superseded and no longer represents distinct exposure"
        )

        position["broker_reconciliation_state"] = (
            "DUPLICATE_BROKER_CLAIM"
        )
        position["broker_reconciliation_note"] = reason
        position["last_reconciliation_at"] = (
            self.get_datetime().isoformat()
        )

        self._transition_trade_lifecycle(
            position,
            "SUPERSEDED",
            reason,
            details={
                "owner_position_id": owner_position_id,
                "broker_order_id": duplicate_claim.get(
                    "order_id",
                    "",
                ),
                "client_order_id": duplicate_claim.get(
                    "client_order_id",
                    "",
                ),
            },
        )

        return {
            "position_id": position.get("id", ""),
            "asset_type": position.get("asset_type", "OPTION"),
            "underlying": position.get("underlying", ""),
            "prior_status": prior_status,
            "status": position.get("status", "SUPERSEDED"),
            "broker_open_qty": 0.0,
            "entry_order_status": "SUPERSEDED",
            "close_order_status": "NONE",
            "match_confidence": "DUPLICATE_BROKER_CLAIM",
            "reason": reason,
        }


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

        claim_ownership = snapshot.get(
            "broker_claim_ownership",
            {},
        )

        current_position_id = str(
            position.get(
                "id",
                "",
            )
            or ""
        )

        position_claim_signature = (
            self._lifecycle_position_claim_signature(
                position
            )
        )

        signature_owners = set(
            claim_ownership.get(
                "entry_signature_owners",
                {},
            ).get(
                position_claim_signature,
                set(),
            )
            or set()
        )

        position_claim_suppressed = (
            bool(signature_owners)
            and current_position_id
            not in signature_owners
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

            raw_actual = float(
                signed_qty.get(
                    symbol,
                    0.0,
                )
                or 0.0
            )

            actual = (
                0.0
                if position_claim_suppressed
                else raw_actual
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
            ] = raw_actual

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
                    claim_owners=claim_ownership,
                )
            )

            close_orders = (
                self._matching_lifecycle_orders(
                    position,
                    snapshot[
                        "normalized_orders"
                    ],
                    close=True,
                    claim_owners=claim_ownership,
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

        entry_outcome = self._persist_order_outcome_metadata(
            position,
            "entry",
            entry_evidence,
            expected_quantity,
            now,
        )

        close_outcome = self._persist_order_outcome_metadata(
            position,
            "close",
            close_evidence,
            max(
                broker_peak,
                expected_quantity,
            ),
            now,
        )

        self._update_lifecycle_fill_accounting(
            position,
            snapshot,
            entry_evidence,
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

            # A broken multi-leg ratio is not a healthy partial spread.
            # Allow only a short order/position synchronization grace
            # when broker fill evidence has just arrived; otherwise
            # surface the unpaired exposure as ORPHANED for attention.
            sync_prefix = (
                "close"
                if previously_open
                else "entry"
            )
            sync_evidence = (
                close_evidence
                if previously_open
                else entry_evidence
            )
            sync_outcome = (
                close_outcome
                if previously_open
                else entry_outcome
            )

            if (
                sync_outcome.get(
                    "has_fill",
                    False,
                )
                and self._order_position_sync_grace_active(
                    position,
                    sync_prefix,
                    sync_evidence.get(
                        "latest"
                    ),
                    now,
                )
            ):
                new_status = (
                    "CLOSE_WORKING"
                    if previously_open
                    else "ENTRY_WORKING"
                )
                reason = (
                    "Only part of the expected multi-leg broker "
                    "position is visible while broker fill activity "
                    "is inside the position-sync grace period"
                )
                match_confidence = "ORDER_POSITION_SYNC_GRACE"
            else:
                new_status = "ORPHANED"
                reason = (
                    "Only part of the expected multi-leg broker "
                    "position is present; broken-leg exposure is not "
                    "treated as a valid partial spread"
                )
                match_confidence = "POSITION_PARTIAL_LEGS"

        elif broker_open_quantity > tolerance:

            match_confidence = (
                "POSITION_FULL"
                if full_position_match
                else "POSITION_PARTIAL"
            )

            close_working = bool(
                close_outcome.get(
                    "working",
                    False,
                )
            )
            close_has_fill = bool(
                close_outcome.get(
                    "has_fill",
                    False,
                )
            )
            close_terminal = bool(
                close_outcome.get(
                    "terminal",
                    False,
                )
            )
            close_category = str(
                close_outcome.get(
                    "category",
                    "NONE",
                )
                or "NONE"
            )

            if full_position_match:

                if (
                    close_has_fill
                    and previously_open
                ):

                    close_latest = close_evidence.get(
                        "latest"
                    )

                    if self._order_position_sync_grace_active(
                        position,
                        "close",
                        close_latest,
                        now,
                    ):
                        new_status = "CLOSE_WORKING"
                        reason = (
                            "Matching close order reports broker fill "
                            "activity; awaiting broker position snapshot "
                            "to reflect the reduction"
                        )
                        match_confidence = (
                            "ORDER_POSITION_SYNC_GRACE"
                        )
                    else:
                        new_status = "ORPHANED"
                        reason = (
                            "Matching close order reports fill activity "
                            "but the full broker position remains present "
                            "beyond the position-sync grace period"
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
                    close_terminal
                    and previously_open
                ):

                    new_status = "CLOSE_ALERTED"
                    retry_after = str(
                        position.get(
                            "paper_exit_retry_after_date",
                            "",
                        )
                        or ""
                    )
                    reason = (
                        "Matching close order reached terminal outcome "
                        f"{close_category} while the broker position "
                        "remains open; no same-day retry/chase is allowed"
                        + (
                            f"; retry eligible on/after {retry_after}"
                            if retry_after
                            else ""
                        )
                    )
                    match_confidence = "ORDER_TERMINAL_POSITION_OPEN"

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

                    if close_working:
                        reason = (
                            "Broker position is partially closed and "
                            "the close order remains working for the "
                            "remaining quantity"
                        )
                    elif close_terminal:
                        retry_after = str(
                            position.get(
                                "paper_exit_retry_after_date",
                                "",
                            )
                            or ""
                        )
                        reason = (
                            "Broker position is partially closed; the "
                            f"remaining close order is {close_category} "
                            "and the residual position remains managed "
                            "without same-day chasing"
                            + (
                                f"; retry eligible on/after {retry_after}"
                                if retry_after
                                else ""
                            )
                        )
                    else:
                        reason = (
                            "Broker position quantity is below the "
                            "previously open quantity"
                        )
                else:
                    new_status = "PARTIALLY_OPEN"

                    if (
                        entry_outcome.get(
                            "has_fill",
                            False,
                        )
                        and entry_outcome.get(
                            "terminal",
                            False,
                        )
                    ):
                        reason = (
                            "Entry order partially filled and its "
                            f"unfilled remainder is {entry_outcome.get('category', 'TERMINAL')}; "
                            "the smaller broker position is retained and "
                            "will not be topped up automatically"
                        )
                    elif entry_outcome.get(
                        "working",
                        False,
                    ):
                        reason = (
                            "Entry order is partially filled; broker "
                            "position is below target quantity and the "
                            "remaining order is still working"
                        )
                    else:
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

            elif entry_outcome.get(
                "has_fill",
                False,
            ):

                entry_latest = entry_evidence.get(
                    "latest"
                )

                if self._order_position_sync_grace_active(
                    position,
                    "entry",
                    entry_latest,
                    now,
                ):
                    new_status = "ENTRY_WORKING"
                    reason = (
                        "Matching entry order reports fill activity; "
                        "awaiting broker position snapshot to reflect "
                        "the new exposure"
                    )
                    match_confidence = (
                        "ORDER_POSITION_SYNC_GRACE"
                    )
                else:
                    new_status = "ORPHANED"
                    reason = (
                        "Matching entry order reports fill activity but "
                        "no current broker position is present beyond "
                        "the position-sync grace period"
                    )
                    match_confidence = (
                        "ORDER_POSITION_CONFLICT"
                    )

            elif entry_working is not None:

                new_status = "ENTRY_WORKING"
                reason = (
                    "Matching broker entry order is working"
                )
                match_confidence = "ORDER_ONLY"

            elif entry_terminal is not None:

                category = str(
                    entry_outcome.get(
                        "category",
                        "UNKNOWN",
                    )
                    or "UNKNOWN"
                )

                if category == "REJECTED":
                    new_status = "REJECTED"
                elif category == "EXPIRED":
                    new_status = "EXPIRED"
                elif category == "CANCELED":
                    new_status = "CANCELED"
                else:
                    new_status = "ORPHANED"

                reason = (
                    "Matching broker entry order reached terminal "
                    f"outcome {category} without a broker position"
                )
                match_confidence = (
                    "ORDER_ONLY"
                    if new_status in {
                        "REJECTED",
                        "EXPIRED",
                        "CANCELED",
                    }
                    else "ORDER_TERMINAL_UNEXPECTED"
                )

            if (
                new_status == prior_status
                and entry_evidence.get(
                    "latest"
                ) is None
                and close_evidence.get(
                    "latest"
                ) is None
            ):

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

                        if position_claim_suppressed:
                            owners_text = ", ".join(
                                sorted(signature_owners)
                            )
                            reason = (
                                "Matching broker position/order is "
                                "exclusively claimed by linked lifecycle "
                                f"{owners_text}; this lifecycle remains "
                                "unmatched"
                            )
                            match_confidence = (
                                "CLAIMED_BY_OTHER_LIFECYCLE"
                            )
                        else:
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
                "entry_order_outcome": entry_outcome.get(
                    "category",
                    "NONE",
                ),
                "close_order_outcome": close_outcome.get(
                    "category",
                    "NONE",
                ),
                "paper_exit_retry_after_date": position.get(
                    "paper_exit_retry_after_date",
                    "",
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
            "entry_order_outcome": entry_outcome.get(
                "category",
                "NONE",
            ),
            "close_order_outcome": close_outcome.get(
                "category",
                "NONE",
            ),
            "exit_retry_after": position.get(
                "paper_exit_retry_after_date",
                "",
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

        snapshot[
            "broker_claim_ownership"
        ] = self._build_lifecycle_broker_claim_ownership(
            snapshot
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

            duplicate_claim = (
                snapshot.get(
                    "broker_claim_ownership",
                    {},
                )
                .get(
                    "duplicate_entry_claims",
                    {},
                )
                .get(
                    str(position_id)
                )
            )

            if duplicate_claim:
                row = self._supersede_duplicate_broker_claim(
                    position,
                    duplicate_claim,
                )
            else:
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
    # PRODUCTION SAFETY / HARDENING
    # ======================================================

    @staticmethod
    def _env_bool(name, default=False):
        raw = os.environ.get(name, None)
        if raw is None:
            return bool(default)
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _env_float(name, default):
        raw = os.environ.get(name, None)
        if raw is None or str(raw).strip() == "":
            return float(default)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _env_int(name, default):
        raw = os.environ.get(name, None)
        if raw is None or str(raw).strip() == "":
            return int(default)
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            return int(default)

    def _validate_trade_state_payload(self, state):
        if not isinstance(state, dict):
            raise ValueError("lifecycle state root must be a JSON object")
        positions = state.get("positions", None)
        if not isinstance(positions, list):
            raise ValueError("lifecycle state positions must be a list")
        seen = set()
        for index, position in enumerate(positions):
            if not isinstance(position, dict):
                raise ValueError(f"lifecycle record {index} is not an object")
            lifecycle_id = str(position.get("id", "") or "").strip()
            if not lifecycle_id:
                raise ValueError(f"lifecycle record {index} has no id")
            if lifecycle_id in seen:
                raise ValueError(f"duplicate lifecycle id: {lifecycle_id}")
            seen.add(lifecycle_id)
        return state

    def _read_valid_trade_state_file(self, path):
        with open(path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
        return self._validate_trade_state_payload(state)

    def _trade_state_backup_candidates(self):
        directory = str(getattr(self, "trade_state_backup_dir", "") or "")
        if not directory or not os.path.isdir(directory):
            return []
        candidates = []
        for name in os.listdir(directory):
            if not name.startswith("trade-state-") or not name.endswith(".json"):
                continue
            path = os.path.join(directory, name)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            candidates.append((mtime, path))
        candidates.sort(reverse=True)
        return [path for _, path in candidates]

    def _prune_trade_state_backups(self):
        max_files = max(3, int(getattr(self, "trade_state_backup_max_files", 25)))
        for path in self._trade_state_backup_candidates()[max_files:]:
            try:
                os.remove(path)
            except OSError:
                pass

    def _backup_current_trade_state(self, force=False):
        if not bool(getattr(self, "trade_state_backup_enabled", True)):
            return None
        path = str(getattr(self, "trade_alert_positions_path", "") or "")
        directory = str(getattr(self, "trade_state_backup_dir", "") or "")
        if not path or not directory or not os.path.exists(path):
            return None

        now = datetime.now(timezone.utc)
        last = getattr(self, "_last_trade_state_backup_at", None)
        minimum = float(
            getattr(self, "trade_state_backup_min_interval_seconds", 60.0) or 0.0
        )
        if (
            not force
            and isinstance(last, datetime)
            and (now - last).total_seconds() < minimum
        ):
            return None

        try:
            self._read_valid_trade_state_file(path)
            with open(path, "rb") as handle:
                raw = handle.read()
        except Exception:
            return None

        digest = hashlib.sha256(raw).hexdigest()[:12]
        os.makedirs(directory, exist_ok=True)
        stamp = now.strftime("%Y%m%dT%H%M%S%fZ")
        backup_path = os.path.join(
            directory,
            f"trade-state-{stamp}-{digest}.json",
        )
        temporary = backup_path + ".tmp"
        shutil.copy2(path, temporary)
        self._read_valid_trade_state_file(temporary)
        os.replace(temporary, backup_path)
        self._last_trade_state_backup_at = now
        self._prune_trade_state_backups()
        return backup_path

    def _quarantine_corrupt_trade_state(self, path):
        if not path or not os.path.exists(path):
            return ""
        directory = str(getattr(self, "trade_state_backup_dir", "") or "")
        if not directory:
            return ""
        try:
            os.makedirs(directory, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            target = os.path.join(directory, f"CORRUPT-{stamp}.json")
            shutil.copy2(path, target)
            return target
        except Exception:
            return ""

    def _load_trade_state_with_recovery(self, path):
        if not path or not os.path.exists(path):
            self._lifecycle_state_integrity_ok = True
            return None

        try:
            state = self._read_valid_trade_state_file(path)
            self._lifecycle_state_integrity_ok = True
            return state
        except Exception as primary_exc:
            self._lifecycle_state_integrity_ok = False
            quarantined = self._quarantine_corrupt_trade_state(path)
            for backup_path in self._trade_state_backup_candidates():
                try:
                    state = self._read_valid_trade_state_file(backup_path)
                    temporary = path + ".recovery.tmp"
                    shutil.copy2(backup_path, temporary)
                    self._read_valid_trade_state_file(temporary)
                    os.replace(temporary, path)
                    self._lifecycle_state_integrity_ok = True
                    self._lifecycle_state_recovered_from_backup = True
                    self._lifecycle_state_recovery_reason = (
                        f"primary invalid ({primary_exc}); restored {backup_path}"
                    )
                    message = (
                        "Corrupt lifecycle primary was restored from valid backup "
                        f"{backup_path}."
                        + (
                            f" Corrupt copy preserved at {quarantined}."
                            if quarantined
                            else ""
                        )
                    )
                    self.log_message("STATE RECOVERY: " + message)
                    self._record_trading_anomaly(
                        "STATE_RECOVERY",
                        "WARNING",
                        message,
                        context={"backup_path": backup_path, "quarantined_path": quarantined},
                    )
                    return state
                except Exception:
                    continue

            self._lifecycle_state_recovery_reason = (
                "primary lifecycle state invalid and no valid backup exists: "
                f"{primary_exc}"
            )
            message = (
                "CRITICAL STATE RECOVERY FAILURE: lifecycle state is corrupt and "
                "no valid backup is available. NEW entries are forbidden."
            )
            self.log_message(message)
            self._record_trading_anomaly(
                "STATE_RECOVERY_FAILURE",
                "CRITICAL",
                message,
                context={"reason": self._lifecycle_state_recovery_reason},
            )
            if bool(getattr(self, "trade_state_fail_fast_on_unrecoverable", True)):
                raise RuntimeError(message) from primary_exc
            return None

    def _path_writable_health(self, path, label, directory_only=False):
        if not path:
            return {
                "check": label,
                "status": "WARN",
                "detail": "path disabled/unset",
            }
        try:
            directory = (
                path
                if directory_only
                else os.path.dirname(os.path.abspath(path))
            )
            if not directory:
                directory = os.getcwd()
            os.makedirs(directory, exist_ok=True)
            probe = os.path.join(
                directory,
                f".healthcheck-{os.getpid()}-{label.replace(' ', '_')}.tmp",
            )
            with open(probe, "w", encoding="utf-8") as handle:
                handle.write("ok\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.remove(probe)
            return {"check": label, "status": "PASS", "detail": directory}
        except Exception as exc:
            return {"check": label, "status": "FAIL", "detail": str(exc)}

    def _run_startup_health_check(self):
        results = []
        market_clock_timestamp = None

        lifecycle_ok = bool(
            getattr(self, "_lifecycle_state_integrity_ok", False)
        )
        lifecycle_detail = (
            "valid"
            + (
                "; recovered from backup"
                if getattr(self, "_lifecycle_state_recovered_from_backup", False)
                else ""
            )
            if lifecycle_ok
            else str(getattr(self, "_lifecycle_state_recovery_reason", "invalid"))
        )
        results.append(
            {
                "check": "lifecycle_state_integrity",
                "status": "PASS" if lifecycle_ok else "FAIL",
                "detail": lifecycle_detail,
            }
        )

        try:
            account = self.alpaca_trading_client.get_account()
            blocked_fields = [
                name
                for name in (
                    "trading_blocked",
                    "account_blocked",
                    "trade_suspended_by_user",
                )
                if bool(getattr(account, name, False))
            ]
            if blocked_fields:
                results.append(
                    {
                        "check": "alpaca_account",
                        "status": "FAIL",
                        "detail": "blocked flags: " + ",".join(blocked_fields),
                    }
                )
            else:
                results.append(
                    {
                        "check": "alpaca_account",
                        "status": "PASS",
                        "detail": f"equity={getattr(account, 'equity', None)}",
                    }
                )
        except Exception as exc:
            results.append(
                {"check": "alpaca_account", "status": "FAIL", "detail": str(exc)}
            )

        try:
            clock = self.alpaca_trading_client.get_clock()
            timestamp = getattr(clock, "timestamp", None)
            if timestamp is None:
                raise ValueError("market clock returned no timestamp")
            market_clock_timestamp = timestamp
            results.append(
                {
                    "check": "alpaca_market_clock",
                    "status": "PASS",
                    "detail": str(timestamp),
                }
            )
        except Exception as exc:
            results.append(
                {
                    "check": "alpaca_market_clock",
                    "status": "FAIL",
                    "detail": str(exc),
                }
            )

        try:
            broker_snapshot = self._get_broker_lifecycle_snapshot()
            status = (
                "PASS"
                if (
                    broker_snapshot.get("positions_available")
                    and broker_snapshot.get("orders_available")
                )
                else "FAIL"
            )
            detail = (
                f"positions={len(broker_snapshot.get('positions', []))}; "
                f"orders={len(broker_snapshot.get('orders', []))}; "
                f"truncated={bool(broker_snapshot.get('orders_truncated'))}"
            )
            if broker_snapshot.get("positions_error"):
                detail += f"; positions_error={broker_snapshot['positions_error']}"
            if broker_snapshot.get("orders_error"):
                detail += f"; orders_error={broker_snapshot['orders_error']}"
            results.append(
                {
                    "check": "broker_positions_orders",
                    "status": status,
                    "detail": detail,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "check": "broker_positions_orders",
                    "status": "FAIL",
                    "detail": str(exc),
                }
            )

        if self.paper_execution_armed or self.paper_exit_execution_armed:
            results.append(
                {
                    "check": "paper_execution_safety",
                    "status": "PASS" if self.alpaca_is_paper else "FAIL",
                    "detail": (
                        "ALPACA_IS_PAPER=true"
                        if self.alpaca_is_paper
                        else "execution arm active on non-paper account"
                    ),
                }
            )
            try:
                level = int(self.options_trading_level)
            except (TypeError, ValueError):
                level = None
            results.append(
                {
                    "check": "options_trading_level",
                    "status": "PASS" if level is not None and level >= 2 else "FAIL",
                    "detail": str(level),
                }
            )

        circuit_state_ok = bool(
            getattr(self, "_circuit_breaker_state_integrity_ok", True)
        )
        results.append(
            {
                "check": "circuit_breaker_state_integrity",
                "status": "PASS" if circuit_state_ok else "FAIL",
                "detail": (
                    "valid"
                    if circuit_state_ok
                    else str(
                        getattr(
                            self,
                            "_circuit_breaker_state_integrity_reason",
                            "invalid",
                        )
                    )
                ),
            }
        )

        results.append(
            self._path_writable_health(
                self.trade_alert_positions_path,
                "lifecycle_state_path",
            )
        )
        if self.trade_state_backup_enabled:
            results.append(
                self._path_writable_health(
                    self.trade_state_backup_dir,
                    "lifecycle_backup_dir",
                    directory_only=True,
                )
            )
        results.append(
            self._path_writable_health(
                self.trading_circuit_breaker_state_path,
                "circuit_breaker_state_path",
            )
        )
        if bool(self.parameters.get("trade_journal_enabled", True)):
            if self.trade_journal_csv_path:
                results.append(
                    self._path_writable_health(
                        self.trade_journal_csv_path,
                        "trade_journal_path",
                    )
                )
            if self.trade_analytics_json_path:
                results.append(
                    self._path_writable_health(
                        self.trade_analytics_json_path,
                        "trade_analytics_path",
                    )
                )
        if bool(self.parameters.get("daily_operational_summary_enabled", True)):
            results.append(
                self._path_writable_health(
                    self.daily_trading_summary_path,
                    "daily_trading_summary_path",
                )
            )
        if bool(self.parameters.get("trading_anomaly_log_enabled", True)):
            results.append(
                self._path_writable_health(
                    self.trading_anomalies_jsonl_path,
                    "trading_anomalies_path",
                )
            )

        provider = str(
            getattr(self, "earnings_calendar_provider", "") or ""
        ).upper()
        manual_path = str(getattr(self, "earnings_calendar_path", "") or "")
        if manual_path:
            earnings_status = "PASS" if os.path.exists(manual_path) else "WARN"
            earnings_detail = f"manual={manual_path}"
        elif provider == "ALPHAVANTAGE":
            earnings_status = (
                "PASS" if bool(getattr(self, "alphavantage_api_key", "")) else "WARN"
            )
            earnings_detail = (
                "Alpha Vantage key configured"
                if earnings_status == "PASS"
                else "Alpha Vantage key missing; event-risk layer will fail closed"
            )
        else:
            earnings_status = "WARN"
            earnings_detail = f"provider={provider or 'NONE'}"
        results.append(
            {
                "check": "earnings_event_risk_config",
                "status": earnings_status,
                "detail": earnings_detail,
            }
        )

        kill_active, kill_env, kill_file = self._kill_switch_active()
        results.append(
            {
                "check": "emergency_kill_switch",
                "status": "BLOCK" if kill_active else "PASS",
                "detail": (
                    "active via "
                    + ",".join(
                        source
                        for source, active in (("env", kill_env), ("file", kill_file))
                        if active
                    )
                    if kill_active
                    else "inactive"
                ),
            }
        )

        failures = [row for row in results if row.get("status") == "FAIL"]
        self._startup_health_results = results
        self.startup_health_entries_allowed = not failures

        # Evaluate current breaker state before printing the startup gate so the
        # operator sees the same decision that entry execution will enforce.
        circuit_state = self.refresh_trading_circuit_breakers(
            market_now=market_clock_timestamp,
            log_status=False,
        )
        gate_allowed, gate_reasons = self._new_entry_execution_safety_gate()

        report = {
            "generated_at": self.get_datetime().isoformat(),
            "startup_health_passed": bool(self.startup_health_entries_allowed),
            "new_entries_allowed": bool(gate_allowed),
            "new_entry_block_reasons": list(gate_reasons),
            "circuit_breaker": circuit_state,
            "results": results,
        }
        if self.startup_health_report_path:
            try:
                self._atomic_write_json(report, self.startup_health_report_path)
            except Exception as exc:
                results.append(
                    {
                        "check": "startup_health_report",
                        "status": "WARN",
                        "detail": str(exc),
                    }
                )

        for row in failures:
            self._record_trading_anomaly(
                "STARTUP_HEALTH_FAILURE",
                "ERROR",
                f"{row.get('check')}: {row.get('detail')}",
                context=row,
            )
        if kill_active:
            self._record_trading_anomaly(
                "EMERGENCY_KILL_SWITCH_ACTIVE",
                "INFO",
                "Emergency kill switch is active; new entries are blocked while exits remain enabled.",
                context={"env": bool(kill_env), "file": bool(kill_file)},
            )

        self.log_message(
            "\n\n===== STARTUP HEALTH CHECK =====\n"
            + pd.DataFrame(results)[["check", "status", "detail"]].to_string(
                index=False
            )
            + "\n================================\n"
            + (
                "NEW ENTRY GATE: ALLOWED"
                if gate_allowed
                else (
                    "NEW ENTRY GATE: BLOCKED; existing positions/exits remain managed; "
                    "reasons=" + ",".join(gate_reasons)
                )
            )
        )
        self.refresh_daily_operational_summary()
        return report

    def _load_recent_anomaly_keys(self):
        self._anomaly_emitted_keys = set()
        path = str(getattr(self, "trading_anomalies_jsonl_path", "") or "")
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                lines = handle.readlines()[-2000:]
            for line in lines:
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                key = str(payload.get("anomaly_key", "") or "")
                if key:
                    self._anomaly_emitted_keys.add(key)
        except Exception:
            # Anomaly reporting must never block trading/lifecycle management.
            return

    def _record_trading_anomaly(
        self,
        code,
        severity,
        message,
        lifecycle_id="",
        underlying="",
        context=None,
    ):
        if not bool(self.parameters.get("trading_anomaly_log_enabled", True)):
            return False
        path = str(getattr(self, "trading_anomalies_jsonl_path", "") or "")
        if not path:
            return False

        now = self.get_datetime()
        date_key = now.date().isoformat() if isinstance(now, datetime) else str(date.today())
        stable = "|".join(
            [
                date_key,
                str(code or "UNKNOWN"),
                str(lifecycle_id or ""),
                str(underlying or ""),
                str(message or ""),
            ]
        )
        anomaly_key = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:24]
        if anomaly_key in getattr(self, "_anomaly_emitted_keys", set()):
            return False

        payload = {
            "anomaly_key": anomaly_key,
            "timestamp": now.isoformat() if isinstance(now, datetime) else datetime.now(timezone.utc).isoformat(),
            "trading_date": date_key,
            "severity": str(severity or "WARNING").upper(),
            "code": str(code or "UNKNOWN").upper(),
            "message": str(message or ""),
            "lifecycle_id": str(lifecycle_id or ""),
            "underlying": str(underlying or ""),
            "context": context if isinstance(context, dict) else {},
        }
        try:
            directory = os.path.dirname(os.path.abspath(path))
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._anomaly_emitted_keys.add(anomaly_key)
            self.log_message(
                "TRADING ANOMALY: "
                f"{payload['severity']} {payload['code']}"
                + (f" {payload['underlying']}" if payload["underlying"] else "")
                + f" | {payload['message']}"
            )
            return True
        except Exception as exc:
            self.log_message(f"Could not persist trading anomaly log: {exc}")
            return False

    def _scan_and_record_trading_anomalies(self, lifecycle_snapshot=None):
        snapshot = lifecycle_snapshot if isinstance(lifecycle_snapshot, dict) else {}
        if snapshot:
            if not snapshot.get("positions_available", True):
                self._record_trading_anomaly(
                    "BROKER_POSITIONS_UNAVAILABLE",
                    "ERROR",
                    "Broker position snapshot is unavailable; reconciliation fails closed.",
                    context={"error": snapshot.get("positions_error", "")},
                )
            if not snapshot.get("orders_available", True):
                self._record_trading_anomaly(
                    "BROKER_ORDERS_UNAVAILABLE",
                    "ERROR",
                    "Broker order snapshot is unavailable; reconciliation fails closed.",
                    context={"error": snapshot.get("orders_error", "")},
                )
            if snapshot.get("orders_truncated"):
                self._record_trading_anomaly(
                    "BROKER_ORDER_HISTORY_TRUNCATED",
                    "WARNING",
                    "Broker order history hit the configured reconciliation limit.",
                    context={"order_count": len(snapshot.get("orders", []))},
                )

        terminal_order_alerts = {"REJECTED", "CANCELED", "EXPIRED", "DONE_FOR_DAY"}
        for lifecycle_id, position in self._tracked_alert_positions.items():
            if not isinstance(position, dict):
                continue
            underlying = str(position.get("underlying", "") or "")
            status = self._normalize_lifecycle_status(position.get("status", ""))
            if status == "ORPHANED":
                self._record_trading_anomaly(
                    "ORPHANED_LIFECYCLE",
                    "CRITICAL",
                    str(position.get("status_reason", "") or "Lifecycle is ORPHANED."),
                    lifecycle_id=lifecycle_id,
                    underlying=underlying,
                )

            entry_status = str(position.get("broker_entry_order_status", "") or "").upper()
            if entry_status in terminal_order_alerts:
                self._record_trading_anomaly(
                    "ENTRY_ORDER_TERMINAL",
                    "WARNING",
                    f"Entry order reached terminal status {entry_status}.",
                    lifecycle_id=lifecycle_id,
                    underlying=underlying,
                    context={"broker_order_id": position.get("broker_entry_order_id", "")},
                )

            close_status = str(position.get("broker_close_order_status", "") or "").upper()
            open_qty = self._lifecycle_float(position.get("broker_open_quantity", 0.0), 0.0) or 0.0
            if close_status in terminal_order_alerts and open_qty > 0:
                self._record_trading_anomaly(
                    "EXIT_ORDER_TERMINAL_WITH_REMAINDER",
                    "WARNING",
                    f"Exit order reached terminal status {close_status} with {open_qty:g} contract(s) still open.",
                    lifecycle_id=lifecycle_id,
                    underlying=underlying,
                    context={
                        "broker_order_id": position.get("broker_close_order_id", ""),
                        "retry_after_date": position.get("paper_exit_retry_after_date", ""),
                    },
                )

            if position.get("paper_execution_last_error"):
                self._record_trading_anomaly(
                    "ENTRY_EXECUTION_ERROR",
                    "ERROR",
                    str(position.get("paper_execution_last_error")),
                    lifecycle_id=lifecycle_id,
                    underlying=underlying,
                )
            if position.get("paper_exit_execution_last_error"):
                self._record_trading_anomaly(
                    "EXIT_EXECUTION_ERROR",
                    "ERROR",
                    str(position.get("paper_exit_execution_last_error")),
                    lifecycle_id=lifecycle_id,
                    underlying=underlying,
                )

    def _anomaly_counts_for_date(self, trade_date):
        counts = {"total": 0, "CRITICAL": 0, "ERROR": 0, "WARNING": 0, "INFO": 0}
        path = str(getattr(self, "trading_anomalies_jsonl_path", "") or "")
        if not path or not os.path.exists(path):
            return counts
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if str(row.get("trading_date", "")) != trade_date.isoformat():
                        continue
                    severity = str(row.get("severity", "INFO") or "INFO").upper()
                    counts["total"] += 1
                    counts[severity] = counts.get(severity, 0) + 1
        except Exception:
            return counts
        return counts

    def refresh_daily_operational_summary(
        self,
        session_status=None,
        lifecycle_snapshot=None,
        analytics=None,
    ):
        if not bool(self.parameters.get("daily_operational_summary_enabled", True)):
            return None
        path = str(getattr(self, "daily_trading_summary_path", "") or "")
        if not path:
            return None

        now = self.get_datetime()
        session = session_status if isinstance(session_status, dict) else {}
        market_now = session.get("now") if session else None
        trade_date = self._current_trading_date(market_now)

        try:
            account = self._get_account_risk_snapshot()
        except Exception as exc:
            account = {"error": str(exc)}
            self._record_trading_anomaly(
                "DAILY_SUMMARY_ACCOUNT_UNAVAILABLE",
                "ERROR",
                f"Operational summary could not read account snapshot: {exc}",
            )

        if analytics is None:
            try:
                analytics = self._build_trade_analytics(pd.DataFrame(self._trade_journal_rows()))
            except Exception as exc:
                analytics = {"error": str(exc)}

        statuses = {}
        open_positions = []
        for lifecycle_id, position in self._tracked_alert_positions.items():
            if not isinstance(position, dict):
                continue
            status = self._normalize_lifecycle_status(position.get("status", "UNKNOWN"))
            statuses[status] = statuses.get(status, 0) + 1
            open_qty = self._lifecycle_float(position.get("broker_open_quantity", 0.0), 0.0) or 0.0
            if open_qty > 0:
                open_positions.append(
                    {
                        "lifecycle_id": lifecycle_id,
                        "underlying": str(position.get("underlying", "") or ""),
                        "decision": str(position.get("decision", "") or ""),
                        "status": status,
                        "open_qty": open_qty,
                        "actual_entry_fill_per_share": self._trade_journal_json_number(
                            position.get("actual_entry_debit_per_share")
                        ),
                        "realized_pnl_dollars": self._trade_journal_json_number(
                            position.get("realized_pnl_dollars", 0.0)
                        ),
                    }
                )

        gate_allowed, gate_reasons = self._new_entry_execution_safety_gate()
        circuit_state = dict(getattr(self, "_trading_circuit_breaker_state", {}) or {})
        snapshot = lifecycle_snapshot if isinstance(lifecycle_snapshot, dict) else {}
        anomaly_counts = self._anomaly_counts_for_date(trade_date)

        summary = {
            "generated_at": now.isoformat() if isinstance(now, datetime) else str(now),
            "trading_date": trade_date.isoformat(),
            "market": {
                "allowed": session.get("allowed") if session else None,
                "now": str(session.get("now")) if session.get("now") is not None else None,
                "reason": str(session.get("reason", "") or ""),
                "actionable_open": str(session.get("actionable_open")) if session.get("actionable_open") is not None else None,
                "actionable_close": str(session.get("actionable_close")) if session.get("actionable_close") is not None else None,
            },
            "account": account,
            "entry_gate": {"allowed": bool(gate_allowed), "block_reasons": list(gate_reasons)},
            "circuit_breaker": circuit_state,
            "lifecycle_status_counts": statuses,
            "open_positions": open_positions,
            "broker_snapshot": {
                "positions_available": snapshot.get("positions_available"),
                "orders_available": snapshot.get("orders_available"),
                "orders_truncated": snapshot.get("orders_truncated"),
                "position_count": len(snapshot.get("positions", [])) if snapshot else None,
                "order_count": len(snapshot.get("orders", [])) if snapshot else None,
            },
            "trade_analytics": analytics or {},
            "anomalies_today": anomaly_counts,
        }
        try:
            self._atomic_write_json(summary, path)
        except Exception as exc:
            self.log_message(f"Daily operational summary persistence failed: {exc}")
            return summary

        signature = (
            bool(gate_allowed),
            tuple(gate_reasons),
            len(open_positions),
            int((analytics or {}).get("completed_trades", 0) or 0),
            round(float((analytics or {}).get("realized_pnl_dollars", 0.0) or 0.0), 2),
            int(anomaly_counts.get("total", 0)),
        )
        if signature != getattr(self, "_last_daily_summary_log_signature", None):
            self._last_daily_summary_log_signature = signature
            self.log_message(
                "DAILY OPERATIONAL SUMMARY: "
                f"open_positions={len(open_positions)}, "
                f"completed_trades={int((analytics or {}).get('completed_trades', 0) or 0)}, "
                f"realized_pnl=${float((analytics or {}).get('realized_pnl_dollars', 0.0) or 0.0):,.2f}, "
                f"entry_gate={'ALLOWED' if gate_allowed else 'BLOCKED'}, "
                f"anomalies_today={int(anomaly_counts.get('total', 0))}."
            )
        return summary

    def _load_trading_circuit_breaker_state(self):
        self._trading_circuit_breaker_state = {}
        self._circuit_breaker_state_integrity_ok = True
        self._circuit_breaker_state_integrity_reason = ""
        path = str(
            getattr(self, "trading_circuit_breaker_state_path", "") or ""
        )
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                state = json.load(handle)
            if not isinstance(state, dict):
                raise ValueError("circuit-breaker state root must be a JSON object")
            self._trading_circuit_breaker_state = state
        except Exception as exc:
            self._circuit_breaker_state_integrity_ok = False
            self._circuit_breaker_state_integrity_reason = str(exc)
            self.log_message(
                "Circuit-breaker state could not be loaded; NEW entries will "
                f"remain blocked until the state file is repaired/removed and "
                f"the strategy is restarted. Reason={exc}"
            )
            self._trading_circuit_breaker_state = {
                "state_load_failed": True,
                "latched_reasons": ["CIRCUIT_BREAKER_STATE_UNREADABLE"],
            }

    def _save_trading_circuit_breaker_state(self):
        path = str(
            getattr(self, "trading_circuit_breaker_state_path", "") or ""
        )
        if not path:
            return
        if not bool(getattr(self, "_circuit_breaker_state_integrity_ok", True)):
            return
        try:
            self._atomic_write_json(self._trading_circuit_breaker_state, path)
        except Exception as exc:
            self.log_message(f"Could not persist circuit-breaker state: {exc}")

    @staticmethod
    def _as_eastern_date(value):
        if not isinstance(value, datetime):
            return None
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo("America/New_York")).date()

    def _current_trading_date(self, market_now=None):
        if isinstance(market_now, datetime):
            result = self._as_eastern_date(market_now)
            if result is not None:
                return result
        now = self.get_datetime()
        if now.tzinfo is None:
            now = now.replace(tzinfo=ZoneInfo("America/New_York"))
        return now.astimezone(ZoneInfo("America/New_York")).date()

    def _daily_realized_pnl_for_date(self, trade_date):
        total = 0.0
        for position in self._tracked_alert_positions.values():
            entry_basis = self._lifecycle_float(
                position.get("actual_entry_debit_per_share", None),
                None,
            )
            if entry_basis is None:
                continue
            ledger = position.get("broker_close_fill_ledger", {}) or {}
            for fill in ledger.values():
                filled_at = self._parse_lifecycle_datetime(
                    fill.get("filled_at", "")
                )
                if self._as_eastern_date(filled_at) != trade_date:
                    continue
                qty = self._lifecycle_float(fill.get("filled_qty", 0.0), 0.0) or 0.0
                credit = self._lifecycle_float(
                    fill.get("credit_per_share", None),
                    None,
                )
                if qty <= 0 or credit is None:
                    continue
                total += (
                    (float(credit) - float(entry_basis))
                    * 100.0
                    * float(qty)
                )
        return float(total)

    def _new_entry_count_for_date(self, trade_date):
        count = 0
        for position in self._tracked_alert_positions.values():
            order_id = str(position.get("broker_entry_order_id", "") or "")
            if not order_id:
                continue
            dt = self._parse_lifecycle_datetime(
                position.get("broker_entry_submitted_at", "")
            )
            if dt is None:
                dt = self._parse_lifecycle_datetime(
                    position.get("actual_entry_filled_at", "")
                )
            if self._as_eastern_date(dt) == trade_date:
                count += 1
        return count

    def _consecutive_losing_closed_trades(self):
        rows = [
            row
            for row in self._trade_journal_rows()
            if row.get("status") == "CLOSED"
        ]
        rows.sort(
            key=lambda row: (
                str(row.get("close_timestamp", "")),
                str(row.get("lifecycle_id", "")),
            ),
            reverse=True,
        )
        count = 0
        for row in rows:
            pnl = self._lifecycle_float(
                row.get("realized_pnl_dollars", 0.0),
                0.0,
            ) or 0.0
            if pnl < 0:
                count += 1
            else:
                break
        return count

    def _kill_switch_active(self):
        env_active = self._env_bool("TRADING_KILL_SWITCH", False)
        file_path = str(getattr(self, "trading_kill_switch_file", "") or "")
        file_active = bool(file_path and os.path.exists(file_path))
        return env_active or file_active, env_active, file_active

    def refresh_trading_circuit_breakers(self, market_now=None, log_status=True):
        if not bool(self.parameters.get("circuit_breaker_enabled", True)):
            self._entry_execution_blocked_reasons = []
            return {"enabled": False, "blocked": False}

        trade_date = self._current_trading_date(market_now)
        state = dict(getattr(self, "_trading_circuit_breaker_state", {}) or {})
        today_text = trade_date.isoformat()
        same_day = state.get("trading_date") == today_text

        if not same_day:
            state = {
                "trading_date": today_text,
                "latched_reasons": [],
                "day_start_equity": None,
                "day_peak_equity": None,
                "last_updated_at": self.get_datetime().isoformat(),
            }

        dynamic_reasons = []
        if not bool(getattr(self, "_circuit_breaker_state_integrity_ok", True)):
            dynamic_reasons.append("CIRCUIT_BREAKER_STATE_UNREADABLE")
        try:
            account = self._get_account_risk_snapshot()
            equity = float(account.get("equity", 0.0) or 0.0)
            if equity <= 0:
                raise ValueError("account equity is not positive")
            if state.get("day_start_equity") in (None, 0, 0.0):
                state["day_start_equity"] = equity
            peak = self._lifecycle_float(
                state.get("day_peak_equity", None),
                None,
            )
            if peak is None or equity > peak:
                peak = equity
            state["day_peak_equity"] = float(peak)
            state["current_equity"] = equity
            state.pop("account_snapshot_error", None)
        except Exception as exc:
            equity = None
            dynamic_reasons.append("ACCOUNT_SNAPSHOT_UNAVAILABLE")
            state["account_snapshot_error"] = str(exc)

        daily_realized = self._daily_realized_pnl_for_date(trade_date)
        new_entries = self._new_entry_count_for_date(trade_date)
        consecutive_losses = self._consecutive_losing_closed_trades()
        orphaned = sorted(
            {
                str(position.get("underlying", "") or "")
                for position in self._tracked_alert_positions.values()
                if self._normalize_lifecycle_status(position.get("status", ""))
                == "ORPHANED"
            }
        )

        state["daily_realized_pnl_dollars"] = float(daily_realized)
        state["new_entries_today"] = int(new_entries)
        state["consecutive_losing_trades"] = int(consecutive_losses)
        state["orphaned_underlyings"] = orphaned

        latched = list(state.get("latched_reasons", []) or [])

        def latch(reason):
            if reason not in latched:
                latched.append(reason)

        start_equity = self._lifecycle_float(
            state.get("day_start_equity", None),
            None,
        )
        loss_pct = max(
            0.0,
            self._env_float(
                "CIRCUIT_BREAKER_MAX_DAILY_REALIZED_LOSS_PCT_EQUITY",
                self.parameters.get(
                    "circuit_breaker_max_daily_realized_loss_pct_equity",
                    0.01,
                ),
            ),
        )
        if (
            start_equity
            and loss_pct > 0
            and daily_realized <= -(start_equity * loss_pct)
        ):
            latch("MAX_DAILY_REALIZED_LOSS")

        drawdown_pct_limit = max(
            0.0,
            self._env_float(
                "CIRCUIT_BREAKER_MAX_DAILY_EQUITY_DRAWDOWN_PCT",
                self.parameters.get(
                    "circuit_breaker_max_daily_equity_drawdown_pct",
                    0.02,
                ),
            ),
        )
        peak_equity = self._lifecycle_float(
            state.get("day_peak_equity", None),
            None,
        )
        drawdown_pct = 0.0
        if equity is not None and peak_equity and peak_equity > 0:
            drawdown_pct = max(0.0, (peak_equity - equity) / peak_equity)
            if drawdown_pct_limit > 0 and drawdown_pct >= drawdown_pct_limit:
                latch("MAX_DAILY_EQUITY_DRAWDOWN")
        state["daily_equity_drawdown_pct"] = float(drawdown_pct)

        max_entries = max(
            0,
            self._env_int(
                "CIRCUIT_BREAKER_MAX_NEW_ENTRIES_PER_DAY",
                self.parameters.get("circuit_breaker_max_new_entries_per_day", 5),
            ),
        )
        if max_entries > 0 and new_entries >= max_entries:
            latch("MAX_NEW_ENTRIES_PER_DAY")

        max_losses = max(
            0,
            self._env_int(
                "CIRCUIT_BREAKER_MAX_CONSECUTIVE_LOSSES",
                self.parameters.get("circuit_breaker_max_consecutive_losses", 3),
            ),
        )
        if max_losses > 0 and consecutive_losses >= max_losses:
            latch("MAX_CONSECUTIVE_LOSSES")

        halt_orphaned = self._env_bool(
            "CIRCUIT_BREAKER_HALT_ON_ORPHANED",
            self.parameters.get("circuit_breaker_halt_on_orphaned", True),
        )
        if halt_orphaned and orphaned:
            latch("ORPHANED_LIFECYCLE")

        kill_active, kill_env, kill_file = self._kill_switch_active()
        if kill_active:
            dynamic_reasons.append("EMERGENCY_KILL_SWITCH")
        if (
            bool(self.parameters.get("startup_health_block_new_entries", True))
            and not bool(getattr(self, "startup_health_entries_allowed", False))
        ):
            dynamic_reasons.append("STARTUP_HEALTH_FAILED")

        state["latched_reasons"] = latched
        state["kill_switch_env"] = bool(kill_env)
        state["kill_switch_file"] = bool(kill_file)
        state["last_updated_at"] = self.get_datetime().isoformat()
        state["blocked"] = bool(latched or dynamic_reasons)
        state["active_block_reasons"] = latched + [
            reason for reason in dynamic_reasons if reason not in latched
        ]

        self._trading_circuit_breaker_state = state
        self._entry_execution_blocked_reasons = list(
            state["active_block_reasons"]
        )
        self._save_trading_circuit_breaker_state()

        for reason in state["active_block_reasons"]:
            if reason == "EMERGENCY_KILL_SWITCH":
                continue
            self._record_trading_anomaly(
                "CIRCUIT_BREAKER_" + str(reason),
                "WARNING",
                f"New-entry circuit breaker is active: {reason}.",
                context={
                    "realized_today": float(daily_realized),
                    "equity_drawdown_pct": float(drawdown_pct),
                    "new_entries_today": int(new_entries),
                    "consecutive_losses": int(consecutive_losses),
                },
            )

        signature = (
            tuple(state["active_block_reasons"]),
            round(float(daily_realized), 2),
            round(float(drawdown_pct), 5),
            int(new_entries),
            int(consecutive_losses),
        )
        if log_status and signature != getattr(self, "_last_circuit_breaker_log_signature", None):
            self._last_circuit_breaker_log_signature = signature
            self.log_message(
                "TRADING CIRCUIT BREAKERS: "
                + (
                    "BLOCKING NEW ENTRIES"
                    if state["blocked"]
                    else "CLEAR"
                )
                + f" | realized_today=${daily_realized:,.2f}"
                + f" | equity_drawdown={drawdown_pct * 100:.2f}%"
                + (
                    f" | new_entries={new_entries}/{max_entries}"
                    if max_entries
                    else f" | new_entries={new_entries}/OFF"
                )
                + (
                    f" | consecutive_losses={consecutive_losses}/{max_losses}"
                    if max_losses
                    else f" | consecutive_losses={consecutive_losses}/OFF"
                )
                + (
                    f" | reasons={','.join(state['active_block_reasons'])}"
                    if state["active_block_reasons"]
                    else ""
                )
            )
        return state

    def _new_entry_execution_safety_gate(self):
        reasons = list(
            getattr(self, "_entry_execution_blocked_reasons", []) or []
        )
        if (
            bool(self.parameters.get("startup_health_block_new_entries", True))
            and not bool(getattr(self, "startup_health_entries_allowed", False))
            and "STARTUP_HEALTH_FAILED" not in reasons
        ):
            reasons.append("STARTUP_HEALTH_FAILED")
        kill_active, _, _ = self._kill_switch_active()
        if kill_active and "EMERGENCY_KILL_SWITCH" not in reasons:
            reasons.append("EMERGENCY_KILL_SWITCH")
        return (not reasons), reasons


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

            state = self._load_trade_state_with_recovery(path)
            if state is None:
                return

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

            # Validate the complete temp file before touching the current
            # lifecycle source of truth, then preserve a rotating valid backup.
            self._read_valid_trade_state_file(temporary_path)
            self._backup_current_trade_state()

            os.replace(
                temporary_path,
                path,
            )

            self._lifecycle_state_integrity_ok = True

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

            "entry_stock_score":
                self._lifecycle_float(
                    row.get(
                        "stock_score",
                        None,
                    ),
                    None,
                ),

            "entry_option_score":
                self._lifecycle_float(
                    row.get(
                        "option_score",
                        None,
                    ),
                    None,
                ),

            "entry_iv":
                self._lifecycle_float(
                    row.get(
                        "iv",
                        None,
                    ),
                    None,
                ),

            "entry_reward_risk":
                self._lifecycle_float(
                    row.get(
                        "reward_risk",
                        None,
                    ),
                    None,
                ),

            "entry_selection_reason":
                str(
                    row.get(
                        "reason",
                        "",
                    )
                    or ""
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

            "actual_entry_debit_per_share":
                None,

            "actual_entry_filled_qty":
                0.0,

            "actual_entry_basis_source":
                "",

            "actual_entry_filled_at":
                "",

            "actual_entry_total_debit":
                0.0,

            "broker_close_fill_ledger":
                {},

            "actual_close_filled_qty":
                0.0,

            "actual_close_avg_credit_per_share":
                None,

            "actual_realized_pnl_dollars":
                0.0,

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

            entry_debit = self._lifecycle_float(
                position.get(
                    "actual_entry_debit_per_share",
                    None,
                ),
                None,
            )

            if (
                entry_debit is None
                or entry_debit <= 0
            ):
                self.log_message(
                    f"{position['underlying']}: exit management skipped; "
                    "broker-confirmed actual entry fill basis is unavailable."
                )
                continue

            broker_open_quantity = self._lifecycle_float(
                position.get(
                    "broker_open_quantity",
                    0.0,
                ),
                0.0,
            ) or 0.0

            if broker_open_quantity <= 0:
                self.log_message(
                    f"{position['underlying']}: exit management skipped; "
                    "no broker-confirmed open quantity remains."
                )
                continue

            quantity = max(
                1,
                int(
                    round(
                        broker_open_quantity
                    )
                ),
            )

            current_total_value = (
                current_value
                * 100
                * quantity
            )

            entry_total_debit = (
                entry_debit
                * 100
                * quantity
            )

            unrealized_pnl_dollars = (
                current_total_value
                - entry_total_debit
            )

            unrealized_pnl_pct = (
                current_value
                / entry_debit
                - 1
            )

            realized_pnl_dollars = self._lifecycle_float(
                position.get(
                    "actual_realized_pnl_dollars",
                    0.0,
                ),
                0.0,
            ) or 0.0

            total_pnl_dollars = (
                realized_pnl_dollars
                + unrealized_pnl_dollars
            )

            # Keep the historic aliases for downstream alert formatting,
            # but they now mean actual-fill unrealized % and total P/L $.
            pnl_pct = unrealized_pnl_pct
            pnl_dollars = total_pnl_dollars

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

                    "entry_basis_source":
                        str(
                            position.get(
                                "actual_entry_basis_source",
                                "BROKER_FILL",
                            )
                            or "BROKER_FILL"
                        ),

                    "current_value":
                        current_value,

                    "pnl_pct":
                        pnl_pct,

                    "pnl_dollars":
                        pnl_dollars,

                    "unrealized_pnl_pct":
                        unrealized_pnl_pct,

                    "unrealized_pnl_dollars":
                        unrealized_pnl_dollars,

                    "realized_pnl_dollars":
                        realized_pnl_dollars,

                    "total_pnl_dollars":
                        total_pnl_dollars,

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
                "entry_basis_source",
                "current_value",
                "unrealized_pnl_pct",
                "unrealized_pnl_dollars",
                "realized_pnl_dollars",
                "total_pnl_dollars",
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
            "unrealized_pnl_pct"
        ] = (
            pd.to_numeric(
                display[
                    "unrealized_pnl_pct"
                ],
                errors="coerce",
            )
            * 100
        ).round(1)

        for column in [
            "unrealized_pnl_dollars",
            "realized_pnl_dollars",
            "total_pnl_dollars",
        ]:
            display[column] = (
                pd.to_numeric(
                    display[column],
                    errors="coerce",
                )
                .round(2)
            )

        display = display.rename(
            columns={
                "entry_debit":
                    "actual_entry/share",
                "entry_basis_source":
                    "entry_source",
                "current_value":
                    "exit_value/share",
                "unrealized_pnl_pct":
                    "unrealized_%",
                "unrealized_pnl_dollars":
                    "unrealized_$",
                "realized_pnl_dollars":
                    "realized_$",
                "total_pnl_dollars":
                    "total_pnl_$",
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

        self._paper_exit_orders_submitted_this_run = 0

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

        for _, row in actionable.iterrows():

            position_id = str(
                row[
                    "position_id"
                ]
            )

            position = self._tracked_alert_positions.get(
                position_id
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

            today_close_client_id = (
                self._paper_exit_execution_client_order_id(
                    position_id,
                    today,
                )
                if (
                    action == "CLOSE"
                    and self.paper_exit_execution_armed
                )
                else ""
            )

            already_has_today_close_order = (
                bool(today_close_client_id)
                and str(
                    position.get(
                        "broker_close_client_order_id",
                        "",
                    )
                    or ""
                )
                == today_close_client_id
            )

            should_execute_close = (
                action == "CLOSE"
                and self.paper_exit_execution_armed
                and not already_has_today_close_order
            )

            emit_alert = not already_alerted

            if (
                not emit_alert
                and not should_execute_close
            ):
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
                "timestamp": self.get_datetime().isoformat(),
                "lifecycle_id": position_id,
                "lifecycle_status_before": self._normalize_lifecycle_status(
                    position.get(
                        "status",
                        "ALERTED",
                    )
                ),
                "action": action,
                "underlying": str(
                    row[
                        "underlying"
                    ]
                ),
                "direction": str(
                    row[
                        "direction"
                    ]
                ),
                "decision": str(
                    row[
                        "decision"
                    ]
                ),
                "quantity": int(
                    row[
                        "quantity"
                    ]
                ),
                "legs": legs,
                "actual_entry_debit_per_share": float(
                    row[
                        "entry_debit"
                    ]
                ),
                "entry_basis_source": str(
                    row.get(
                        "entry_basis_source",
                        "BROKER_FILL",
                    )
                    or "BROKER_FILL"
                ),
                "estimated_exit_value_per_share": float(
                    row[
                        "current_value"
                    ]
                ),
                "unrealized_pnl_pct": float(
                    row[
                        "unrealized_pnl_pct"
                    ]
                ),
                "unrealized_pnl_dollars": float(
                    row[
                        "unrealized_pnl_dollars"
                    ]
                ),
                "realized_pnl_dollars": float(
                    row[
                        "realized_pnl_dollars"
                    ]
                ),
                "total_pnl_dollars": float(
                    row[
                        "total_pnl_dollars"
                    ]
                ),
                # Backward-compatible aliases now based on actual fill.
                "estimated_pnl_pct": float(
                    row[
                        "unrealized_pnl_pct"
                    ]
                ),
                "estimated_pnl_dollars": float(
                    row[
                        "total_pnl_dollars"
                    ]
                ),
                "dte": int(
                    row[
                        "dte"
                    ]
                ),
                "days_held": int(
                    row[
                        "days_held"
                    ]
                ),
                "thesis_state": str(
                    row[
                        "thesis_state"
                    ]
                ),
                "reason": str(
                    row[
                        "reason"
                    ]
                ),
                "mode": (
                    "ALPACA_PAPER_EXIT_EXECUTION_ARMED"
                    if (
                        action == "CLOSE"
                        and self.paper_exit_execution_armed
                    )
                    else "ALERT_ONLY_NO_ORDER"
                ),
            }

            if emit_alert:
                self.log_message(
                    "\n\n"
                    "========== EXIT ALERT ==========\n"
                    f"{payload['action']} | "
                    f"{payload['underlying']} | "
                    f"{payload['decision']}\n"
                    f"Lifecycle: {payload['lifecycle_id']} "
                    f"[{payload['lifecycle_status_before']}]\n"
                    f"Contracts: {payload['quantity']}\n"
                    f"Legs: {payload['legs']}\n"
                    f"Actual entry/share: "
                    f"${payload['actual_entry_debit_per_share']:.2f} "
                    f"[{payload['entry_basis_source']}]\n"
                    f"Executable exit value/share: "
                    f"${payload['estimated_exit_value_per_share']:.2f}\n"
                    f"Unrealized P/L: "
                    f"{payload['unrealized_pnl_pct'] * 100:.1f}% "
                    f"(${payload['unrealized_pnl_dollars']:,.2f})\n"
                    f"Realized P/L: "
                    f"${payload['realized_pnl_dollars']:,.2f}\n"
                    f"Total P/L: "
                    f"${payload['total_pnl_dollars']:,.2f}\n"
                    f"DTE: {payload['dte']}\n"
                    f"Held: {payload['days_held']} day(s)\n"
                    f"Thesis: {payload['thesis_state']}\n"
                    f"Reason: {payload['reason']}\n"
                    + (
                        "MODE: ALPACA PAPER EXIT EXECUTION ARMED\n"
                        if (
                            action == "CLOSE"
                            and self.paper_exit_execution_armed
                        )
                        else "MODE: ALERT ONLY - NO ORDER SUBMITTED\n"
                    )
                    + "================================"
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
                position[
                    "journal_exit_signal_reason"
                ] = payload[
                    "reason"
                ]
                position[
                    "journal_exit_signal_at"
                ] = self.get_datetime().isoformat()

                current_status = self._normalize_lifecycle_status(
                    position.get(
                        "status",
                        "OPEN",
                    )
                )

                if current_status not in {
                    "CLOSE_WORKING",
                    "PARTIALLY_CLOSED",
                }:
                    self._transition_trade_lifecycle(
                        position,
                        "CLOSE_ALERTED",
                        "Exit-management close signal emitted from actual-fill P/L; "
                        "awaiting PAPER close submission/reconciliation",
                        details={
                            "unrealized_pnl_pct": payload[
                                "unrealized_pnl_pct"
                            ],
                            "realized_pnl_dollars": payload[
                                "realized_pnl_dollars"
                            ],
                            "reason": payload[
                                "reason"
                            ],
                        },
                    )
            elif emit_alert:
                self._record_lifecycle_event(
                    position,
                    "ADJUST_ALERT_EMITTED",
                    payload[
                        "reason"
                    ],
                    details={
                        "unrealized_pnl_pct": payload[
                            "unrealized_pnl_pct"
                        ],
                    },
                )

            self._tracked_alert_positions[
                position_id
            ] = position
            self._save_trade_alert_positions_state()

            execution_result = None

            if should_execute_close:
                execution_result = self._execute_paper_option_exit(
                    row,
                    position_id,
                )
                payload[
                    "paper_exit_execution"
                ] = execution_result
                self._log_paper_exit_execution_result(
                    payload,
                    execution_result,
                )

            if emit_alert or execution_result is not None:
                self._append_exit_alert_jsonl(
                    payload
                )
                alerts.append(
                    payload
                )

        if alerts:
            self.log_message(
                f"Generated {len(alerts)} close/adjust action record(s)."
            )

        return alerts

    # ======================================================
    # TRADE JOURNAL + ANALYTICS
    # ======================================================

    @staticmethod
    def _trade_journal_score_bucket(score):
        try:
            value = float(score)
        except (TypeError, ValueError):
            return "UNKNOWN"
        if not math.isfinite(value):
            return "UNKNOWN"
        if value >= 85:
            return "85+"
        if value >= 80:
            return "80-84.99"
        if value >= 75:
            return "75-79.99"
        if value >= 70:
            return "70-74.99"
        return "<70"

    def _trade_journal_datetime(self, value):
        return self._parse_lifecycle_datetime(value)

    def _trade_journal_close_timestamp(self, position):
        latest = None
        ledger = position.get("broker_close_fill_ledger", {}) or {}
        for fill in ledger.values():
            dt = self._trade_journal_datetime(fill.get("filled_at", ""))
            if dt is not None and (latest is None or dt > latest):
                latest = dt
        if latest is not None:
            return latest
        if self._normalize_lifecycle_status(position.get("status", "")) == "CLOSED":
            return self._trade_journal_datetime(position.get("status_updated_at", ""))
        return None

    def _update_trade_journal_marks(self, exit_results):
        """Persist last mark plus MAE/MFE without changing trade decisions."""
        if not bool(self.parameters.get("trade_journal_enabled", True)):
            return
        if exit_results is None or exit_results.empty:
            return

        changed = False
        now_text = self.get_datetime().isoformat()
        for _, row in exit_results.iterrows():
            lifecycle_id = str(row.get("position_id", "") or "")
            position = self._tracked_alert_positions.get(lifecycle_id)
            if not isinstance(position, dict):
                continue

            pnl_pct = self._lifecycle_float(row.get("unrealized_pnl_pct", None), None)
            pnl_dollars = self._lifecycle_float(row.get("unrealized_pnl_dollars", None), None)
            total_pnl = self._lifecycle_float(row.get("total_pnl_dollars", None), None)
            current_value = self._lifecycle_float(row.get("current_value", None), None)
            if pnl_pct is None or not math.isfinite(float(pnl_pct)):
                continue

            position["journal_last_mark_at"] = now_text
            position["journal_last_exit_value_per_share"] = current_value
            position["journal_last_unrealized_pnl_pct"] = float(pnl_pct)
            position["journal_last_unrealized_pnl_dollars"] = pnl_dollars
            position["journal_last_total_pnl_dollars"] = total_pnl

            old_mae_pct = self._lifecycle_float(position.get("journal_mae_unrealized_pct", None), None)
            old_mfe_pct = self._lifecycle_float(position.get("journal_mfe_unrealized_pct", None), None)
            old_mae_dollars = self._lifecycle_float(position.get("journal_mae_unrealized_dollars", None), None)
            old_mfe_dollars = self._lifecycle_float(position.get("journal_mfe_unrealized_dollars", None), None)

            position["journal_mae_unrealized_pct"] = float(pnl_pct) if old_mae_pct is None else min(float(old_mae_pct), float(pnl_pct))
            position["journal_mfe_unrealized_pct"] = float(pnl_pct) if old_mfe_pct is None else max(float(old_mfe_pct), float(pnl_pct))

            if pnl_dollars is not None and math.isfinite(float(pnl_dollars)):
                position["journal_mae_unrealized_dollars"] = float(pnl_dollars) if old_mae_dollars is None else min(float(old_mae_dollars), float(pnl_dollars))
                position["journal_mfe_unrealized_dollars"] = float(pnl_dollars) if old_mfe_dollars is None else max(float(old_mfe_dollars), float(pnl_dollars))

            self._tracked_alert_positions[lifecycle_id] = position
            changed = True

        if changed:
            self._save_trade_alert_positions_state()

    def _trade_journal_rows(self):
        rows = []
        for lifecycle_id, position in self._tracked_alert_positions.items():
            if str(position.get("asset_type", "OPTION") or "OPTION").upper() != "OPTION":
                continue
            status = self._normalize_lifecycle_status(position.get("status", "ALERTED"))
            if status == "SUPERSEDED":
                continue

            entry_qty = self._lifecycle_float(position.get("actual_entry_filled_qty", 0.0), 0.0) or 0.0
            peak_qty = self._lifecycle_float(position.get("broker_peak_open_quantity", 0.0), 0.0) or 0.0
            if max(entry_qty, peak_qty) <= 0:
                # Alert-only / rejected-without-fill records are lifecycle audit
                # records, not executed trades, and are excluded from P/L stats.
                continue

            entry_basis = self._lifecycle_float(position.get("actual_entry_debit_per_share", None), None)
            close_qty = self._lifecycle_float(position.get("actual_close_filled_qty", 0.0), 0.0) or 0.0
            close_credit = self._lifecycle_float(position.get("actual_close_avg_credit_per_share", None), None)
            realized = self._lifecycle_float(position.get("actual_realized_pnl_dollars", 0.0), 0.0) or 0.0
            open_qty = self._lifecycle_float(position.get("broker_open_quantity", 0.0), 0.0) or 0.0

            entry_dt = self._trade_journal_datetime(position.get("actual_entry_filled_at", ""))
            if entry_dt is None:
                entry_dt = self._trade_journal_datetime(position.get("entry_timestamp", ""))
            close_dt = self._trade_journal_close_timestamp(position)

            holding_hours = None
            if entry_dt is not None and close_dt is not None:
                holding_hours = max(0.0, (close_dt - entry_dt).total_seconds() / 3600.0)

            realized_return_pct = None
            if entry_basis is not None and entry_basis > 0 and close_qty > 0:
                realized_cost = float(entry_basis) * 100.0 * float(close_qty)
                if realized_cost > 0:
                    realized_return_pct = float(realized) / realized_cost

            completed_return_pct = realized_return_pct if status == "CLOSED" else None
            max_risk = self._lifecycle_float(position.get("entry_max_risk", None), None)
            score = self._lifecycle_float(position.get("entry_structure_score", None), None)

            rows.append({
                "lifecycle_id": str(lifecycle_id),
                "status": status,
                "underlying": str(position.get("underlying", "") or ""),
                "direction": str(position.get("direction", "") or ""),
                "decision": str(position.get("decision", "") or ""),
                "entry_date": str(position.get("entry_date", "") or ""),
                "entry_timestamp": str(position.get("entry_timestamp", "") or ""),
                "actual_entry_filled_at": str(position.get("actual_entry_filled_at", "") or ""),
                "close_timestamp": close_dt.isoformat() if close_dt is not None else "",
                "expiration": str(position.get("expiration", "") or ""),
                "long_contract": str(position.get("long_contract", "") or ""),
                "short_contract": str(position.get("short_contract", "") or ""),
                "planned_quantity": self._lifecycle_float(position.get("quantity", 0.0), 0.0) or 0.0,
                "actual_entry_qty": float(entry_qty),
                "open_qty": float(open_qty),
                "actual_close_qty": float(close_qty),
                "actual_entry_debit_per_share": entry_basis,
                "actual_close_credit_per_share": close_credit,
                "actual_entry_total_debit": self._lifecycle_float(position.get("actual_entry_total_debit", None), None),
                "realized_pnl_dollars": float(realized),
                "realized_return_pct": realized_return_pct,
                "completed_return_pct": completed_return_pct,
                "last_unrealized_pnl_pct": self._lifecycle_float(position.get("journal_last_unrealized_pnl_pct", None), None),
                "last_unrealized_pnl_dollars": self._lifecycle_float(position.get("journal_last_unrealized_pnl_dollars", None), None),
                "last_total_pnl_dollars": self._lifecycle_float(position.get("journal_last_total_pnl_dollars", None), None),
                "mae_unrealized_pct": self._lifecycle_float(position.get("journal_mae_unrealized_pct", None), None),
                "mfe_unrealized_pct": self._lifecycle_float(position.get("journal_mfe_unrealized_pct", None), None),
                "mae_unrealized_dollars": self._lifecycle_float(position.get("journal_mae_unrealized_dollars", None), None),
                "mfe_unrealized_dollars": self._lifecycle_float(position.get("journal_mfe_unrealized_dollars", None), None),
                "last_mark_at": str(position.get("journal_last_mark_at", "") or ""),
                "holding_hours": holding_hours,
                "holding_days": (holding_hours / 24.0) if holding_hours is not None else None,
                "entry_max_risk": max_risk,
                "entry_max_reward": position.get("entry_max_reward", None),
                "entry_breakeven": self._lifecycle_float(position.get("entry_breakeven", None), None),
                "entry_structure_score": score,
                "score_bucket": self._trade_journal_score_bucket(score),
                "entry_stock_score": self._lifecycle_float(position.get("entry_stock_score", None), None),
                "entry_option_score": self._lifecycle_float(position.get("entry_option_score", None), None),
                "entry_iv": self._lifecycle_float(position.get("entry_iv", None), None),
                "entry_iv_percentile": self._lifecycle_float(position.get("entry_iv_percentile", None), None),
                "entry_iv_rank": self._lifecycle_float(position.get("entry_iv_rank", None), None),
                "entry_iv_history_samples": self._lifecycle_float(position.get("entry_iv_history_samples", None), None),
                "entry_event_risk": str(position.get("entry_event_risk", "UNKNOWN") or "UNKNOWN"),
                "entry_reward_risk": self._lifecycle_float(position.get("entry_reward_risk", None), None),
                "entry_selection_reason": str(position.get("entry_selection_reason", "") or ""),
                "entry_basis_source": str(position.get("actual_entry_basis_source", "") or ""),
                "exit_reason": str(position.get("journal_exit_signal_reason", "") or ""),
                "exit_signal_at": str(position.get("journal_exit_signal_at", "") or ""),
                "broker_entry_order_id": str(position.get("broker_entry_order_id", "") or ""),
                "broker_entry_client_order_id": str(position.get("broker_entry_client_order_id", "") or ""),
                "broker_close_order_id": str(position.get("broker_close_order_id", "") or ""),
                "broker_close_client_order_id": str(position.get("broker_close_client_order_id", "") or ""),
            })

        rows.sort(key=lambda row: (row.get("entry_timestamp", ""), row.get("lifecycle_id", "")))
        return rows

    @staticmethod
    def _trade_journal_json_number(value):
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    def _trade_analytics_group(self, completed, column):
        output = {}
        if completed.empty or column not in completed.columns:
            return output
        for key, group in completed.groupby(column, dropna=False):
            label = "UNKNOWN" if pd.isna(key) or str(key).strip() == "" else str(key)
            pnl = pd.to_numeric(group["realized_pnl_dollars"], errors="coerce").fillna(0.0)
            returns = pd.to_numeric(group["completed_return_pct"], errors="coerce")
            wins = int((pnl > 0).sum())
            losses = int((pnl < 0).sum())
            output[label] = {
                "trades": int(len(group)),
                "wins": wins,
                "losses": losses,
                "win_rate": self._trade_journal_json_number(wins / len(group) if len(group) else None),
                "realized_pnl_dollars": self._trade_journal_json_number(pnl.sum()),
                "expectancy_dollars": self._trade_journal_json_number(pnl.mean() if len(group) else None),
                "average_return_pct": self._trade_journal_json_number(returns.mean()),
            }
        return output

    def _build_trade_analytics(self, journal_df):
        if journal_df is None or journal_df.empty:
            return {
                "generated_at": self.get_datetime().isoformat(),
                "executed_trades": 0,
                "open_trades": 0,
                "completed_trades": 0,
                "realized_pnl_dollars": 0.0,
                "message": "No broker-filled option lifecycles are available yet.",
            }

        completed = journal_df[journal_df["status"] == "CLOSED"].copy()
        open_mask = journal_df["status"].isin({"PARTIALLY_OPEN", "OPEN", "CLOSE_ALERTED", "CLOSE_WORKING", "PARTIALLY_CLOSED", "ORPHANED"})
        open_df = journal_df[open_mask].copy()

        pnl = pd.to_numeric(completed.get("realized_pnl_dollars", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        returns = pd.to_numeric(completed.get("completed_return_pct", pd.Series(dtype=float)), errors="coerce")
        hold_days = pd.to_numeric(completed.get("holding_days", pd.Series(dtype=float)), errors="coerce")
        wins = int((pnl > 0).sum())
        losses = int((pnl < 0).sum())
        breakeven = int((pnl == 0).sum())
        gross_profit = float(pnl[pnl > 0].sum()) if len(pnl) else 0.0
        gross_loss = float(pnl[pnl < 0].sum()) if len(pnl) else 0.0
        profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else None

        max_drawdown = 0.0
        if not completed.empty:
            ordered = completed.copy()
            ordered["_close_sort"] = pd.to_datetime(ordered["close_timestamp"], errors="coerce", utc=True)
            ordered = ordered.sort_values(["_close_sort", "entry_timestamp"], na_position="last")
            curve = pd.to_numeric(ordered["realized_pnl_dollars"], errors="coerce").fillna(0.0).cumsum()
            peak = curve.cummax().clip(lower=0.0)
            drawdown = curve - peak
            if len(drawdown):
                max_drawdown = float(abs(min(0.0, float(drawdown.min()))))

        open_unrealized = pd.to_numeric(open_df.get("last_unrealized_pnl_dollars", pd.Series(dtype=float)), errors="coerce").fillna(0.0)

        analytics = {
            "generated_at": self.get_datetime().isoformat(),
            "executed_trades": int(len(journal_df)),
            "open_trades": int(len(open_df)),
            "completed_trades": int(len(completed)),
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate": self._trade_journal_json_number(wins / len(completed) if len(completed) else None),
            "realized_pnl_dollars": self._trade_journal_json_number(pnl.sum() if len(pnl) else 0.0),
            "open_last_mark_unrealized_pnl_dollars": self._trade_journal_json_number(open_unrealized.sum() if len(open_unrealized) else 0.0),
            "expectancy_dollars_per_completed_trade": self._trade_journal_json_number(pnl.mean() if len(completed) else None),
            "average_completed_return_pct": self._trade_journal_json_number(returns.mean()),
            "median_completed_return_pct": self._trade_journal_json_number(returns.median()),
            "average_holding_days": self._trade_journal_json_number(hold_days.mean()),
            "gross_profit_dollars": self._trade_journal_json_number(gross_profit),
            "gross_loss_dollars": self._trade_journal_json_number(gross_loss),
            "profit_factor": self._trade_journal_json_number(profit_factor),
            "max_closed_trade_equity_drawdown_dollars": self._trade_journal_json_number(max_drawdown),
            "by_decision": self._trade_analytics_group(completed, "decision"),
            "by_direction": self._trade_analytics_group(completed, "direction"),
            "by_score_bucket": self._trade_analytics_group(completed, "score_bucket"),
            "by_event_risk": self._trade_analytics_group(completed, "entry_event_risk"),
            "by_exit_reason": self._trade_analytics_group(completed, "exit_reason"),
        }
        return analytics

    @staticmethod
    def _atomic_write_dataframe_csv(df, path):
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        temp = path + ".tmp"
        df.to_csv(temp, index=False)
        os.replace(temp, path)

    @staticmethod
    def _atomic_write_json(payload, path):
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        temp = path + ".tmp"
        with open(temp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)

    def refresh_trade_journal_analytics(self):
        """Regenerate journal + analytics atomically from lifecycle truth."""
        if not bool(self.parameters.get("trade_journal_enabled", True)):
            return None

        rows = self._trade_journal_rows()
        journal_df = pd.DataFrame(rows)
        analytics = self._build_trade_analytics(journal_df)

        try:
            if self.trade_journal_csv_path:
                self._atomic_write_dataframe_csv(journal_df, self.trade_journal_csv_path)
            if self.trade_analytics_json_path:
                self._atomic_write_json(analytics, self.trade_analytics_json_path)
        except Exception as exc:
            self.log_message(
                "Trade journal/analytics persistence failed; trading state is "
                f"unchanged. Reason={exc}"
            )
            return analytics

        signature = (
            int(analytics.get("executed_trades", 0) or 0),
            int(analytics.get("open_trades", 0) or 0),
            int(analytics.get("completed_trades", 0) or 0),
            round(float(analytics.get("realized_pnl_dollars", 0.0) or 0.0), 2),
        )
        if (
            bool(self.parameters.get("trade_journal_log_summary", True))
            and signature != self._last_trade_analytics_log_signature
        ):
            self._last_trade_analytics_log_signature = signature
            win_rate = analytics.get("win_rate")
            win_rate_text = "N/A" if win_rate is None else f"{float(win_rate) * 100:.1f}%"
            profit_factor = analytics.get("profit_factor")
            pf_text = "N/A" if profit_factor is None else f"{float(profit_factor):.2f}"
            self.log_message(
                "TRADE JOURNAL ANALYTICS: "
                f"executed={analytics.get('executed_trades', 0)}, "
                f"open={analytics.get('open_trades', 0)}, "
                f"completed={analytics.get('completed_trades', 0)}, "
                f"realized P/L=${float(analytics.get('realized_pnl_dollars', 0.0) or 0.0):,.2f}, "
                f"win rate={win_rate_text}, profit factor={pf_text}."
            )

        return analytics

    # ======================================================
    # CONTROLLED PAPER-ONLY EXIT VALIDATION
    # ======================================================

    def _apply_controlled_paper_exit_test(self, exit_results):
        """Inject one restart-safe PAPER-only CLOSE for exact symbol.

        The production exit request builder, idempotency key, broker linking,
        lifecycle transitions, and fill accounting remain unchanged. This
        helper only changes the selected row's management action to CLOSE.
        """

        if not self.paper_exit_test_armed:
            return exit_results

        if not self.alpaca_is_paper or not self.paper_exit_execution_armed:
            raise RuntimeError(
                "Controlled PAPER exit test reached runtime without PAPER-only "
                "exit execution safeguards"
            )

        if exit_results is None or exit_results.empty:
            self.log_message(
                "CONTROLLED PAPER EXIT TEST DEFERRED: exit-management rows are "
                "not available; no close order will be submitted."
            )
            return exit_results

        symbol = self.paper_exit_test_symbol
        token = self.paper_exit_test_token
        token_hash = hashlib.sha256(
            token.encode("utf-8")
        ).hexdigest()

        active_statuses = {
            "PARTIALLY_OPEN",
            "OPEN",
        }

        candidates = []
        for lifecycle_id, position in self._tracked_alert_positions.items():
            if str(position.get("asset_type", "OPTION") or "OPTION").upper() != "OPTION":
                continue
            if str(position.get("underlying", "") or "").upper() != symbol:
                continue
            status = self._normalize_lifecycle_status(
                position.get("status", "ALERTED")
            )
            if status not in active_statuses:
                continue
            broker_open_qty = self._lifecycle_float(
                position.get("broker_open_quantity", 0.0),
                0.0,
            ) or 0.0
            if broker_open_qty <= 0:
                continue
            candidates.append((lifecycle_id, position))

        if len(candidates) != 1:
            self.log_message(
                "CONTROLLED PAPER EXIT TEST FAIL-CLOSED: exact underlying "
                f"{symbol} has {len(candidates)} broker-confirmed active "
                "lifecycle(s); exactly one is required."
            )
            return exit_results

        lifecycle_id, position = candidates[0]

        if str(position.get("paper_exit_test_consumed_token_sha256", "") or "") == token_hash:
            if not bool(position.get("paper_exit_test_consumed_log_emitted", False)):
                self.log_message(
                    "CONTROLLED PAPER EXIT TEST ALREADY CONSUMED: "
                    f"{symbol} lifecycle={lifecycle_id}; this token cannot "
                    "force another close. Use a new token for a new test."
                )
                position["paper_exit_test_consumed_log_emitted"] = True
                self._tracked_alert_positions[lifecycle_id] = position
                self._save_trade_alert_positions_state()
            return exit_results

        row_mask = (
            exit_results["position_id"].astype(str) == str(lifecycle_id)
        )
        matching_rows = exit_results.loc[row_mask]

        if len(matching_rows) != 1:
            self.log_message(
                "CONTROLLED PAPER EXIT TEST DEFERRED: exact lifecycle does not "
                "have one usable exit-management row; no close order will be "
                "submitted."
            )
            return exit_results

        row_index = matching_rows.index[0]

        entry_basis = self._lifecycle_float(
            exit_results.at[row_index, "entry_debit"],
            None,
        )
        exit_value = self._lifecycle_float(
            exit_results.at[row_index, "current_value"],
            None,
        )
        if (
            entry_basis is None
            or not math.isfinite(float(entry_basis))
            or entry_basis <= 0
            or exit_value is None
            or not math.isfinite(float(exit_value))
            or exit_value < 0
        ):
            self.log_message(
                "CONTROLLED PAPER EXIT TEST DEFERRED: broker fill basis or "
                "executable exit value is unavailable; no close order will be "
                "submitted."
            )
            return exit_results

        now = self.get_datetime()

        # Consume before calling submit_order. A crash in the tiny gap between
        # persistence and submission fails safe by NOT repeating a forced close.
        position["paper_exit_test_consumed_token_sha256"] = token_hash
        position["paper_exit_test_consumed_at"] = now.isoformat()
        position["paper_exit_test_symbol"] = symbol
        position["paper_exit_test_consumed_log_emitted"] = False
        self._record_lifecycle_event(
            position,
            "CONTROLLED_PAPER_EXIT_TEST_TRIGGERED",
            "Explicit PAPER-only validation token forced this lifecycle into "
            "the normal CLOSE pipeline",
            details={
                "symbol": symbol,
                "token_sha256": token_hash[:10],
                "broker_open_quantity": self._lifecycle_float(
                    position.get("broker_open_quantity", 0.0),
                    0.0,
                ) or 0.0,
                "entry_basis": float(entry_basis),
                "executable_exit_value": float(exit_value),
            },
        )
        self._tracked_alert_positions[lifecycle_id] = position
        self._save_trade_alert_positions_state()

        exit_results = exit_results.copy()
        original_action = str(exit_results.at[row_index, "action"] or "HOLD")
        original_reason = str(exit_results.at[row_index, "reason"] or "")
        exit_results.at[row_index, "action"] = "CLOSE"
        exit_results.at[row_index, "reason"] = (
            "CONTROLLED PAPER EXIT TEST: explicit one-shot PAPER-only close "
            f"for {symbol}; production exit rule before override was "
            f"{original_action}. "
            + original_reason
        ).strip()

        self.log_message(
            "CONTROLLED PAPER EXIT TEST TRIGGERED: "
            f"{symbol} lifecycle={lifecycle_id}; forcing exactly the current "
            "broker-confirmed open quantity through the normal PAPER close "
            "order path. Token is now consumed."
        )

        return exit_results


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

        exit_results = self._apply_controlled_paper_exit_test(
            exit_results
        )

        self._update_trade_journal_marks(
            exit_results
        )

        self.log_exit_management(
            exit_results
        )

        exit_alerts = (
            self.generate_exit_alerts(
                exit_results
            )
        )

        self.refresh_trade_journal_analytics()

        return (
            exit_results,
            exit_alerts,
        )


    # ======================================================
    # OPTIONAL ALPACA PAPER ENTRY EXECUTION
    # ======================================================

    @staticmethod
    def _paper_execution_client_order_id(
        lifecycle_id,
    ):
        """Return a deterministic, compact idempotency key."""

        digest = hashlib.sha256(
            str(lifecycle_id).encode(
                "utf-8"
            )
        ).hexdigest()[:24]

        return (
            "lumi-pe-"
            + digest
        )


    @staticmethod
    def _paper_execution_order_not_found(
        exc,
    ):
        status_code = getattr(
            exc,
            "status_code",
            None,
        )

        if status_code == 404:
            return True

        text = str(exc).lower()

        return (
            "404" in text
            or "not found" in text
            or "order does not exist" in text
        )


    def _build_paper_option_entry_order_request(
        self,
        row,
        client_order_id,
    ):
        """Build one PAPER-only DAY limit option entry request."""

        decision = str(
            row.get(
                "decision",
                "",
            )
            or ""
        ).upper()

        quantity = int(
            row.get(
                "quantity",
                0,
            )
            or 0
        )

        if quantity <= 0:
            raise ValueError(
                "Paper execution requires positive option quantity"
            )

        round_decimals = max(
            0,
            int(
                self.parameters.get(
                    "paper_execution_limit_price_round_decimals",
                    2,
                )
            ),
        )

        limit_price = round(
            float(
                row.get(
                    "net_debit",
                    0.0,
                )
                or 0.0
            ),
            round_decimals,
        )

        if limit_price <= 0:
            raise ValueError(
                "Paper execution requires a positive debit limit price"
            )

        long_contract = str(
            row.get(
                "long_contract",
                "",
            )
            or ""
        ).upper()

        short_contract = str(
            row.get(
                "short_contract",
                "",
            )
            or ""
        ).upper()

        if not long_contract:
            raise ValueError(
                "Paper execution is missing the long option contract"
            )

        if decision in {
            "LONG CALL",
            "LONG PUT",
        }:
            request = LimitOrderRequest(
                symbol=long_contract,
                qty=quantity,
                side=OrderSide.BUY,
                position_intent=(
                    PositionIntent.BUY_TO_OPEN
                ),
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                client_order_id=client_order_id,
            )

            return (
                request,
                limit_price,
                "SIMPLE_OPTION_LIMIT",
            )

        if decision in {
            "BULL CALL SPREAD",
            "BEAR PUT SPREAD",
        }:
            if not short_contract:
                raise ValueError(
                    "Multi-leg paper execution is missing the short leg"
                )

            request = LimitOrderRequest(
                qty=quantity,
                order_class=OrderClass.MLEG,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                client_order_id=client_order_id,
                legs=[
                    OptionLegRequest(
                        symbol=long_contract,
                        ratio_qty=1,
                        side=OrderSide.BUY,
                        position_intent=(
                            PositionIntent.BUY_TO_OPEN
                        ),
                    ),
                    OptionLegRequest(
                        symbol=short_contract,
                        ratio_qty=1,
                        side=OrderSide.SELL,
                        position_intent=(
                            PositionIntent.SELL_TO_OPEN
                        ),
                    ),
                ],
            )

            return (
                request,
                limit_price,
                "MLEG_DEBIT_LIMIT",
            )

        raise ValueError(
            "Paper execution does not support structure "
            f"{decision or 'UNKNOWN'}"
        )


    def _persist_paper_entry_order_link(
        self,
        position,
        order,
        client_order_id,
        limit_price,
        order_style,
        source,
    ):
        """Persist returned broker identifiers into the lifecycle."""

        now = self.get_datetime()

        broker_order_id = str(
            self._broker_field(
                order,
                "id",
                "",
            )
            or ""
        )

        returned_client_order_id = str(
            self._broker_field(
                order,
                "client_order_id",
                "",
            )
            or client_order_id
        )

        status = self._enum_text(
            self._broker_field(
                order,
                "status",
                "",
            )
        )

        position[
            "broker_entry_order_id"
        ] = broker_order_id
        position[
            "broker_entry_client_order_id"
        ] = returned_client_order_id
        position[
            "broker_entry_order_status"
        ] = status
        position[
            "broker_entry_order_style"
        ] = order_style
        position[
            "broker_entry_limit_price"
        ] = float(limit_price)
        position[
            "broker_entry_link_source"
        ] = source
        position[
            "broker_entry_linked_at"
        ] = now.isoformat()
        position[
            "paper_execution_enabled_at_entry"
        ] = True

        submitted_at = self._broker_field(
            order,
            "submitted_at",
            None,
        )

        if submitted_at is not None:
            position[
                "broker_entry_submitted_at"
            ] = str(submitted_at)

        self._record_lifecycle_event(
            position,
            (
                "PAPER_ENTRY_ORDER_RECOVERED"
                if source == "EXISTING_CLIENT_ORDER_ID"
                else "PAPER_ENTRY_ORDER_SUBMITTED"
            ),
            (
                "Linked deterministic PAPER entry order "
                f"{broker_order_id or returned_client_order_id}"
            ),
            details={
                "broker_order_id": broker_order_id,
                "client_order_id": returned_client_order_id,
                "broker_status": status,
                "limit_price": float(limit_price),
                "order_style": order_style,
                "link_source": source,
            },
        )

        filled_qty = self._lifecycle_float(
            self._broker_field(
                order,
                "filled_qty",
                0.0,
            ),
            0.0,
        ) or 0.0

        requested_qty = self._lifecycle_float(
            self._broker_field(
                order,
                "qty",
                position.get(
                    "quantity",
                    0.0,
                ),
            ),
            self._lifecycle_float(
                position.get(
                    "quantity",
                    0.0,
                ),
                0.0,
            ) or 0.0,
        ) or 0.0

        position[
            "broker_entry_filled_qty"
        ] = filled_qty
        position[
            "broker_entry_requested_qty"
        ] = requested_qty
        position[
            "broker_entry_unfilled_qty"
        ] = max(
            0.0,
            requested_qty - filled_qty,
        )

        terminal_without_fill = {
            "canceled": "CANCELED",
            "rejected": "REJECTED",
            "expired": "EXPIRED",
            "done_for_day": "EXPIRED",
            "calculated": "EXPIRED",
        }

        tolerance = max(
            0.0,
            float(
                self.parameters.get(
                    "lifecycle_quantity_tolerance",
                    1e-6,
                )
            ),
        )

        if (
            status in terminal_without_fill
            and filled_qty <= tolerance
        ):
            self._transition_trade_lifecycle(
                position,
                terminal_without_fill[status],
                (
                    "PAPER entry order returned terminal status "
                    f"{status} without a fill"
                ),
                details={
                    "broker_order_id": broker_order_id,
                    "client_order_id": returned_client_order_id,
                    "filled_qty": filled_qty,
                },
            )
        else:
            if (
                status in terminal_without_fill
                and filled_qty > tolerance
            ):
                position[
                    "paper_entry_remainder_policy"
                ] = str(
                    self.parameters.get(
                        "paper_entry_partial_fill_policy",
                        "KEEP_PARTIAL_NO_TOP_UP",
                    )
                )
                position[
                    "paper_entry_top_up_allowed"
                ] = False

            # Position snapshots remain primary truth for OPEN/PARTIALLY_OPEN.
            # A terminal order with a partial fill stays non-terminal here
            # until reconciliation sees the smaller broker position.
            self._transition_trade_lifecycle(
                position,
                "ENTRY_WORKING",
                (
                    "PAPER entry order linked; awaiting broker "
                    "position reconciliation"
                    + (
                        "; terminal unfilled remainder will not be topped up"
                        if (
                            status in terminal_without_fill
                            and filled_qty > tolerance
                        )
                        else ""
                    )
                ),
                details={
                    "broker_order_id": broker_order_id,
                    "client_order_id": returned_client_order_id,
                    "broker_status": status,
                    "filled_qty": filled_qty,
                    "requested_qty": requested_qty,
                },
            )

        self._tracked_alert_positions[
            position[
                "id"
            ]
        ] = position

        self._save_trade_alert_positions_state()

        return {
            "submitted": (
                source == "SUBMIT_ORDER"
            ),
            "linked": True,
            "order_id": broker_order_id,
            "client_order_id": returned_client_order_id,
            "status": status,
            "limit_price": float(limit_price),
            "order_style": order_style,
            "source": source,
        }


    def _execute_paper_option_entry(
        self,
        row,
        lifecycle_id,
    ):
        """Idempotently submit/link one Alpaca PAPER option entry."""

        if not self.paper_execution_armed:
            return {
                "submitted": False,
                "linked": False,
                "status": "DISABLED",
                "reason": "PAPER execution is not armed",
            }

        if not self.alpaca_is_paper:
            # Defense in depth in addition to initialize() hard fail.
            raise RuntimeError(
                "Paper execution attempted with a non-paper Alpaca client"
            )

        entry_allowed, block_reasons = self._new_entry_execution_safety_gate()
        if not entry_allowed:
            return {
                "submitted": False,
                "linked": False,
                "status": "SAFETY_BLOCKED",
                "reason": "NEW entry blocked by production safety controls: "
                + ", ".join(block_reasons),
                "safety_block_reasons": block_reasons,
            }

        max_per_run = max(
            0,
            int(
                self.parameters.get(
                    "paper_execution_max_orders_per_run",
                    5,
                )
            ),
        )

        if (
            max_per_run > 0
            and self._paper_execution_orders_submitted_this_run
            >= max_per_run
        ):
            return {
                "submitted": False,
                "linked": False,
                "status": "RUN_CAP",
                "reason": "PAPER execution per-run order cap reached",
            }

        decision = str(
            row.get(
                "decision",
                "",
            )
            or ""
        ).upper()

        required_options_level = (
            3
            if decision in {
                "BULL CALL SPREAD",
                "BEAR PUT SPREAD",
            }
            else 2
        )

        try:
            known_options_level = int(
                self.options_trading_level
            )
        except (
            TypeError,
            ValueError,
        ):
            known_options_level = None

        if known_options_level is None:
            return {
                "submitted": False,
                "linked": False,
                "status": "OPTIONS_LEVEL_UNKNOWN",
                "reason": (
                    "PAPER execution requires a known Alpaca options "
                    "trading level and therefore fails closed"
                ),
            }

        if known_options_level < required_options_level:
            return {
                "submitted": False,
                "linked": False,
                "status": "OPTIONS_LEVEL_INSUFFICIENT",
                "reason": (
                    f"Structure requires Alpaca options level "
                    f"{required_options_level}; account reports "
                    f"level {known_options_level}"
                ),
            }

        position = self._tracked_alert_positions.get(
            lifecycle_id
        )

        if position is None:
            raise RuntimeError(
                "Lifecycle record must exist before PAPER execution"
            )

        if str(
            position.get(
                "asset_type",
                "OPTION",
            )
            or "OPTION"
        ).upper() != "OPTION":
            return {
                "submitted": False,
                "linked": False,
                "status": "UNSUPPORTED_ASSET",
                "reason": "Only option entries are enabled in this phase",
            }

        client_order_id = (
            self._paper_execution_client_order_id(
                lifecycle_id
            )
        )

        position[
            "broker_entry_client_order_id"
        ] = client_order_id
        position[
            "paper_execution_submission_planned_at"
        ] = self.get_datetime().isoformat()

        self._tracked_alert_positions[
            lifecycle_id
        ] = position
        self._save_trade_alert_positions_state()

        (
            order_request,
            limit_price,
            order_style,
        ) = self._build_paper_option_entry_order_request(
            row,
            client_order_id,
        )

        # Idempotency check. If a prior process submitted the order
        # but crashed before persisting the UUID, recover by the
        # deterministic client_order_id instead of submitting again.
        try:
            existing_order = (
                self.alpaca_trading_client
                .get_order_by_client_id(
                    client_order_id
                )
            )

        except Exception as exc:
            if self._paper_execution_order_not_found(
                exc
            ):
                existing_order = None
            else:
                position[
                    "paper_execution_last_error"
                ] = str(exc)
                position[
                    "paper_execution_last_error_at"
                ] = self.get_datetime().isoformat()
                self._save_trade_alert_positions_state()
                return {
                    "submitted": False,
                    "linked": False,
                    "status": "LOOKUP_FAILED",
                    "reason": (
                        "Could not verify deterministic client order ID; "
                        "submission fails closed"
                    ),
                    "error": str(exc),
                    "client_order_id": client_order_id,
                }

        if existing_order is not None:
            return self._persist_paper_entry_order_link(
                position,
                existing_order,
                client_order_id,
                limit_price,
                order_style,
                "EXISTING_CLIENT_ORDER_ID",
            )

        try:
            order = (
                self.alpaca_trading_client
                .submit_order(
                    order_data=order_request
                )
            )

        except Exception as exc:
            position[
                "paper_execution_last_error"
            ] = str(exc)
            position[
                "paper_execution_last_error_at"
            ] = self.get_datetime().isoformat()

            self._record_lifecycle_event(
                position,
                "PAPER_ENTRY_SUBMISSION_FAILED",
                "Alpaca PAPER entry submission failed",
                details={
                    "client_order_id": client_order_id,
                    "limit_price": float(limit_price),
                    "order_style": order_style,
                    "error": str(exc),
                },
            )

            self._tracked_alert_positions[
                lifecycle_id
            ] = position
            self._save_trade_alert_positions_state()

            return {
                "submitted": False,
                "linked": False,
                "status": "SUBMIT_FAILED",
                "reason": "Alpaca PAPER submit_order failed",
                "error": str(exc),
                "client_order_id": client_order_id,
                "limit_price": float(limit_price),
                "order_style": order_style,
            }

        self._paper_execution_orders_submitted_this_run += 1

        return self._persist_paper_entry_order_link(
            position,
            order,
            client_order_id,
            limit_price,
            order_style,
            "SUBMIT_ORDER",
        )


    def _log_paper_execution_result(
        self,
        payload,
        result,
    ):
        if not self.paper_execution_armed:
            return

        status = str(
            result.get(
                "status",
                "UNKNOWN",
            )
            or "UNKNOWN"
        )

        if result.get(
            "linked",
            False,
        ):
            self.log_message(
                "\n\n"
                "======= ALPACA PAPER ENTRY ORDER =======\n"
                f"{payload['direction']} | {payload['underlying']} | "
                f"{payload['decision']}\n"
                f"Lifecycle: {payload['lifecycle_id']}\n"
                f"Broker order ID: {result.get('order_id', '')}\n"
                f"Client order ID: {result.get('client_order_id', '')}\n"
                f"Order style: {result.get('order_style', '')}\n"
                f"Limit debit/share: ${result.get('limit_price', 0.0):.2f}\n"
                f"Broker status: {status.upper()}\n"
                f"Link source: {result.get('source', '')}\n"
                "ACCOUNT: ALPACA PAPER ONLY\n"
                "========================================"
            )
        else:
            self.log_message(
                "PAPER entry order was NOT submitted/linked for "
                f"{payload['underlying']}: {result.get('reason', status)}"
                + (
                    f". Error={result.get('error')}"
                    if result.get('error')
                    else ""
                )
            )

    @staticmethod
    def _paper_exit_execution_client_order_id(
        lifecycle_id,
        trade_date,
    ):
        """Daily deterministic idempotency key for one PAPER close attempt."""

        digest = hashlib.sha256(
            (
                str(lifecycle_id)
                + "|"
                + str(trade_date)
            ).encode(
                "utf-8"
            )
        ).hexdigest()[:24]

        return (
            "lumi-px-"
            + digest
        )


    def _build_paper_option_exit_order_request(
        self,
        row,
        client_order_id,
    ):
        """Build one PAPER-only DAY limit close request."""

        decision = str(
            row.get(
                "decision",
                "",
            )
            or ""
        ).upper()

        quantity = int(
            row.get(
                "quantity",
                0,
            )
            or 0
        )

        if quantity <= 0:
            raise ValueError(
                "Paper exit execution requires positive broker open quantity"
            )

        current_value = self._lifecycle_float(
            row.get(
                "current_value",
                0.0,
            ),
            0.0,
        ) or 0.0

        if current_value <= 0:
            raise ValueError(
                "Paper exit execution requires a positive executable close value"
            )

        round_decimals = max(
            0,
            int(
                self.parameters.get(
                    "paper_exit_execution_limit_price_round_decimals",
                    2,
                )
            ),
        )

        long_contract = str(
            row.get(
                "long_contract",
                "",
            )
            or ""
        ).upper()

        short_contract = str(
            row.get(
                "short_contract",
                "",
            )
            or ""
        ).upper()

        if not long_contract:
            raise ValueError(
                "Paper exit execution is missing the long option contract"
            )

        if decision in {
            "LONG CALL",
            "LONG PUT",
        }:
            limit_price = round(
                current_value,
                round_decimals,
            )

            if limit_price <= 0:
                raise ValueError(
                    "Rounded simple-option close limit is not positive"
                )

            request = LimitOrderRequest(
                symbol=long_contract,
                qty=quantity,
                side=OrderSide.SELL,
                position_intent=(
                    PositionIntent.SELL_TO_CLOSE
                ),
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                client_order_id=client_order_id,
            )

            return (
                request,
                limit_price,
                "SIMPLE_OPTION_CLOSE_LIMIT",
            )

        if decision in {
            "BULL CALL SPREAD",
            "BEAR PUT SPREAD",
        }:
            if not short_contract:
                raise ValueError(
                    "Multi-leg paper exit is missing the short leg"
                )

            # Alpaca MLEG convention: negative limit = credit received.
            limit_price = round(
                -current_value,
                round_decimals,
            )

            if limit_price >= 0:
                raise ValueError(
                    "MLEG close credit limit must be negative"
                )

            request = LimitOrderRequest(
                qty=quantity,
                order_class=OrderClass.MLEG,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                client_order_id=client_order_id,
                legs=[
                    OptionLegRequest(
                        symbol=long_contract,
                        ratio_qty=1,
                        side=OrderSide.SELL,
                        position_intent=(
                            PositionIntent.SELL_TO_CLOSE
                        ),
                    ),
                    OptionLegRequest(
                        symbol=short_contract,
                        ratio_qty=1,
                        side=OrderSide.BUY,
                        position_intent=(
                            PositionIntent.BUY_TO_CLOSE
                        ),
                    ),
                ],
            )

            return (
                request,
                limit_price,
                "MLEG_CREDIT_CLOSE_LIMIT",
            )

        raise ValueError(
            "Paper exit execution does not support structure "
            f"{decision or 'UNKNOWN'}"
        )


    def _persist_paper_exit_order_link(
        self,
        position,
        order,
        client_order_id,
        limit_price,
        order_style,
        source,
    ):
        """Persist a PAPER close order and move lifecycle to close-working."""

        now = self.get_datetime()

        broker_order_id = str(
            self._broker_field(
                order,
                "id",
                "",
            )
            or ""
        )

        returned_client_order_id = str(
            self._broker_field(
                order,
                "client_order_id",
                "",
            )
            or client_order_id
        )

        status = self._enum_text(
            self._broker_field(
                order,
                "status",
                "",
            )
        )

        filled_qty = self._lifecycle_float(
            self._broker_field(
                order,
                "filled_qty",
                0.0,
            ),
            0.0,
        ) or 0.0

        filled_avg_price = self._lifecycle_float(
            self._broker_field(
                order,
                "filled_avg_price",
                None,
            ),
            None,
        )

        position[
            "broker_close_order_id"
        ] = broker_order_id
        position[
            "broker_close_client_order_id"
        ] = returned_client_order_id
        position[
            "broker_close_order_status"
        ] = status
        position[
            "broker_close_filled_qty"
        ] = filled_qty
        position[
            "broker_close_filled_avg_price"
        ] = filled_avg_price
        position[
            "broker_close_order_style"
        ] = order_style
        position[
            "broker_close_limit_price"
        ] = float(limit_price)
        position[
            "broker_close_link_source"
        ] = source
        position[
            "broker_close_linked_at"
        ] = now.isoformat()
        position[
            "paper_exit_execution_enabled_at_close"
        ] = True

        terminal_statuses = {
            "canceled",
            "expired",
            "rejected",
            "done_for_day",
            "calculated",
            "suspended",
            "replaced",
        }

        requested_qty = self._lifecycle_float(
            self._broker_field(
                order,
                "qty",
                position.get(
                    "broker_open_quantity",
                    0.0,
                ),
            ),
            self._lifecycle_float(
                position.get(
                    "broker_open_quantity",
                    0.0,
                ),
                0.0,
            ) or 0.0,
        ) or 0.0

        position[
            "broker_close_requested_qty"
        ] = requested_qty
        position[
            "broker_close_unfilled_qty"
        ] = max(
            0.0,
            requested_qty - filled_qty,
        )

        retryable_terminal = status in {
            "canceled",
            "expired",
            "rejected",
            "done_for_day",
            "calculated",
        }

        if not (
            status in terminal_statuses
            and requested_qty - filled_qty > 1e-6
            and retryable_terminal
        ):
            position[
                "paper_exit_retry_eligible"
            ] = False
            position[
                "paper_exit_retry_after_date"
            ] = ""
            position[
                "paper_exit_retry_reason"
            ] = ""

        if (
            status in terminal_statuses
            and requested_qty - filled_qty > 1e-6
            and retryable_terminal
        ):
            retry_after = (
                now.date()
                + timedelta(
                    days=1
                )
            )
            position[
                "paper_exit_retry_policy"
            ] = str(
                self.parameters.get(
                    "paper_exit_terminal_retry_policy",
                    "NEXT_TRADING_DATE",
                )
            )
            position[
                "paper_exit_retry_after_date"
            ] = retry_after.isoformat()
            position[
                "paper_exit_retry_eligible"
            ] = False
            position[
                "paper_exit_retry_reason"
            ] = status.upper()

        if status in terminal_statuses and filled_qty <= 0:
            self._transition_trade_lifecycle(
                position,
                "CLOSE_ALERTED",
                "PAPER close order is terminal without a fill; "
                "position remains open and same-day retry/chasing is disabled",
                details={
                    "broker_order_id": broker_order_id,
                    "client_order_id": returned_client_order_id,
                    "status": status,
                    "retry_after_date": position.get(
                        "paper_exit_retry_after_date",
                        "",
                    ),
                },
            )
        else:
            self._transition_trade_lifecycle(
                position,
                "CLOSE_WORKING",
                "Alpaca PAPER close order is linked; awaiting broker "
                "position reconciliation before declaring the trade closed",
                details={
                    "broker_order_id": broker_order_id,
                    "client_order_id": returned_client_order_id,
                    "status": status,
                    "filled_qty": filled_qty,
                    "requested_qty": requested_qty,
                    "limit_price": float(limit_price),
                    "order_style": order_style,
                    "source": source,
                    "retry_after_date": position.get(
                        "paper_exit_retry_after_date",
                        "",
                    ),
                },
            )

        self._tracked_alert_positions[
            position[
                "id"
            ]
        ] = position
        self._save_trade_alert_positions_state()

        return {
            "submitted": source == "SUBMIT_ORDER",
            "linked": True,
            "order_id": broker_order_id,
            "client_order_id": returned_client_order_id,
            "status": status,
            "limit_price": float(limit_price),
            "order_style": order_style,
            "source": source,
            "filled_qty": filled_qty,
        }


    def _execute_paper_option_exit(
        self,
        row,
        lifecycle_id,
    ):
        """Idempotently submit/link one Alpaca PAPER option close."""

        if not self.paper_exit_execution_armed:
            return {
                "submitted": False,
                "linked": False,
                "status": "DISABLED",
                "reason": "PAPER exit execution is not armed",
            }

        if not self.alpaca_is_paper:
            raise RuntimeError(
                "Paper exit execution attempted with a non-paper Alpaca client"
            )

        position = self._tracked_alert_positions.get(
            lifecycle_id
        )

        if position is None:
            raise RuntimeError(
                "Lifecycle record must exist before PAPER exit execution"
            )

        broker_open_qty = self._lifecycle_float(
            position.get(
                "broker_open_quantity",
                0.0,
            ),
            0.0,
        ) or 0.0

        if broker_open_qty <= 0:
            return {
                "submitted": False,
                "linked": False,
                "status": "NO_BROKER_POSITION",
                "reason": "No broker-confirmed open quantity remains",
            }

        max_per_run = max(
            0,
            int(
                self.parameters.get(
                    "paper_exit_execution_max_orders_per_run",
                    5,
                )
            ),
        )

        if (
            max_per_run > 0
            and self._paper_exit_orders_submitted_this_run
            >= max_per_run
        ):
            return {
                "submitted": False,
                "linked": False,
                "status": "RUN_CAP",
                "reason": "PAPER exit execution per-run order cap reached",
            }

        decision = str(
            row.get(
                "decision",
                "",
            )
            or ""
        ).upper()

        required_options_level = (
            3
            if decision in {
                "BULL CALL SPREAD",
                "BEAR PUT SPREAD",
            }
            else 2
        )

        try:
            known_options_level = int(
                self.options_trading_level
            )
        except (
            TypeError,
            ValueError,
        ):
            known_options_level = None

        if known_options_level is None:
            return {
                "submitted": False,
                "linked": False,
                "status": "OPTIONS_LEVEL_UNKNOWN",
                "reason": "PAPER exit execution fails closed when options level is unknown",
            }

        if known_options_level < required_options_level:
            return {
                "submitted": False,
                "linked": False,
                "status": "OPTIONS_LEVEL_INSUFFICIENT",
                "reason": (
                    f"Structure requires Alpaca options level "
                    f"{required_options_level}; account reports "
                    f"level {known_options_level}"
                ),
            }

        row_for_order = row.copy()
        row_for_order[
            "quantity"
        ] = max(
            1,
            int(
                round(
                    broker_open_qty
                )
            ),
        )

        trade_date = self.get_datetime().date()

        retry_after_text = str(
            position.get(
                "paper_exit_retry_after_date",
                "",
            )
            or ""
        )

        if retry_after_text:
            try:
                retry_after_date = date.fromisoformat(
                    retry_after_text
                )
            except ValueError:
                retry_after_date = None

            if (
                retry_after_date is not None
                and trade_date < retry_after_date
            ):
                return {
                    "submitted": False,
                    "linked": False,
                    "status": "RETRY_DEFERRED",
                    "reason": (
                        "Prior PAPER close order left a terminal "
                        "remainder; same-day retry/chasing is disabled"
                    ),
                    "retry_after_date": retry_after_text,
                }

        client_order_id = self._paper_exit_execution_client_order_id(
            lifecycle_id,
            trade_date,
        )

        (
            order_request,
            limit_price,
            order_style,
        ) = self._build_paper_option_exit_order_request(
            row_for_order,
            client_order_id,
        )

        # Restart idempotency: recover today's deterministic close order
        # instead of creating a duplicate. A new calendar day gets a new
        # id so a DAY order that expired can be retried for the remaining qty.
        try:
            existing_order = (
                self.alpaca_trading_client
                .get_order_by_client_id(
                    client_order_id
                )
            )
        except Exception as exc:
            if self._paper_execution_order_not_found(
                exc
            ):
                existing_order = None
            else:
                position[
                    "paper_exit_execution_last_error"
                ] = str(exc)
                position[
                    "paper_exit_execution_last_error_at"
                ] = self.get_datetime().isoformat()
                self._save_trade_alert_positions_state()
                return {
                    "submitted": False,
                    "linked": False,
                    "status": "LOOKUP_FAILED",
                    "reason": (
                        "Could not verify deterministic PAPER close client "
                        "order ID; submission fails closed"
                    ),
                    "error": str(exc),
                    "client_order_id": client_order_id,
                }

        if existing_order is not None:
            return self._persist_paper_exit_order_link(
                position,
                existing_order,
                client_order_id,
                limit_price,
                order_style,
                "EXISTING_CLIENT_ORDER_ID",
            )

        try:
            order = self.alpaca_trading_client.submit_order(
                order_data=order_request
            )
        except Exception as exc:
            position[
                "paper_exit_execution_last_error"
            ] = str(exc)
            position[
                "paper_exit_execution_last_error_at"
            ] = self.get_datetime().isoformat()

            self._record_lifecycle_event(
                position,
                "PAPER_EXIT_SUBMISSION_FAILED",
                "Alpaca PAPER close submission failed",
                details={
                    "client_order_id": client_order_id,
                    "limit_price": float(limit_price),
                    "order_style": order_style,
                    "error": str(exc),
                },
            )

            self._tracked_alert_positions[
                lifecycle_id
            ] = position
            self._save_trade_alert_positions_state()

            return {
                "submitted": False,
                "linked": False,
                "status": "SUBMIT_FAILED",
                "reason": "Alpaca PAPER submit_order close failed",
                "error": str(exc),
                "client_order_id": client_order_id,
                "limit_price": float(limit_price),
                "order_style": order_style,
            }

        self._paper_exit_orders_submitted_this_run += 1

        return self._persist_paper_exit_order_link(
            position,
            order,
            client_order_id,
            limit_price,
            order_style,
            "SUBMIT_ORDER",
        )


    def _log_paper_exit_execution_result(
        self,
        payload,
        result,
    ):
        if not self.paper_exit_execution_armed:
            return

        status = str(
            result.get(
                "status",
                "UNKNOWN",
            )
            or "UNKNOWN"
        )

        if result.get(
            "linked",
            False,
        ):
            self.log_message(
                "\n\n"
                "======= ALPACA PAPER EXIT ORDER =======\n"
                f"{payload['direction']} | {payload['underlying']} | "
                f"{payload['decision']}\n"
                f"Lifecycle: {payload['lifecycle_id']}\n"
                f"Broker order ID: {result.get('order_id', '')}\n"
                f"Client order ID: {result.get('client_order_id', '')}\n"
                f"Order style: {result.get('order_style', '')}\n"
                f"Limit signed price/share: ${result.get('limit_price', 0.0):.2f}\n"
                f"Broker status: {status.upper()}\n"
                f"Link source: {result.get('source', '')}\n"
                "ACCOUNT: ALPACA PAPER ONLY\n"
                "======================================="
            )
        else:
            self.log_message(
                "PAPER exit order was NOT submitted/linked for "
                f"{payload['underlying']}: {result.get('reason', status)}"
                + (
                    f". Error={result.get('error')}"
                    if result.get('error')
                    else ""
                )
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

        # Per scanner iteration, not per process lifetime.
        self._paper_execution_orders_submitted_this_run = 0

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
                + (
                    "MODE: ALPACA PAPER ENTRY EXECUTION ARMED\n"
                    if self.paper_execution_armed
                    else "MODE: ALERT ONLY - NO ORDER SUBMITTED\n"
                )
                + "================================="
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

            if self.paper_execution_armed:
                execution_result = (
                    self._execute_paper_option_entry(
                        row,
                        lifecycle_id,
                    )
                )

                payload[
                    "paper_execution"
                ] = execution_result

                lifecycle_record = (
                    self._tracked_alert_positions.get(
                        lifecycle_id,
                        {},
                    )
                )

                payload[
                    "lifecycle_status"
                ] = str(
                    lifecycle_record.get(
                        "status",
                        payload[
                            "lifecycle_status"
                        ],
                    )
                )

                self._log_paper_execution_result(
                    payload,
                    execution_result,
                )

            self._append_trade_alert_jsonl(
                payload
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
            + (
                "PAPER-executable trade alerts..."
                if self.paper_execution_armed
                else "read-only trade alerts..."
            )
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
    # RUNTIME SCAN / MANAGEMENT CADENCE
    # ======================================================

    def _intraday_management_lifecycles(self):
        """Return option lifecycles needing broker/order/exit monitoring."""

        managed_statuses = {
            "ENTRY_WORKING",
            "PARTIALLY_OPEN",
            "OPEN",
            "CLOSE_ALERTED",
            "CLOSE_WORKING",
            "PARTIALLY_CLOSED",
            "ORPHANED",
        }

        rows = []

        for lifecycle_id, position in (
            self._tracked_alert_positions.items()
        ):
            if (
                str(position.get("asset_type", "OPTION") or "OPTION").upper()
                != "OPTION"
            ):
                continue

            status = self._normalize_lifecycle_status(
                position.get("status", "ALERTED")
            )

            # An explicitly linked broker entry should never be allowed to
            # disappear into the old ALERTED cadence even if a stale state
            # file has not yet promoted its lifecycle status.
            linked_alert = (
                status == "ALERTED"
                and bool(position.get("broker_entry_order_id"))
            )

            if status in managed_statuses or linked_alert:
                rows.append((lifecycle_id, position))

        return rows

    def _set_in_session_runtime_cadence(self):
        """Report logical work mode; framework wake cadence stays fixed at 1M."""

        managed = self._intraday_management_lifecycles()
        needs_management = bool(managed)

        if needs_management:
            label = "INTRADAY_MANAGEMENT"
            work_text = (
                f"broker/order/exit management active for {len(managed)} "
                "option lifecycle(s); every 1M framework wake is eligible "
                "for lightweight management. Full scanning remains once per date."
            )
        else:
            label = "DAILY_SCANNER"
            work_text = (
                "no broker-managed option lifecycle is active; framework still "
                "wakes every 1M, but expensive full scanning remains once per date."
            )

        # Do NOT mutate self.sleeptime here. LumiBot 4.5.x can schedule the
        # next wake using the value captured at iteration start, making dynamic
        # changes lag or sleep until the following day. The framework driver is
        # permanently options_management_sleeptime (1M by default).
        if getattr(self, "_runtime_cadence_label", None) != label:
            self._runtime_cadence_label = label
            self.log_message(
                "RUNTIME WORK MODE: " + work_text
            )

        return needs_management

    @staticmethod
    def _sleeptime_seconds(value):
        """Parse LumiBot-style S/M/H/D duration strings for internal throttles."""

        text = str(value or "").strip().upper()
        if not text:
            return 60.0

        units = {"S": 1.0, "M": 60.0, "H": 3600.0, "D": 86400.0}
        suffix = text[-1]
        if suffix in units:
            number = text[:-1].strip()
            try:
                return max(1.0, float(number) * units[suffix])
            except Exception:
                return 60.0

        try:
            # Match LumiBot's integer/numeric convention: bare values are minutes.
            return max(1.0, float(text) * 60.0)
        except Exception:
            return 60.0

    def _regular_market_is_open_from_session_status(self, session_status):
        """True during regular hours, including our opening/closing buffers."""

        if bool(session_status.get("allowed")):
            return True

        reason = str(session_status.get("reason", "") or "").lower()
        return reason.startswith("inside regular session")

    def _closed_market_reconciliation_due(self, session_status):
        """Throttle after-hours broker reconciliation while the 1M driver stays alive."""

        now = session_status.get("now")
        if not isinstance(now, datetime):
            now = datetime.now(timezone.utc)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        now_utc = now.astimezone(timezone.utc)
        interval_seconds = self._sleeptime_seconds(
            self.parameters["options_closed_retry_sleeptime"]
        )

        last = getattr(self, "_last_closed_market_reconcile_at", None)
        if isinstance(last, datetime):
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            elapsed = (now_utc - last.astimezone(timezone.utc)).total_seconds()
            if elapsed < interval_seconds:
                return False

        self._last_closed_market_reconcile_at = now_utc
        return True

    def _full_scan_due_for_market_date(self, market_date):
        return getattr(
            self,
            "_last_full_scan_market_date",
            None,
        ) != market_date

    def _management_only_stock_results(self):
        """Build only the stock thesis metrics needed by open positions."""

        underlyings = sorted({
            str(position.get("underlying", "") or "")
            for _, position in self._intraday_management_lifecycles()
            if (
                self._normalize_lifecycle_status(
                    position.get("status", "ALERTED")
                )
                in self._lifecycle_exit_managed_statuses()
                and str(position.get("underlying", "") or "")
            )
        })

        if not underlyings:
            return pd.DataFrame()

        try:
            histories = self.get_historical_prices_for_assets(
                underlyings,
                65,
                timestep="day",
                chunk_size=100,
                max_workers=min(5, max(1, len(underlyings))),
            )
        except Exception as exc:
            self.log_message(
                "Management-only stock thesis lookup failed; "
                f"P/L/DTE management will continue with thesis UNKNOWN. "
                f"Reason={exc}"
            )
            return pd.DataFrame()

        rows = []
        today = self.get_datetime().date()

        for asset, bars in histories.items():
            if bars is None:
                continue

            df = bars.pandas_df.copy()
            if len(df) > 0 and df.index[-1].date() == today:
                df = df.iloc[:-1]

            if len(df) < 21:
                continue

            close = df["close"].astype(float)
            price = float(close.iloc[-1])
            sma20 = float(close.tail(20).mean())
            momentum20 = float(price / close.iloc[-21] - 1)
            symbol = asset.symbol if hasattr(asset, "symbol") else str(asset)

            rows.append({
                "symbol": symbol,
                "price": price,
                "sma20": sma20,
                "momentum20": momentum20,
            })

        result = pd.DataFrame(rows)
        self.log_message(
            "MANAGEMENT-ONLY THESIS DATA: "
            f"{len(result)} of {len(underlyings)} broker-managed "
            "underlying(s) have current completed-daily metrics."
        )
        return result

    # ======================================================
    # MAIN SCANNER
    # ======================================================

    def on_trading_iteration(self):

        # --------------------------------------------------
        # FIXED 1M FRAMEWORK DRIVER + INTERNAL WORK THROTTLING
        # --------------------------------------------------
        #
        # LumiBot's scheduler may use the sleeptime value captured at the start
        # of an iteration when deciding the next wake. Therefore self.sleeptime
        # remains fixed at 1M for the lifetime of this process. Full scans,
        # management work, and closed-market reconciliation are throttled here.
        # --------------------------------------------------

        session_status = self._get_options_session_status()
        regular_market_open = self._regular_market_is_open_from_session_status(
            session_status
        )

        if regular_market_open:
            self._closed_gate_skip_logged = False
            reconcile_now = True
        else:
            reconcile_now = self._closed_market_reconciliation_due(
                session_status
            )

        if reconcile_now:
            try:
                (
                    lifecycle_results,
                    lifecycle_snapshot,
                ) = self.reconcile_trade_lifecycle_states()

                self.log_trade_lifecycle_reconciliation(
                    lifecycle_results,
                    lifecycle_snapshot,
                )

                self.log_broker_fill_accounting()
                analytics = self.refresh_trade_journal_analytics()
                self.refresh_trading_circuit_breakers(
                    market_now=session_status.get("now")
                )
                self._scan_and_record_trading_anomalies(
                    lifecycle_snapshot=lifecycle_snapshot
                )
                self.refresh_daily_operational_summary(
                    session_status=session_status,
                    lifecycle_snapshot=lifecycle_snapshot,
                    analytics=analytics,
                )

            except Exception as exc:
                self._record_trading_anomaly(
                    "LIFECYCLE_RECONCILIATION_FAILURE",
                    "ERROR",
                    f"Trade lifecycle reconciliation failed: {exc}",
                )
                self.log_message(
                    "Trade lifecycle reconciliation failed; "
                    "prior persistent states are retained. "
                    f"Reason={exc}"
                )
        else:
            # Avoid hammering Alpaca all night. The framework still wakes every
            # minute so it cannot get stranded on a once-per-day scheduler.
            if getattr(self, "_runtime_cadence_label", None) != "CLOSED_THROTTLED":
                self._runtime_cadence_label = "CLOSED_THROTTLED"
                self.log_message(
                    "CLOSED-MARKET THROTTLE: framework wake cadence="
                    f"{self.sleeptime}; broker reconciliation interval="
                    f"{self.parameters['options_closed_retry_sleeptime']}."
                )

        # --------------------------------------------------
        # OPTIONS MARKET-SESSION GATE
        # --------------------------------------------------
        # New entry scans and P/L exit signals remain regular-session only.
        # Broker/order reconciliation above can still run after hours on the
        # internal throttled interval.
        # --------------------------------------------------

        if not session_status["allowed"]:
            market_now = session_status.get("now")
            now_text = (
                "unknown"
                if market_now is None
                else market_now.strftime("%Y-%m-%d %I:%M:%S %p ET")
            )

            actionable_open = session_status.get("actionable_open")
            actionable_close = session_status.get("actionable_close")

            if actionable_open is not None and actionable_close is not None:
                window_text = (
                    actionable_open.strftime("%I:%M %p ET")
                    + " - "
                    + actionable_close.strftime("%I:%M %p ET")
                )
            else:
                window_text = "not currently available"

            if reconcile_now or not self._closed_gate_skip_logged:
                self.log_message(
                    "MARKET SESSION GATE: CLOSED. "
                    f"Market time={now_text}. "
                    f"Reason={session_status['reason']}. "
                    f"Actionable window={window_text}. "
                    + (
                        "Broker/order lifecycle reconciliation completed; "
                        if reconcile_now
                        else "Broker reconciliation skipped on this throttled wake; "
                    )
                    + "stock/options entries and exit-management P/L signals are skipped. "
                    + f"Framework wake cadence={self.sleeptime}; "
                    + f"closed-market reconciliation interval="
                    + f"{self.parameters['options_closed_retry_sleeptime']}."
                )
                self._closed_gate_skip_logged = True
            return

        market_now = session_status["now"]
        actionable_open = session_status["actionable_open"]
        actionable_close = session_status["actionable_close"]

        if actionable_open is not None and actionable_close is not None:
            actionable_window_text = (
                actionable_open.strftime("%I:%M %p ET")
                + " - "
                + actionable_close.strftime("%I:%M %p ET")
            )
        else:
            actionable_window_text = "gate disabled"

        self.log_message(
            "MARKET SESSION GATE: OPEN. "
            f"Market time={market_now.strftime('%I:%M:%S %p ET')}. "
            f"Actionable window={actionable_window_text}. "
            f"Max option quote age={self.parameters['option_quote_max_age_seconds']}s."
        )

        needs_intraday_management = self._set_in_session_runtime_cadence()
        market_date = market_now.date()
        full_scan_due = self._full_scan_due_for_market_date(
            market_date
        )

        if not full_scan_due:
            if needs_intraday_management:
                self.log_message(
                    "MANAGEMENT-ONLY ITERATION: full stock/options scan already "
                    "completed for this market date; reconciling exposure and "
                    "evaluating exits without rebuilding the full universe."
                )
                stock_results = self._management_only_stock_results()
                self.run_exit_management(stock_results)
                self._set_in_session_runtime_cadence()
            else:
                self.log_message(
                    "Full stock/options scan already completed for this market date "
                    "and no broker-managed option lifecycle requires intraday work."
                )
            return

        # Mark the expensive scanner as consumed for this market date before
        # starting it, so an unrelated downstream exception cannot turn a 1M
        # management cadence into repeated full-universe API scans. A process
        # restart intentionally permits one fresh full scan.
        self._last_full_scan_market_date = market_date

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

                self._set_in_session_runtime_cadence()

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

            # New submissions or newly opened/closing positions must switch
            # the next wakeup from the daily scanner cadence to lightweight
            # intraday management immediately.
            self._set_in_session_runtime_cadence()

        except Exception as exc:

            self.log_message(
                "Options eligibility/ranking "
                f"failed: {exc}"
            )