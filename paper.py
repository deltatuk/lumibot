import os
import signal
import threading
import time

from dotenv import load_dotenv
from lumibot.brokers import Alpaca
from lumibot.traders import Trader

from strategy import StockSuggestionStrategy


# ==========================================================
# LOAD ENVIRONMENT
# ==========================================================

load_dotenv()


ALPACA_CONFIG = {
    "API_KEY": os.environ["ALPACA_API_KEY"],
    "API_SECRET": os.environ["ALPACA_API_SECRET"],
    "PAPER": (
        os.environ.get(
            "ALPACA_IS_PAPER",
            "true",
        ).lower()
        == "true"
    ),
}


# Maximum time to allow graceful shutdown.
SHUTDOWN_TIMEOUT = 5


# Used by the signal handler to tell the main thread
# that shutdown has been requested.
shutdown_requested = threading.Event()


# ==========================================================
# SIGNAL HANDLER
# ==========================================================

def request_shutdown(signum, frame):
    """
    Handle Ctrl+C / SIGTERM.

    First interrupt:
        Request a graceful shutdown.

    Second interrupt:
        Force the Python process to exit immediately.
    """

    if shutdown_requested.is_set():

        print(
            "\nSecond interrupt received. "
            "Forcing exit."
        )

        os._exit(130)

    print(
        "\nShutdown requested..."
    )

    shutdown_requested.set()


# ==========================================================
# GRACEFUL SHUTDOWN
# ==========================================================

def graceful_shutdown(trader, broker):

    print(
        "Beginning graceful shutdown..."
    )

    # ------------------------------------------------------
    # GET STRATEGY EXECUTORS
    # ------------------------------------------------------

    executors = getattr(
        trader,
        "_pool",
        [],
    )

    # ------------------------------------------------------
    # 1. STOP LUMIBOT STRATEGY
    # ------------------------------------------------------

    try:

        trader.stop_all()

    except Exception as exc:

        print(
            f"Warning stopping trader: {exc}"
        )

    # ------------------------------------------------------
    # 2. EXPLICITLY SIGNAL EXECUTOR THREADS
    # ------------------------------------------------------
    #
    # trader.stop_all() should already stop them, but we
    # explicitly set both events as an additional safeguard.
    # ------------------------------------------------------

    for executor in executors:

        try:

            executor.stop_event.set()

        except Exception:
            pass

        try:

            executor.check_queue_stop_event.set()

        except Exception:
            pass

    # ------------------------------------------------------
    # 3. CLEAN UP LUMIBOT BROKER THREADS
    # ------------------------------------------------------

    try:

        cleanup_streams = getattr(
            broker,
            "cleanup_streams",
            None,
        )

        if callable(cleanup_streams):

            cleanup_streams()

    except Exception as exc:

        print(
            f"Warning cleaning broker streams: {exc}"
        )

    # ------------------------------------------------------
    # 4. SHUT DOWN DATA SOURCE THREAD POOLS
    # ------------------------------------------------------

    sources = [
        getattr(
            broker,
            "data_source",
            None,
        ),
        getattr(
            broker,
            "option_source",
            None,
        ),
    ]

    seen = set()

    for source in sources:

        if source is None:
            continue

        source_id = id(source)

        # Avoid shutting down the same object twice.
        if source_id in seen:
            continue

        seen.add(source_id)

        shutdown = getattr(
            source,
            "shutdown",
            None,
        )

        if callable(shutdown):

            try:

                shutdown()

            except Exception as exc:

                print(
                    "Warning shutting down "
                    f"data source: {exc}"
                )

    # ------------------------------------------------------
    # 5. WAIT FOR STRATEGY EXECUTORS
    # ------------------------------------------------------

    deadline = (
        time.monotonic()
        + SHUTDOWN_TIMEOUT
    )

    for executor in executors:

        remaining = (
            deadline
            - time.monotonic()
        )

        if remaining <= 0:
            break

        try:

            executor.join(
                timeout=remaining
            )

        except Exception:
            pass

    # ------------------------------------------------------
    # 6. CLEAN UP SCHEDULERS AFTER EXECUTOR EXIT
    # ------------------------------------------------------
    #
    # LumiBot can sometimes shut APScheduler down and then
    # briefly recreate it while the executor thread is
    # unwinding.
    #
    # Waiting until AFTER executor.join() prevents that race.
    # ------------------------------------------------------

    for executor in executors:

        scheduler = getattr(
            executor,
            "scheduler",
            None,
        )

        if scheduler is None:
            continue

        try:

            if scheduler.running:

                try:

                    scheduler.remove_all_jobs()

                except Exception:
                    pass

                scheduler.shutdown(
                    wait=False
                )

        except Exception as exc:

            print(
                f"Warning stopping scheduler: {exc}"
            )

        finally:

            try:

                executor.scheduler = None

            except Exception:
                pass

    # ------------------------------------------------------
    # 7. CHECK FOR THREADS THAT CAN BLOCK PYTHON EXIT
    # ------------------------------------------------------
    #
    # Daemon threads do NOT prevent Python from exiting.
    #
    # Only non-daemon threads are considered a shutdown
    # failure.
    # ------------------------------------------------------

    current = threading.current_thread()

    blocking_threads = [
        thread
        for thread in threading.enumerate()
        if (
            thread is not current
            and thread.is_alive()
            and not thread.daemon
        )
    ]

    daemon_threads = [
        thread
        for thread in threading.enumerate()
        if (
            thread is not current
            and thread.is_alive()
            and thread.daemon
        )
    ]

    # ------------------------------------------------------
    # REPORT REMAINING DAEMON THREADS
    # ------------------------------------------------------

    if daemon_threads:

        print(
            "Remaining daemon threads:"
        )

        for thread in daemon_threads:

            print(
                f"  - {thread.name}"
            )

    # ------------------------------------------------------
    # FORCE EXIT ONLY IF NON-DAEMON THREADS REMAIN
    # ------------------------------------------------------

    if blocking_threads:

        print(
            "\nNon-daemon threads still alive:"
        )

        for thread in blocking_threads:

            print(
                f"  - {thread.name}"
            )

        print(
            "Forcing exit because blocking "
            "threads remain."
        )

        os._exit(130)

    print(
        "Shutdown complete."
    )


# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    # ------------------------------------------------------
    # CREATE ALPACA BROKER
    # ------------------------------------------------------
    #
    # Scanner-only background behavior is controlled through
    # .env:
    #
    # LUMIBOT_CONNECT_STREAM=false
    # LUMIBOT_START_ORDERS_THREAD=false
    # LUMIBOT_TELEMETRY=false
    #
    # ------------------------------------------------------

    broker = Alpaca(
        ALPACA_CONFIG
    )

    # ------------------------------------------------------
    # CREATE STRATEGY
    # ------------------------------------------------------

    strategy = StockSuggestionStrategy(
        broker=broker
    )

    # ------------------------------------------------------
    # CREATE TRADER
    # ------------------------------------------------------

    trader = Trader()

    trader.add_strategy(
        strategy
    )

    # ------------------------------------------------------
    # START STRATEGY ASYNCHRONOUSLY
    # ------------------------------------------------------
    #
    # We keep the main Python thread available so that it
    # owns Ctrl+C handling.
    # ------------------------------------------------------

    trader.run_all(
        async_=True
    )

    # ------------------------------------------------------
    # INSTALL OUR SIGNAL HANDLERS
    # ------------------------------------------------------
    #
    # LumiBot installs its own SIGINT handler during
    # run_all(), so ours must be installed afterward.
    # ------------------------------------------------------

    signal.signal(
        signal.SIGINT,
        request_shutdown,
    )

    signal.signal(
        signal.SIGTERM,
        request_shutdown,
    )

    print(
        "StockSuggestionStrategy running."
    )

    print(
        "Press Ctrl+C to stop."
    )

    # ------------------------------------------------------
    # MAIN PROCESS LOOP
    # ------------------------------------------------------

    try:

        while True:

            # Wait up to half a second for shutdown signal.
            if shutdown_requested.wait(
                timeout=0.5
            ):
                break

            # --------------------------------------------------
            # DETECT STRATEGY EXIT
            # --------------------------------------------------
            #
            # If LumiBot's strategy thread exits unexpectedly,
            # don't leave paper.py alive indefinitely.
            # --------------------------------------------------

            executors = getattr(
                trader,
                "_pool",
                [],
            )

            if (
                executors
                and not any(
                    executor.is_alive()
                    for executor in executors
                )
            ):

                print(
                    "Strategy stopped."
                )

                break

    finally:

        graceful_shutdown(
            trader,
            broker,
        )