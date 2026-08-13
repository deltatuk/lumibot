from datetime import datetime

from lumibot.backtesting import YahooDataBacktesting

from strategy import StockSuggestionStrategy


if __name__ == "__main__":

    StockSuggestionStrategy.backtest(
        YahooDataBacktesting,
        datetime(2025, 1, 1),
        datetime(2025, 6, 30),
        benchmark_asset="SPY",
    )