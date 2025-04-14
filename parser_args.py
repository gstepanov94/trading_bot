import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Crypto Trading Bot with Stop Loss and Take Profit"
    )
    parser.add_argument(
        "--symbol", type=str, default="BTCUSDT", help="Trading pair symbol"
    )
    parser.add_argument(
        "--amount", type=float, default=100.0, help="Amount to invest in USDT"
    )
    parser.add_argument(
        "--stop_loss", type=float, default=0.25, help="Stop loss percentage"
    )
    parser.add_argument(
        "--take_profit", type=float, default=0.25, help="Take profit percentage"
    )
    parser.add_argument(
        "--timeout", type=int, default=60, help="Trade timeout in seconds"
    )
    parser.add_argument(
        "--cooldown", type=int, default=30, help="Cooldown between trades in seconds"
    )
    return parser.parse_args()
