import json
import asyncio
import websockets
import random
import ssl
import uuid
from parser_args import parse_arguments
import aiohttp
from typing import Literal, Union
from datetime import datetime, timedelta, timezone
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import os
from dotenv import load_dotenv
from pydantic import BaseModel
import base64
from decimal import Decimal
from logger import setup_logger

logger = setup_logger(__name__)

ssl_context = ssl._create_unverified_context()

TZ = timezone.utc


ssl_context = ssl._create_unverified_context()
load_dotenv()

API_KEY = os.getenv("API_KEY_ED25519")
PRIVATE_KEY_PATH = os.getenv("API_SECRET_ED25519_PATH")

with open(PRIVATE_KEY_PATH, "rb") as f:
    private_key = load_pem_private_key(data=f.read(), password=None)


class PairInfo(BaseModel):
    symbol: str
    base_symbol: str
    quote_symbol: str
    base_precision: int
    quote_precision: int
    min_qty: float
    step_size: float
    tick_size: float


class MarketConnector:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self.symbol_lower = symbol.lower()
        self.ws_url = f"wss://stream.testnet.binance.vision:9443/ws/{self.symbol_lower}@bookTicker"
        self.rest_url = "https://testnet.binance.vision/api"

    async def get_lot_info(self) -> PairInfo:
        """Get lot size information for the symbol"""
        url = f"{self.rest_url}/v3/exchangeInfo?symbol={self.symbol}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=ssl_context) as response:
                data = await response.json()
                filters = data["symbols"][0]["filters"]
                for f in filters:
                    if f["filterType"] == "PRICE_FILTER":
                        tick_size = f["tickSize"]
                    if f["filterType"] == "LOT_SIZE":
                        min_qty = f["minQty"]
                        step_size = f["stepSize"]

                pair_info = PairInfo(
                    symbol=self.symbol,
                    base_symbol=data["symbols"][0]["baseAsset"],
                    quote_symbol=data["symbols"][0]["quoteAsset"],
                    base_precision=data["symbols"][0]["baseAssetPrecision"],
                    quote_precision=data["symbols"][0]["quotePrecision"],
                    min_qty=min_qty,
                    step_size=step_size,
                    tick_size=tick_size,
                )
                return pair_info

    async def connect_price_stream(self):
        """Connect to WebSocket for price updates"""
        return await websockets.connect(
            self.ws_url, ssl=ssl_context, ping_interval=60, ping_timeout=180
        )

    def process_price_update(self, data):
        """Process price updates from WebSocket"""
        best_bid = float(data["b"])
        best_ask = float(data["a"])
        current_price = (best_bid + best_ask) / 2
        return {"bid": best_bid, "ask": best_ask, "price": current_price}


class Balance(BaseModel):
    base_balance: Decimal = 0
    quote_balance: Decimal = 0

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }


class TradeConnector:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.ws_url_api = "wss://ws-api.testnet.binance.vision/ws-api/v3"
        self.rest_url = "https://testnet.binance.vision/api"

    async def get_balance(self, base_symbol, quote_symbol) -> Balance:
        """Get account balance"""
        url = f"{self.rest_url}/v3/account"
        params = {
            "timestamp": int(datetime.now(tz=TZ).timestamp() * 1000),
        }
        params["signature"] = self.sign_request(params)
        headers = {"X-MBX-APIKEY": API_KEY}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=headers, params=params, ssl=ssl_context
            ) as response:
                if response.status != 200:
                    raise Exception(f"Error fetching balance: {response.status}")
                else:
                    account_info = await response.json()
                    balances = account_info["balances"]
                    base_balance = [b for b in balances if b["asset"] == base_symbol]
                    quote_balance = [b for b in balances if b["asset"] == quote_symbol]
                    if base_balance:
                        base_balance = Decimal(base_balance[0]["free"])
                    else:
                        base_balance = Decimal(0)
                    if quote_balance:
                        quote_balance = Decimal(quote_balance[0]["free"])
                    else:
                        quote_balance = Decimal(0)
                    return Balance(
                        base_balance=base_balance, quote_balance=quote_balance
                    )
        return Balance(base_balance=0, quote_balance=0)

    def sign_request(self, params: dict[str, Union[str, int]]) -> str:
        query_string = "&".join(
            [f"{param}={value}" for param, value in sorted(params.items())]
        )
        signature = base64.b64encode(private_key.sign(query_string.encode("ASCII")))
        return signature.decode("ASCII")

    def get_market_order(self, amount: str, side: Literal["BUY", "SELL"]):
        order = {
            "id": uuid.uuid4().hex,
            "method": "order.place",
            "params": {
                "apiKey": API_KEY,
                "symbol": self.symbol,
                "side": side,
                "type": "MARKET",
                "quantity": amount,
                "timestamp": int(datetime.now(tz=TZ).timestamp() * 1000),
            },
        }
        order["params"]["signature"] = self.sign_request(order["params"])
        return order

    def get_avg_filled_price(self, order_response) -> Decimal:
        total = 0
        total_qty = Decimal(order_response.get("executedQty", 0))
        fills = order_response.get("fills", [])
        for fill in fills:
            price = Decimal(fill["price"])
            qty = Decimal(fill["qty"])
            total += price * qty
        return total / total_qty if total_qty > 0 else Decimal(0)

    async def execute_market_order(self, amount: str, side: Literal["BUY", "SELL"]):
        """Execute a real crypto order"""
        try:
            async with websockets.connect(self.ws_url_api, ssl=ssl_context) as ws_api:
                order = self.get_market_order(amount=amount, side=side)
                try:
                    await ws_api.send(json.dumps(order))
                except Exception as e:
                    logger.error(f"Error sending order: {json.dumps(order)}")
                    return {"success": False, "error": str(e)}
                response = await ws_api.recv()
                order_response = json.loads(response)

                if (
                    "result" in order_response
                    and order_response.get("result", {}).get("status", "") == "FILLED"
                    and order_response.get("id") == order["id"]
                ):
                    return {
                        "success": True,
                        "order_id": order_response["result"].get("orderId"),
                        "price": self.get_avg_filled_price(order_response["result"]),
                        "amount": Decimal(order_response["result"].get("executedQty")),
                        "response": order_response["result"],
                    }
                else:
                    return {"success": False, "error": order_response, "order": order}
        except Exception as e:
            return {"success": False, "error": str(e)}


class BinanceTradingBot:
    def __init__(
        self,
        symbol: str,
        amount: Decimal,
        stop_loss_pct: Decimal,
        take_profit_pct: Decimal,
        timeout_seconds: int,
        cooldown_seconds: int,
    ) -> None:
        self.symbol = symbol.upper()
        self.amount = amount
        self.stop_loss_pct = Decimal(stop_loss_pct)
        self.take_profit_pct = Decimal(take_profit_pct)
        self.timeout_seconds = timeout_seconds
        self.cooldown_seconds = cooldown_seconds

        # Initialize connectors
        self.market_connector = MarketConnector(symbol)
        self.trade_connector = TradeConnector(symbol)

        # Trading state
        self.current_position: Decimal = 0
        self.entry_price: Decimal = 0
        self.in_trade: bool = False
        self.trade_start_time = None
        self.last_price = 0
        self.last_bid = 0
        self.last_ask = 0

        # For lot size
        self.step_size = "0"
        self.min_qty = "0"
        self.tick_size = "0"
        self.base_symbol = ""
        self.quote_symbol = ""

        self.base_balance = 0
        self.quote_balance = 0

        self.total_profit = 0

        self._initial_msg()

    def _initial_msg(self) -> None:
        print("Trading bot initialized with parameters:")
        print(f"Symbol: {self.symbol}")
        print(f"Amount: {self.amount}")
        print(f"Stop loss: {self.stop_loss_pct}%")
        print(f"Take profit: {self.take_profit_pct}%")
        print(f"Timeout: {self.timeout_seconds} seconds")
        print(f"Cooldown: {self.cooldown_seconds} seconds")

    def adjust_amount(self, amount) -> str:
        amount = Decimal(str(amount))
        min_qty = Decimal(str(self.min_qty))
        step_size = Decimal(str(self.step_size))
        steps = amount // step_size
        adjusted = steps * step_size
        if adjusted < min_qty:
            adjusted = min_qty
        return str(adjusted)

    async def _load_lot_info(self) -> None:
        """Load lot size information"""
        pair_info = await self.market_connector.get_lot_info()
        self.min_qty = pair_info.min_qty
        self.step_size = pair_info.step_size
        self.tick_size = pair_info.tick_size
        self.base_symbol = pair_info.base_symbol
        self.quote_symbol = pair_info.quote_symbol

    async def _load_balance(self) -> None:
        """Load account balance"""
        balance = await self.trade_connector.get_balance(
            self.base_symbol, self.quote_symbol
        )
        self.base_balance = balance.base_balance
        self.quote_balance = balance.quote_balance

    def _update_balance(self, order_result, side) -> None:
        """Update the balance after a trade"""
        if order_result["success"]:
            if side == "BUY":
                self.base_balance -= order_result["amount"]
                self.quote_balance -= order_result["price"] * order_result["amount"]

            elif side == "SELL":
                self.base_balance += order_result["amount"]
                self.quote_balance += order_result["price"] * order_result["amount"]

    async def start_trading(self) -> None:
        """Main trading loop"""

        await self._load_lot_info()
        await self._load_balance()
        print(f"Starting trading bot for {self.symbol}")
        # Wait a random time before first trade
        initial_wait = random.randint(5, 15)
        print(f"Waiting {initial_wait} seconds before first trade...")
        await asyncio.sleep(initial_wait)

        while True:
            try:
                websocket = await self.market_connector.connect_price_stream()
                if not self.in_trade:
                    await self.buy_crypto(websocket)

                async for message in websocket:
                    data = json.loads(message)
                    price_data = self.market_connector.process_price_update(data)
                    self.last_bid = price_data["bid"]
                    self.last_ask = price_data["ask"]

                    if self.in_trade:
                        await self.process_active_trade()

            except Exception as e:
                print(f"Error in trading loop: {e}")
                print("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def process_active_trade(self):
        """Process an active trade and check exit conditions"""
        current_price = Decimal(self.last_bid)  # TODO bid or avg
        pnl_pct = ((current_price - self.entry_price) / self.entry_price) * 100
        time_in_trade = (datetime.now() - self.trade_start_time).total_seconds()

        status = (
            f"Current price: {self.round_to_tick(current_price)} | "
            f"Entry price: {self.round_to_tick(self.entry_price)} | "
            f"P&L: {pnl_pct:+.2f}% | "
            f"Time in trade: {int(time_in_trade)}s"
        )
        print(f"\r{status.ljust(120)}", end="", flush=True)

        should_exit, reason = self.check_exit_conditions()
        if should_exit:
            print("\n")
            sell_success = False
            while not sell_success:
                sell_success = await self.sell_crypto(reason)
            if sell_success:
                print(f"Cooling down for {self.cooldown_seconds} seconds...")
                await asyncio.sleep(self.cooldown_seconds)
                await self.buy_crypto(None)

    def check_exit_conditions(self):
        """Check if we should exit the trade based on our strategy"""
        current_pct_change = (
            (Decimal(self.last_bid) - self.entry_price) / self.entry_price
        ) * 100  # TODO bid or avg

        if current_pct_change <= -self.stop_loss_pct:
            return True, "STOP LOSS"
        if current_pct_change >= self.take_profit_pct:
            return True, "TAKE PROFIT"

        time_in_trade = (datetime.now() - self.trade_start_time).total_seconds()
        if time_in_trade >= self.timeout_seconds:
            return True, "TIMEOUT"
        return False, None

    async def buy_crypto(self, websocket):
        """Execute crypto purchase"""
        # Get current price data
        if not websocket:
            websocket = await self.market_connector.connect_price_stream()

        price_data = await self._get_price_data(websocket)

        crypto_amount = self.amount / Decimal(price_data["ask"])
        crypto_amount_str = self.adjust_amount(crypto_amount)

        # Execute order and handle result
        order_result = await self._execute_order(crypto_amount_str, "BUY")
        self._update_balance(order_result, "BUY")
        return order_result

    async def sell_crypto(self, reason) -> bool:
        """Execute crypto sale"""
        # Execute the order
        order_result = await self._execute_order(self.current_position, "SELL")
        self._update_balance(order_result, "SELL")
        if order_result["success"]:
            self._log_trade_results(order_result, reason)
            self._reset_trading_state()
            return True
        else:
            print(f"Sell order failed: {order_result['error']}")
            return False

    async def _get_price_data(self, websocket):
        """Get current price data from websocket"""
        message = await websocket.recv()
        data = json.loads(message)
        return self.market_connector.process_price_update(data)

    def round_to_tick(self, price: Decimal) -> Decimal:
        return round(
            round(price / Decimal(self.tick_size)) * Decimal(self.tick_size),
            len(str(self.tick_size).split(".")[-1]),
        )

    async def _execute_order(self, amount, side):
        """Execute market order and handle the result"""
        order_result = await self.trade_connector.execute_market_order(
            amount=amount, side=side
        )
        if order_result["success"] and side == "BUY":
            self._update_trade_state(order_result, amount)
            self._log_buy_confirmation(order_result, amount)
        elif not order_result["success"]:
            print(f"{side} order failed: {order_result['error']}")

        return order_result

    def _log_buy_confirmation(self, order_result, amount):
        """Log buy order confirmation details"""
        timeout_trade = (
            self.trade_start_time + timedelta(seconds=self.timeout_seconds)
        ).strftime("%H:%M:%S")
        stop_loss_price = order_result["price"] * (1 - self.stop_loss_pct / 100)
        stop_loss_price = self.round_to_tick(stop_loss_price)
        take_profit_price = order_result["price"] * (1 + self.take_profit_pct / 100)
        take_profit_price = self.round_to_tick(take_profit_price)
        print(f"\n=== BUY ORDER EXECUTED at {datetime.now()} ===")
        print(f"Bought {amount} {self.symbol} at {self.entry_price}")
        print(f"Order ID: {order_result['order_id']}")
        print(f"Investment: {self.amount} USDT")
        print(f"Setting stop loss at: {stop_loss_price}")
        print(f"Setting take profit at: {take_profit_price}")
        print(f"Trade timeout at: {timeout_trade}\n")
        log = {
            "order_id": order_result["order_id"],
            "symbol": self.symbol,
            "side": "BUY",
            "base": self.base_symbol,
            "quote": self.quote_symbol,
            "amount": "{0:f}".format(order_result["amount"]),
            "price": "{0:f}".format(self.entry_price),
            "pnl": "0",
            "pnl_pct": "0",
            "base_balance": "{0:f}".format(self.base_balance),
            "quote_balance": "{0:f}".format(self.quote_balance),
        }
        logger.info(log)

    def _log_trade_results(self, order_result, reason):
        """Log sell order results including profit/loss"""
        sell_price = order_result["price"]
        pnl = (sell_price - self.entry_price) * Decimal(self.current_position)
        pnl_pct = ((sell_price - self.entry_price) / self.entry_price) * 100
        self.total_profit += pnl
        print(f"\n=== SELL ORDER EXECUTED at {datetime.now()} ===")
        print(f"Reason: {reason}")
        print(f"Order ID: {order_result['order_id']}")
        print(f"Sold {self.current_position} {self.symbol} at {sell_price:.8f}")
        print(f"Entry price: {self.entry_price:.8f}")
        print(f"P&L: {pnl:.2f} USDT ({pnl_pct:.2f}%)")
        print(
            f"Balance: {self.base_balance} {self.base_symbol} | {self.quote_balance} {self.quote_symbol}"
        )
        print(f"Total profit: {self.total_profit:.2f} USDT")
        log = {
            "order_id": order_result["order_id"],
            "symbol": self.symbol,
            "side": "SELL",
            "base": self.base_symbol,
            "quote": self.quote_symbol,
            "amount": "{0:f}".format(order_result["amount"]),
            "price": "{0:f}".format(sell_price),
            "pnl": "{0:f}".format(pnl),
            "pnl_pct": "{0:f}".format(pnl_pct),
            "base_balance": "{0:f}".format(self.base_balance),
            "quote_balance": "{0:f}".format(self.quote_balance),
        }
        logger.info(log)

    def _update_trade_state(self, order_result, amount):
        """Update trading state after successful buy"""
        self.in_trade = True
        self.entry_price = order_result["price"]
        self.trade_start_time = datetime.now()
        self.current_position = amount

    def _reset_trading_state(self):
        """Reset trading state after selling"""
        self.in_trade = False
        self.current_position = 0
        self.entry_price = 0
        self.trade_start_time = None


if __name__ == "__main__":
    args = parse_arguments()
    bot = BinanceTradingBot(
        symbol=args.symbol,
        amount=Decimal(args.amount),
        stop_loss_pct=Decimal(args.stop_loss),
        take_profit_pct=Decimal(args.take_profit),
        timeout_seconds=args.timeout,
        cooldown_seconds=args.cooldown,
    )
    asyncio.run(bot.start_trading())
