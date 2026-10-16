"""Publishes real market FX rates from Binance to the fx.rates topic.

Source: Binance public market-data WebSocket (data-stream.binance.vision, no
API key), aggregated trades for USDT pairs in LATAM currencies plus EUR/USDT.
Each trade is a real executed price with the exchange's own timestamp.

USDT is treated as USD (a dollar stablecoin), so USDTBRL = 5.10 means
1 USD = 5.10 BRL. CLP, PEN and UYU have no pair on Binance; fx_producer.py
keeps publishing those from the daily API.

Publishing:
  - At most one tick per currency every FX_MARKET_PUBLISH_MS, with the latest
    trade price. Trade bursts are coalesced, not queued.
  - A currency with no new trade is republished every FX_MARKET_HEARTBEAT_SECONDS
    with its last price, so the streaming join always has a tick in its window.
    Thin pairs (MXN, COP) can go minutes without a trade.
  - On start, the last trade of each pair is read over REST, so every currency
    has a rate before the first WebSocket trade arrives.

The event matches fx_producer.py. provider_updated_at is the exchange time of
the trade behind the price; event_time is when the tick was published. On a
heartbeat the two differ, which is the real age of the rate.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone

import requests
import websockets

from common import JsonProducer, StopFlag, env, utc_now_iso

TOPIC = "fx.rates"
PROVIDER = "binance"
WS_BASE = env("FX_MARKET_WS_URL", "wss://data-stream.binance.vision:443/stream")
REST_BASE = env("FX_MARKET_REST_URL", "https://data-api.binance.vision/api/v3")
PUBLISH_MS = int(env("FX_MARKET_PUBLISH_MS", "1000"))
HEARTBEAT_SECONDS = int(env("FX_MARKET_HEARTBEAT_SECONDS", "10"))
MAX_BACKOFF_SECONDS = 60

# currency:SYMBOL:direction. "inverse" means the pair is priced in the local
# currency per USDT (USDTBRL); "direct" means USDT per unit (EURUSDT).
SYMBOLS_SPEC = env(
    "FX_MARKET_SYMBOLS",
    "BRL:USDTBRL:inverse,ARS:USDTARS:inverse,MXN:USDTMXN:inverse,"
    "COP:USDTCOP:inverse,EUR:EURUSDT:direct",
)


def parse_symbols(spec):
    pairs = {}
    for item in spec.split(","):
        if not item.strip():
            continue
        currency, symbol, direction = (part.strip() for part in item.split(":"))
        if direction not in ("inverse", "direct"):
            raise ValueError(f"direction must be inverse or direct: {item}")
        pairs[symbol.upper()] = (currency.upper(), direction)
    if not pairs:
        raise ValueError("FX_MARKET_SYMBOLS is empty")
    return pairs


PAIRS = parse_symbols(SYMBOLS_SPEC)


def to_rates(price, direction):
    """Return (units_per_usd, usd_per_unit) for a pair price."""
    if direction == "inverse":
        return price, 1.0 / price
    return 1.0 / price, price


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="milliseconds")


class MarketState:
    """Latest trade per symbol and what was last published."""

    def __init__(self):
        self.latest = {}  # symbol -> (price, trade_time_ms)
        self.published_trade_ms = {}  # symbol -> trade_time_ms last published
        self.published_at = {}  # symbol -> monotonic time of last publish

    def update(self, symbol, price, trade_ms):
        current = self.latest.get(symbol)
        if current is None or trade_ms >= current[1]:
            self.latest[symbol] = (price, trade_ms)

    def due(self, now):
        """Symbols with a new trade, or silent for longer than the heartbeat."""
        for symbol, (_, trade_ms) in self.latest.items():
            fresh = trade_ms != self.published_trade_ms.get(symbol)
            stale = now - self.published_at.get(symbol, 0) >= HEARTBEAT_SECONDS
            if fresh or stale:
                yield symbol


def seed_from_rest(state, log):
    for symbol in PAIRS:
        try:
            response = requests.get(
                f"{REST_BASE}/trades", params={"symbol": symbol, "limit": 1}, timeout=10
            )
            response.raise_for_status()
            trade = response.json()[-1]
            state.update(symbol, float(trade["price"]), int(trade["time"]))
            log.info("seeded %s price=%s trade_time=%s", symbol, trade["price"], ms_to_iso(trade["time"]))
        except (requests.RequestException, ValueError, KeyError, IndexError) as exc:
            log.warning("could not seed %s from REST (%s); waiting for the stream", symbol, exc)


async def publish_loop(producer, state, stop):
    stats = {"ticks": 0, "heartbeats": 0}
    last_report = time.monotonic()
    while not stop.stopped:
        now = time.monotonic()
        event_time = utc_now_iso()
        for symbol in list(state.due(now)):
            price, trade_ms = state.latest[symbol]
            currency, direction = PAIRS[symbol]
            units_per_usd, usd_per_unit = to_rates(price, direction)
            producer.send(
                TOPIC,
                key=currency,
                value={
                    "event_id": str(uuid.uuid4()),
                    "event_time": event_time,
                    "currency": currency,
                    "base": "USD",
                    "units_per_usd": units_per_usd,
                    "usd_per_unit": usd_per_unit,
                    "provider": PROVIDER,
                    "provider_updated_at": ms_to_iso(trade_ms),
                },
            )
            is_heartbeat = trade_ms == state.published_trade_ms.get(symbol)
            stats["heartbeats" if is_heartbeat else "ticks"] += 1
            state.published_trade_ms[symbol] = trade_ms
            state.published_at[symbol] = now

        if now - last_report >= 60:
            producer.log.info(
                "last 60s: trade_ticks=%d heartbeats=%d failed=%d",
                stats["ticks"], stats["heartbeats"], producer.failed,
            )
            stats = {"ticks": 0, "heartbeats": 0}
            last_report = now
        await asyncio.sleep(PUBLISH_MS / 1000)


async def stream_loop(state, stop, log):
    streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in PAIRS)
    url = f"{WS_BASE}?streams={streams}"
    backoff = 1
    while not stop.stopped:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info("connected %s", url)
                backoff = 1
                while not stop.stopped:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(raw).get("data", {})
                    symbol = data.get("s")
                    if symbol in PAIRS and data.get("p"):
                        price = float(data["p"])
                        if price > 0:
                            state.update(symbol, price, int(data["T"]))
        except (OSError, websockets.WebSocketException, json.JSONDecodeError) as exc:
            # Heartbeats keep publishing the last real price meanwhile; its
            # provider_updated_at shows how old it is.
            log.warning("stream error (%s), reconnecting in %ss", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)


async def run():
    producer = JsonProducer("fx_market_producer")
    log = producer.log
    stop = StopFlag()
    state = MarketState()

    log.info(
        "pairs=%s publish=%sms heartbeat=%ss",
        {s: c for s, (c, _) in PAIRS.items()}, PUBLISH_MS, HEARTBEAT_SECONDS,
    )
    seed_from_rest(state, log)

    await asyncio.gather(publish_loop(producer, state, stop), stream_loop(state, stop, log))
    producer.close()
    log.info("stopped")


if __name__ == "__main__":
    asyncio.run(run())
