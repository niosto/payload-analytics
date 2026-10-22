"""Publishes USD exchange rates to the fx.rates topic.

The free provider (open.er-api.com by default) refreshes its rates about once a
day, so the API is only fetched every FX_FETCH_SECONDS. In between, the last
fetched rates are republished every FX_PUBLISH_SECONDS as ticks, so the
streaming join always has a recent rate inside its time window.
`provider_updated_at` carries the provider's own timestamp; that is the real
freshness of the rate, while `event_time` is when the tick was published.

One event per currency, keyed by currency:
    {
      "event_id": "...", "event_time": "2026-10-15T22:00:00.000+00:00",
      "currency": "COP", "base": "USD",
      "units_per_usd": 4012.5, "usd_per_unit": 0.000249,
      "provider": "open.er-api.com",
      "provider_updated_at": "2026-10-15T00:02:31+00:00"
    }

usd_per_unit matches the column of seed_fx_rates, so the batch models can swap
the seed for this stream without changing their math.
"""

import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

from common import JsonProducer, StopFlag, env, utc_now_iso

TOPIC = "fx.rates"
API_URL = env("FX_API_URL", "https://open.er-api.com/v6/latest/USD")
CURRENCIES = [
    c.strip().upper()
    for c in env("FX_CURRENCIES", "USD,EUR,COP,ARS,MXN,CLP,PEN,BRL,UYU").split(",")
    if c.strip()
]
FETCH_SECONDS = int(env("FX_FETCH_SECONDS", "600"))
PUBLISH_SECONDS = int(env("FX_PUBLISH_SECONDS", "10"))
MAX_BACKOFF_SECONDS = 300


def fetch_rates():
    """Return ({currency: units_per_usd}, provider_updated_at ISO)."""
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    body = response.json()
    if body.get("result") != "success":
        raise ValueError(f"provider returned result={body.get('result')}")
    if body.get("base_code", "USD") != "USD":
        raise ValueError(f"expected base USD, got {body.get('base_code')}")

    rates = body["rates"]
    missing = [c for c in CURRENCIES if c not in rates]
    if missing:
        raise ValueError(f"provider has no rate for {', '.join(missing)}")

    updated = datetime.fromtimestamp(body["time_last_update_unix"], timezone.utc)
    return {c: float(rates[c]) for c in CURRENCIES}, updated.isoformat()


def main():
    producer = JsonProducer("fx_producer")
    log = producer.log
    stop = StopFlag()
    provider = urlparse(API_URL).hostname

    rates, provider_updated_at = None, None
    next_fetch = 0.0
    backoff = 5

    log.info("currencies=%s fetch=%ss publish=%ss", CURRENCIES, FETCH_SECONDS, PUBLISH_SECONDS)

    while not stop.stopped:
        now = time.monotonic()
        if now >= next_fetch:
            try:
                rates, provider_updated_at = fetch_rates()
                next_fetch = now + FETCH_SECONDS
                backoff = 5
                log.info("fetched %d rates, provider_updated_at=%s", len(rates), provider_updated_at)
            except (requests.RequestException, ValueError, KeyError) as exc:
                # Keep publishing the last good rates, if any; never invent one.
                next_fetch = now + backoff
                log.warning("fetch failed (%s), retrying in %ss", exc, backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

        if rates:
            event_time = utc_now_iso()
            for currency, units_per_usd in rates.items():
                producer.send(
                    TOPIC,
                    key=currency,
                    value={
                        "event_id": str(uuid.uuid4()),
                        "event_time": event_time,
                        "currency": currency,
                        "base": "USD",
                        "units_per_usd": units_per_usd,
                        "usd_per_unit": 1.0 / units_per_usd,
                        "provider": provider,
                        "provider_updated_at": provider_updated_at,
                    },
                )
            log.debug("published %d ticks", len(rates))

        # Sleep in short steps so SIGTERM is honored quickly.
        deadline = time.monotonic() + PUBLISH_SECONDS
        while not stop.stopped and time.monotonic() < deadline:
            time.sleep(0.5)

    producer.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
