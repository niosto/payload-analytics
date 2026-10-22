"""Publishes simulated live transactions to the fintech.live_transactions topic.

Customers and accounts come from the same sample JSON the batch pipeline loads,
so every event references a customer_id and account_id that exist in Silver and
Gold. Each event copies amount, type, category, merchant, channel, status and
description from a real transaction in the account's currency. That keeps the
source's dirt as-is (Spanish and upper-case labels, "$123.4" amounts, "N/A"
nulls), so dbt's cleaning macros are exercised the same way as in batch.

The date is the event's own day, written in one of the four source formats with
the source's frequencies. transaction_id uses the LTX- prefix so live rows never
collide with batch TXN- ids.

Fields match silver.stg_transactions, all strings, plus event metadata:
    {
      "event_id": "...", "event_time": "2026-10-15T22:00:00.000+00:00",
      "source": "simulator",
      "customer_id": "CUST-0004655", "transaction_id": "LTX-1A2B3C4D5E6F",
      "account_id": "ACC-405300E15156", "date": "2026-10-15",
      "amount": "$33593.14", "currency": "USD", "type": "Withdrawal",
      "category": "insurance", "merchant": null, "channel": "pos",
      "status": "pending", "description": "..."
    }
"""

import json
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from common import JsonProducer, StopFlag, env, utc_now_iso

TOPIC = "fintech.live_transactions"
SOURCE_PATH = env("TX_SOURCE_PATH", "/data/raw/fintech_banking_dataset.json")
EVENTS_PER_SECOND = float(env("TX_EVENTS_PER_SECOND", "2"))
SEED = env("TX_SEED", None)
LOG_EVERY = 100

TEMPLATE_FIELDS = ("amount", "type", "category", "merchant", "channel", "status", "description")

# Date masks and weights as profiled in the source transactions (81.2% ISO,
# 6.3% / 6.3% / 6.2% for the rest). format_date in dbt reads all four.
DATE_FORMATS = (
    ("%Y-%m-%d", 812),
    ("%m-%d-%Y", 63),
    ("%d/%m/%Y", 63),
    ("%Y%m%d", 62),
)


def as_text(value):
    """Mirror the Spark StringType contract: numbers become text, null stays null."""
    return None if value is None else str(value)


def load_source(path):
    """Return (accounts, templates_by_currency) from the batch sample file."""
    with open(path, encoding="utf-8") as f:
        customers = json.load(f)

    accounts = []
    templates = defaultdict(list)
    for customer in customers:
        customer_id = customer.get("customer_id")
        for account in customer.get("accounts") or []:
            if customer_id and account.get("account_id") and account.get("currency"):
                accounts.append((customer_id, account["account_id"], account["currency"]))
        for tx in customer.get("transactions") or []:
            currency = tx.get("currency")
            if currency:
                templates[currency.strip().upper()].append(
                    {k: as_text(tx.get(k)) for k in TEMPLATE_FIELDS}
                )

    if not accounts or not templates:
        raise ValueError(f"no accounts or transactions found in {path}")
    return accounts, templates


def build_event(rng, accounts, templates, all_templates):
    customer_id, account_id, currency = rng.choice(accounts)
    pool = templates.get(currency.strip().upper()) or all_templates
    template = rng.choice(pool)

    masks, weights = zip(*DATE_FORMATS)
    date_mask = rng.choices(masks, weights=weights)[0]

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": utc_now_iso(),
        "source": "simulator",
        "customer_id": customer_id,
        "transaction_id": f"LTX-{uuid.uuid4().hex[:12].upper()}",
        "account_id": account_id,
        "date": datetime.now(timezone.utc).strftime(date_mask),
        "currency": currency,
        **template,
    }


def main():
    producer = JsonProducer("transactions_simulator")
    log = producer.log
    stop = StopFlag()
    rng = random.Random(SEED)

    accounts, templates = load_source(SOURCE_PATH)
    all_templates = [t for pool in templates.values() for t in pool]
    log.info(
        "loaded %d accounts, %d templates in %d currencies; rate=%s/s",
        len(accounts), len(all_templates), len(templates), EVENTS_PER_SECOND,
    )

    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    next_send = time.monotonic()

    while not stop.stopped:
        event = build_event(rng, accounts, templates, all_templates)
        producer.send(TOPIC, key=event["customer_id"], value=event)
        sent += 1
        if sent % LOG_EVERY == 0:
            log.info("sent=%d failed=%d", sent, producer.failed)

        next_send += interval
        delay = next_send - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_send = time.monotonic()

    producer.close()
    log.info("stopped after %d events", sent)


if __name__ == "__main__":
    main()
