import json
import random
import re
from datetime import datetime, timezone

import pytest

import transactions_simulator as sim
from conftest import FIXTURES

TINY = FIXTURES / "tiny_customers.json"

# silver.stg_transactions columns the stream fills (all but load_timestamp),
# plus the event metadata the Spark streaming job reads.
STG_TRANSACTION_FIELDS = {
    "customer_id", "transaction_id", "account_id", "date", "amount", "currency",
    "type", "category", "merchant", "channel", "status", "description",
}
EVENT_FIELDS = {"event_id", "event_time", "source"}


@pytest.mark.parametrize(
    "value,expected",
    [(None, None), ("x", "x"), (12.5, "12.5"), (0, "0"), (True, "True"), ("$33.10", "$33.10")],
)
def test_as_text(value, expected):
    assert sim.as_text(value) == expected


def test_load_source_accounts_and_templates():
    accounts, templates = sim.load_source(TINY)

    # Duplicated customers in the file are not deduplicated here.
    assert ("CUST-0000001", "ACC-000000000001", "USD") in accounts
    assert ("CUST-0000001", "ACC-000000000002", " clp ") in accounts
    assert ("CUST-0000002", "ACC-000000000003", "COP") in accounts
    assert len(accounts) == 4

    # Templates are keyed by the normalized currency.
    assert set(templates) == {"USD", "CLP", "COP"}
    assert len(templates["USD"]) == 2

    clp = templates["CLP"][0]
    assert set(clp) == set(sim.TEMPLATE_FIELDS)
    # Dirt is kept on purpose; numbers become text, nulls stay null.
    assert clp["amount"] == "$33593.14"
    assert clp["category"] == "N/A"
    assert templates["USD"][0]["amount"] == "14039.03"
    assert templates["USD"][0]["merchant"] is None


def test_load_source_rejects_file_without_accounts(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps([{"customer_id": "CUST-1", "accounts": [], "transactions": []}]))
    with pytest.raises(ValueError, match="no accounts"):
        sim.load_source(path)


def _parses_with_a_source_mask(value):
    for mask, _ in sim.DATE_FORMATS:
        try:
            datetime.strptime(value, mask)
            return True
        except ValueError:
            continue
    return False


def test_build_event_contract():
    accounts, templates = sim.load_source(TINY)
    all_templates = [t for pool in templates.values() for t in pool]
    rng = random.Random(7)

    for _ in range(50):
        event = sim.build_event(rng, accounts, templates, all_templates)

        assert set(event) == STG_TRANSACTION_FIELDS | EVENT_FIELDS
        assert event["source"] == "simulator"
        assert re.fullmatch(r"LTX-[0-9A-F]{12}", event["transaction_id"])
        assert (event["customer_id"], event["account_id"], event["currency"]) in accounts

        # The template comes from a real transaction in the account's currency.
        template = {k: event[k] for k in sim.TEMPLATE_FIELDS}
        assert template in templates[event["currency"].strip().upper()]

        assert _parses_with_a_source_mask(event["date"])
        event_time = datetime.fromisoformat(event["event_time"])
        assert event_time.tzinfo is not None
        assert abs((datetime.now(timezone.utc) - event_time).total_seconds()) < 60


def test_build_event_falls_back_to_any_template_for_unknown_currency():
    accounts = [("CUST-9", "ACC-9", "EUR")]
    templates = {"USD": [{k: f"usd-{k}" for k in sim.TEMPLATE_FIELDS}]}
    all_templates = templates["USD"]

    event = sim.build_event(random.Random(1), accounts, templates, all_templates)

    assert event["currency"] == "EUR"
    assert event["amount"] == "usd-amount"


def test_build_event_ids_are_unique():
    accounts, templates = sim.load_source(TINY)
    all_templates = [t for pool in templates.values() for t in pool]
    rng = random.Random(3)
    events = [sim.build_event(rng, accounts, templates, all_templates) for _ in range(200)]
    assert len({e["transaction_id"] for e in events}) == 200
    assert len({e["event_id"] for e in events}) == 200
