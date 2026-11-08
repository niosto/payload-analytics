import pytest

import fx_market_producer as fx


def test_parse_symbols_default_spec():
    pairs = fx.parse_symbols(fx.SYMBOLS_SPEC)
    assert pairs == {
        "USDTBRL": ("BRL", "inverse"),
        "USDTARS": ("ARS", "inverse"),
        "USDTMXN": ("MXN", "inverse"),
        "USDTCOP": ("COP", "inverse"),
        "EURUSDT": ("EUR", "direct"),
    }


def test_parse_symbols_normalizes_case_spaces_and_empty_items():
    pairs = fx.parse_symbols(" brl : usdtbrl : inverse ,, eur:eurusdt:direct,")
    assert pairs == {"USDTBRL": ("BRL", "inverse"), "EURUSDT": ("EUR", "direct")}


def test_parse_symbols_rejects_unknown_direction():
    with pytest.raises(ValueError, match="direction"):
        fx.parse_symbols("BRL:USDTBRL:sideways")


@pytest.mark.parametrize("spec", ["", " , ,"])
def test_parse_symbols_rejects_empty_spec(spec):
    with pytest.raises(ValueError, match="empty"):
        fx.parse_symbols(spec)


def test_parse_symbols_rejects_malformed_item():
    with pytest.raises(ValueError):
        fx.parse_symbols("BRL:USDTBRL")


def test_to_rates_inverse_pair_is_local_per_usd():
    units_per_usd, usd_per_unit = fx.to_rates(5.0, "inverse")
    assert units_per_usd == 5.0
    assert usd_per_unit == pytest.approx(0.2)


def test_to_rates_direct_pair_is_usd_per_unit():
    units_per_usd, usd_per_unit = fx.to_rates(1.08, "direct")
    assert usd_per_unit == 1.08
    assert units_per_usd == pytest.approx(1 / 1.08)


@pytest.mark.parametrize("price,direction", [(4000.0, "inverse"), (1.08, "direct"), (0.5, "inverse")])
def test_to_rates_are_reciprocal(price, direction):
    units_per_usd, usd_per_unit = fx.to_rates(price, direction)
    assert units_per_usd * usd_per_unit == pytest.approx(1.0)


def test_ms_to_iso_is_utc_with_milliseconds():
    assert fx.ms_to_iso(1_760_565_600_123) == "2025-10-15T22:00:00.123+00:00"


def test_market_state_update_keeps_latest_trade():
    state = fx.MarketState()
    state.update("USDTBRL", 5.0, 2_000)
    state.update("USDTBRL", 4.0, 1_000)  # older trade arriving late is ignored
    assert state.latest["USDTBRL"] == (5.0, 2_000)
    state.update("USDTBRL", 5.1, 2_000)  # same timestamp replaces
    assert state.latest["USDTBRL"] == (5.1, 2_000)


def test_market_state_due_fresh_trade_then_heartbeat():
    state = fx.MarketState()
    assert list(state.due(now=100.0)) == []

    state.update("USDTBRL", 5.0, 1_000)
    assert list(state.due(now=100.0)) == ["USDTBRL"]

    # Published: nothing new and inside the heartbeat window.
    state.published_trade_ms["USDTBRL"] = 1_000
    state.published_at["USDTBRL"] = 100.0
    assert list(state.due(now=100.0 + fx.HEARTBEAT_SECONDS - 0.1)) == []

    # Silent for the whole heartbeat: republish the last price.
    assert list(state.due(now=100.0 + fx.HEARTBEAT_SECONDS)) == ["USDTBRL"]

    # A new trade is due immediately, heartbeat or not.
    state.update("USDTBRL", 5.2, 1_500)
    assert list(state.due(now=100.5)) == ["USDTBRL"]


def test_market_state_due_only_lists_symbols_that_need_publishing():
    state = fx.MarketState()
    state.update("USDTBRL", 5.0, 1_000)
    state.update("EURUSDT", 1.08, 1_000)
    state.published_trade_ms.update({"USDTBRL": 1_000, "EURUSDT": 900})
    state.published_at.update({"USDTBRL": 50.0, "EURUSDT": 50.0})
    assert list(state.due(now=51.0)) == ["EURUSDT"]
