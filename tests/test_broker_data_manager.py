import broker_data_manager as bdm


def test_resolve_fetches_dhan_master_when_dhan_is_not_preferred(monkeypatch):
    calls = []

    monkeypatch.setattr(bdm.PreferredBrokers, "names", lambda: ["fyers"])
    monkeypatch.setattr(
        bdm.DhanSecurityMaster,
        "get_security_id",
        lambda self, symbol, exchange, instrument_name: calls.append(
            (symbol, exchange, instrument_name)
        )
        or 12345,
    )
    monkeypatch.setattr(bdm.FyersSymbolMaster, "validate_equity_symbol", lambda self, symbol: True)

    instrument = bdm.InstrumentResolver.resolve("SBIN")

    assert instrument.security_id == 12345
    assert calls == [("SBIN", "NSE", "EQUITY")]


def test_resolve_fetches_fyers_master_when_fyers_is_not_preferred(monkeypatch):
    calls = []

    monkeypatch.setattr(bdm.PreferredBrokers, "names", lambda: ["dhan"])
    monkeypatch.setattr(bdm.DhanSecurityMaster, "get_security_id", lambda *args: 12345)
    monkeypatch.setattr(
        bdm.FyersSymbolMaster,
        "validate_equity_symbol",
        lambda self, symbol: calls.append(symbol) or True,
    )

    instrument = bdm.InstrumentResolver.resolve("SBIN")

    assert instrument.symbol == "NSE:SBIN-EQ"
    assert calls == ["NSE:SBIN-EQ"]