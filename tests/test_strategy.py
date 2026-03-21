from __future__ import annotations

from datetime import date

import pytest
from wedge.strategy.ladder import evaluate_ladder
from wedge.strategy.models import EdgeSignal
from wedge.strategy.portfolio import allocate


def _signal(temp_f: int, edge: float, odds: float, p_market: float = 0.10, side: str = "buy") -> EdgeSignal:
    if side == "sell":
        p_model = p_market - edge
    else:
        p_model = p_market + edge
    return EdgeSignal(
        city="NYC",
        date=date(2026, 7, 1),
        temp_value=temp_f,
        temp_unit="F",
        token_id=f"tok_{temp_f}",
        p_model=p_model,
        p_market=p_market,
        edge=edge,
        odds=odds,
        side=side,
    )


class TestLadder:
    def test_filters_by_threshold(self):
        signals = [
            _signal(78, edge=0.10, odds=5),
            _signal(79, edge=0.03, odds=5),
        ]
        positions = evaluate_ladder(signals, budget=700, edge_threshold=0.05)
        assert len(positions) == 1
        assert positions[0].bucket.temp_value == 78

    def test_empty_signals(self):
        positions = evaluate_ladder([], budget=700)
        assert positions == []

    def test_respects_budget(self):
        signals = [_signal(i, edge=0.10, odds=5) for i in range(70, 80)]
        positions = evaluate_ladder(signals, budget=100)
        total = sum(position.size for position in positions)
        assert total <= 100

    def test_all_positions_are_ladder(self):
        signals = [_signal(78, edge=0.10, odds=5)]
        positions = evaluate_ladder(signals, budget=700)
        assert all(position.strategy == "ladder" for position in positions)

    def test_short_signal_creates_sell_position(self):
        signals = [_signal(78, edge=0.10, odds=5, side="sell")]
        positions = evaluate_ladder(signals, budget=700)
        assert len(positions) == 1
        assert positions[0].side == "sell"
        # Entry price for short = 1 - p_market = 0.90
        assert positions[0].entry_price == pytest.approx(0.90)

    def test_mixed_long_short_positions(self):
        signals = [
            _signal(78, edge=0.10, odds=5, side="buy"),
            _signal(79, edge=0.12, odds=5, side="sell"),
        ]
        positions = evaluate_ladder(signals, budget=700)
        assert len(positions) == 2
        sides = {p.side for p in positions}
        assert sides == {"buy", "sell"}

class TestPortfolio:
    def test_default_allocation(self):
        ladder, reserve, cash = allocate(1000)
        assert abs(ladder - 900) < 1e-9
        assert abs(reserve - 0) < 1e-9
        assert abs(cash - 100) < 1e-9

    def test_custom_allocation(self):
        ladder, reserve, cash = allocate(1000, ladder_pct=0.60)
        assert abs(ladder - 600) < 1e-9
        assert abs(reserve - 0) < 1e-9
        assert abs(cash - 400) < 1e-9

    def test_sums_to_bankroll(self):
        ladder, reserve, cash = allocate(1000)
        assert abs(ladder + reserve + cash - 1000) < 1e-9
