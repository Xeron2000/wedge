from __future__ import annotations

from datetime import date

from wedge.strategy.ladder import evaluate_ladder
from wedge.strategy.models import EdgeSignal
from wedge.strategy.portfolio import allocate
from wedge.strategy.tail import evaluate_tail


def _signal(temp_f: int, edge: float, odds: float, p_market: float = 0.10) -> EdgeSignal:
    return EdgeSignal(
        city="NYC",
        date=date(2026, 7, 1),
        temp_f=temp_f,
        token_id=f"tok_{temp_f}",
        p_model=p_market + edge,
        p_market=p_market,
        edge=edge,
        odds=odds,
    )


class TestLadder:
    def test_filters_by_threshold(self):
        signals = [
            _signal(78, edge=0.10, odds=5),
            _signal(79, edge=0.03, odds=5),  # below threshold
        ]
        positions = evaluate_ladder(signals, budget=700, edge_threshold=0.05)
        assert len(positions) == 1
        assert positions[0].bucket.temp_f == 78

    def test_empty_signals(self):
        positions = evaluate_ladder([], budget=700)
        assert positions == []

    def test_respects_budget(self):
        signals = [_signal(i, edge=0.10, odds=5) for i in range(70, 80)]
        positions = evaluate_ladder(signals, budget=100)
        total = sum(p.size for p in positions)
        assert total <= 100

    def test_all_positions_are_ladder(self):
        signals = [_signal(78, edge=0.10, odds=5)]
        positions = evaluate_ladder(signals, budget=700)
        assert all(p.strategy == "ladder" for p in positions)


class TestTail:
    def test_filters_by_odds_and_edge(self):
        signals = [
            _signal(95, edge=0.10, odds=30, p_market=0.03),  # qualifies
            _signal(96, edge=0.10, odds=5, p_market=0.15),  # odds too low
            _signal(97, edge=0.03, odds=30, p_market=0.03),  # edge too low
        ]
        positions = evaluate_tail(signals, budget=200, edge_threshold=0.08, min_odds=10)
        assert len(positions) == 1
        assert positions[0].bucket.temp_f == 95

    def test_empty_signals(self):
        positions = evaluate_tail([], budget=200)
        assert positions == []

    def test_all_positions_are_tail(self):
        signals = [_signal(95, edge=0.10, odds=30, p_market=0.03)]
        positions = evaluate_tail(signals, budget=200)
        assert all(p.strategy == "tail" for p in positions)


class TestPortfolio:
    def test_default_allocation(self):
        ladder, tail, cash = allocate(1000)
        assert abs(ladder - 700) < 1e-9
        assert abs(tail - 200) < 1e-9
        assert abs(cash - 100) < 1e-9

    def test_custom_allocation(self):
        ladder, tail, cash = allocate(1000, ladder_pct=0.60, tail_pct=0.30)
        assert abs(ladder - 600) < 1e-9
        assert abs(tail - 300) < 1e-9
        assert abs(cash - 100) < 1e-9

    def test_sums_to_bankroll(self):
        ladder, tail, cash = allocate(1000)
        assert abs(ladder + tail + cash - 1000) < 1e-9
