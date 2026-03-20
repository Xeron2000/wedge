from __future__ import annotations

from typing import Any, Protocol

from wedge.log import get_logger

log = get_logger("notify")


class Notifier(Protocol):
    async def send(self, message: str) -> None: ...


class StdoutNotifier:
    """Default notifier — logs to stdout."""

    async def send(self, message: str) -> None:
        log.info("notification", message=message)


def format_pipeline_summary(
    *,
    mode: str,
    cities: list[str],
    edges_found: int,
    orders_placed: int,
    balance: float,
    signals: list[dict[str, Any]] | None = None,
) -> str:
    lines = [
        f"[Pipeline Complete] ({mode})",
        "━━━━━━━━━━━━━━━",
        f"Cities: {', '.join(cities)}",
        f"Edges found: {edges_found}",
        f"Orders placed: {orders_placed}",
        f"Balance: ${balance:.2f}",
    ]
    if signals:
        lines.append("\nTop signals:")
        for signal in signals[:5]:
            unit = signal.get("temp_unit", "F")
            lines.append(
                f"  {signal['city']} {signal['temp_value']}°{unit}: "
                f"edge {signal['edge']:.1%}, ${signal['size']:.2f}"
            )
    return "\n".join(lines)


def format_alert(reason: str, details: str = "") -> str:
    lines = [f"[ALERT] {reason}"]
    if details:
        lines.append(details)
    return "\n".join(lines)


def format_stats(
    *,
    days: int,
    total_trades: int,
    wins: int,
    win_rate: float,
    total_pnl: float,
    brier: float | None,
) -> str:
    brier_str = f"{brier:.4f}" if brier is not None else "N/A"
    brier_status = ""
    if brier is not None:
        brier_status = " (GOOD)" if brier < 0.20 else " (OK)" if brier < 0.25 else " (PAUSED)"
    return "\n".join(
        [
            f"[Stats] ({days} days)",
            "━━━━━━━━━━━━━━━",
            f"Trades: {total_trades}",
            f"Wins: {wins} ({win_rate:.1%})" if total_trades > 0 else "Wins: N/A",
            f"P&L: ${total_pnl:.2f}",
            f"Brier: {brier_str}{brier_status}",
        ]
    )


def format_positions(positions: list[dict[str, Any]]) -> str:
    """Format open positions for display."""
    if not positions:
        return "[Positions]\nNo open positions"

    lines = [f"[Positions] ({len(positions)} open)"]
    lines.append("━━━━━━━━━━━━━━━")

    total_invested = sum(position["size"] for position in positions)

    from collections import defaultdict

    by_date = defaultdict(list)
    for position in positions:
        by_date[position["date"]].append(position)

    for position_date in sorted(by_date.keys()):
        date_positions = by_date[position_date]
        date_invested = sum(position["size"] for position in date_positions)
        lines.append(f"\n{position_date} (${date_invested:.0f})")

        for position in sorted(date_positions, key=lambda item: item["temp_value"]):
            edge_pct = position["edge"] * 100
            unit = position.get("temp_unit", "F")
            lines.append(
                f"  {position['city']} {position['temp_value']}°{unit}: "
                f"${position['size']:.0f} @{position['entry_price']:.2f} ({edge_pct:.0f}% edge)"
            )

    lines.append(f"\nTotal invested: ${total_invested:.2f}")
    return "\n".join(lines)


def format_exit_notification(
    city: str,
    date: str,
    temp_f: int | float,
    exit_reason: str,
    pnl: float,
    p_model: float,
    entry_price: float,
) -> str:
    reason_label = "Stop Loss" if exit_reason == "stop_loss" else "Take Profit"
    pnl_sign = "+" if pnl >= 0 else ""
    emoji = "🔴" if exit_reason == "stop_loss" else "🟢"
    return (
        f"{emoji} Early Exit [{reason_label}]\n"
        f"City: {city}\n"
        f"Date: {date}  Temp: {temp_f}°F\n"
        f"p_model: {p_model:.3f}  entry: {entry_price:.3f}\n"
        f"PnL: {pnl_sign}{pnl:.4f}\n"
        f"Reason: {exit_reason}"
    )
