from __future__ import annotations

from typing import Protocol

from wedge.log import get_logger

log = get_logger("notify")


class Notifier(Protocol):
    async def send(self, message: str) -> None: ...


class StdoutNotifier:
    """Default notifier — logs to stdout."""

    async def send(self, message: str) -> None:
        log.info("notification", message=message)


class TelegramNotifier:
    """Send messages via Telegram Bot API."""

    def __init__(self, token: str, chat_id: str) -> None:
        self._token = token
        self._chat_id = chat_id
        self._bot: object | None = None

    async def _ensure_bot(self) -> None:
        if self._bot is None:
            from telegram import Bot
            self._bot = Bot(token=self._token)

    async def send(self, message: str) -> None:
        try:
            await self._ensure_bot()
            await self._bot.send_message(  # type: ignore[union-attr]
                chat_id=self._chat_id,
                text=message,
            )
        except Exception as e:
            log.error("telegram_send_failed", error=str(e))


def create_notifier(token: str, chat_id: str) -> Notifier:
    if token and chat_id:
        return TelegramNotifier(token, chat_id)
    return StdoutNotifier()


def format_pipeline_summary(
    *,
    mode: str,
    cities: list[str],
    edges_found: int,
    orders_placed: int,
    balance: float,
    signals: list[dict] | None = None,
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
        for s in signals[:5]:
            unit = s.get('temp_unit', 'F')
            lines.append(
                f"  {s['city']} {s['temp_value']}°{unit}: edge {s['edge']:.1%}, ${s['size']:.2f}"
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
    return "\n".join([
        f"[Stats] ({days} days)",
        "━━━━━━━━━━━━━━━",
        f"Trades: {total_trades}",
        f"Wins: {wins} ({win_rate:.1%})" if total_trades > 0 else "Wins: N/A",
        f"P&L: ${total_pnl:.2f}",
        f"Brier: {brier_str}{brier_status}",
    ])


def format_positions(positions: list[dict]) -> str:
    """Format open positions for display."""
    if not positions:
        return "[Positions]\nNo open positions"

    lines = [f"[Positions] ({len(positions)} open)"]
    lines.append("━━━━━━━━━━━━━━━")

    total_invested = sum(p["size"] for p in positions)

    # Group by date
    from collections import defaultdict
    by_date = defaultdict(list)
    for p in positions:
        by_date[p["date"]].append(p)

    for date in sorted(by_date.keys()):
        date_positions = by_date[date]
        date_invested = sum(p["size"] for p in date_positions)
        lines.append(f"\n{date} (${date_invested:.0f})")

        for p in sorted(date_positions, key=lambda x: x["temp_value"]):
            edge_pct = p["edge"] * 100
            unit = p.get('temp_unit', 'F')
            lines.append(
                f"  {p['city']} {p['temp_value']}°{unit}: ${p['size']:.0f} @{p['entry_price']:.2f} ({edge_pct:.0f}% edge)"
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
    """Format a Telegram notification for an early position exit."""
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

