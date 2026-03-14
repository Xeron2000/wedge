from __future__ import annotations

from pathlib import Path

import aiosqlite

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    city TEXT NOT NULL,
    date TEXT NOT NULL,
    temp_f INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    entry_price REAL NOT NULL,
    size REAL NOT NULL,
    p_model REAL NOT NULL,
    p_market REAL NOT NULL,
    edge REAL NOT NULL,
    token_id TEXT,
    order_id TEXT,
    settled INTEGER DEFAULT 0,
    outcome REAL,
    pnl REAL,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, city, date, temp_f, strategy)
);

CREATE TABLE IF NOT EXISTS forecasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    city TEXT NOT NULL,
    date TEXT NOT NULL,
    temp_f INTEGER NOT NULL,
    p_model REAL NOT NULL,
    actual_temp_f INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bankroll_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    balance REAL NOT NULL,
    unrealized_pnl REAL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id);
CREATE INDEX IF NOT EXISTS idx_trades_city_date ON trades(city, date);
CREATE INDEX IF NOT EXISTS idx_forecasts_city_date ON forecasts(city, date);
CREATE INDEX IF NOT EXISTS idx_trades_settled ON trades(settled);
"""


class Database:
    def __init__(self, db_path: str = "wedge.db") -> None:
        self._path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._conn:
            raise RuntimeError("Database not connected")
        return self._conn

    async def insert_run(self, run_id: str, started_at: str) -> None:
        await self.conn.execute(
            "INSERT INTO runs (id, started_at, status) VALUES (?, ?, 'running')",
            (run_id, started_at),
        )
        await self.conn.commit()

    async def complete_run(self, run_id: str, completed_at: str, status: str = "completed") -> None:
        await self.conn.execute(
            "UPDATE runs SET completed_at = ?, status = ? WHERE id = ?",
            (completed_at, status, run_id),
        )
        await self.conn.commit()

    async def insert_trade(
        self,
        *,
        run_id: str,
        city: str,
        date: str,
        temp_f: int,
        strategy: str,
        entry_price: float,
        size: float,
        p_model: float,
        p_market: float,
        edge: float,
        token_id: str | None = None,
        order_id: str | None = None,
        created_at: str,
    ) -> bool:
        """Insert trade idempotently. Returns True if inserted, False if duplicate."""
        try:
            await self.conn.execute(
                """INSERT INTO trades
                   (run_id, city, date, temp_f, strategy, entry_price, size,
                    p_model, p_market, edge, token_id, order_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, city, date, temp_f, strategy, entry_price, size,
                 p_model, p_market, edge, token_id, order_id, created_at),
            )
            await self.conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def insert_forecast(
        self,
        *,
        run_id: str,
        city: str,
        date: str,
        temp_f: int,
        p_model: float,
        created_at: str,
    ) -> None:
        await self.conn.execute(
            """INSERT INTO forecasts (run_id, city, date, temp_f, p_model, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, city, date, temp_f, p_model, created_at),
        )
        await self.conn.commit()

    async def insert_bankroll_snapshot(
        self, balance: float, unrealized_pnl: float, created_at: str
    ) -> None:
        await self.conn.execute(
            "INSERT INTO bankroll_snapshots (balance, unrealized_pnl, created_at) VALUES (?, ?, ?)",
            (balance, unrealized_pnl, created_at),
        )
        await self.conn.commit()

    async def settle_trades(self, city: str, date: str, actual_temp: int) -> int:
        """Settle all unsettled trades for a city/date. Returns count settled."""
        cursor = await self.conn.execute(
            "SELECT id, temp_f, entry_price, size FROM trades WHERE city=? AND date=? AND settled=0",
            (city, date),
        )
        rows = await cursor.fetchall()
        count = 0
        for row in rows:
            outcome = 1.0 if row["temp_f"] == actual_temp else 0.0
            pnl = (outcome - row["entry_price"]) * row["size"] / row["entry_price"]
            await self.conn.execute(
                "UPDATE trades SET settled=1, outcome=?, pnl=? WHERE id=?",
                (outcome, pnl, row["id"]),
            )
            count += 1
        await self.conn.commit()
        return count

    async def update_forecast_actual(self, city: str, date: str, actual_temp: int) -> None:
        await self.conn.execute(
            "UPDATE forecasts SET actual_temp_f=? WHERE city=? AND date=?",
            (actual_temp, city, date),
        )
        await self.conn.commit()

    async def get_brier_score(self, days: int = 30) -> float | None:
        cursor = await self.conn.execute(
            """SELECT AVG((p_model - CASE WHEN actual_temp_f = temp_f THEN 1.0 ELSE 0.0 END)
               * (p_model - CASE WHEN actual_temp_f = temp_f THEN 1.0 ELSE 0.0 END))
               FROM forecasts
               WHERE actual_temp_f IS NOT NULL
               AND created_at >= datetime('now', ?)""",
            (f"-{days} days",),
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] is not None else None

    async def get_pnl_summary(self, days: int = 30) -> dict:
        cursor = await self.conn.execute(
            """SELECT
                 COUNT(*) as total_trades,
                 SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                 SUM(pnl) as total_pnl,
                 MIN(pnl) as worst_trade,
                 MAX(pnl) as best_trade
               FROM trades
               WHERE settled = 1
               AND created_at >= datetime('now', ?)""",
            (f"-{days} days",),
        )
        row = await cursor.fetchone()
        if not row or row["total_trades"] == 0:
            return {"total_trades": 0, "wins": 0, "total_pnl": 0, "win_rate": 0}
        return {
            "total_trades": row["total_trades"],
            "wins": row["wins"] or 0,
            "total_pnl": row["total_pnl"] or 0,
            "worst_trade": row["worst_trade"] or 0,
            "best_trade": row["best_trade"] or 0,
            "win_rate": (row["wins"] or 0) / row["total_trades"],
        }
