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
    temp_unit TEXT NOT NULL DEFAULT 'F',
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
    fee_applied REAL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    city TEXT NOT NULL,
    date TEXT NOT NULL,
    bucket_count INTEGER NOT NULL,
    price_sum REAL NOT NULL,
    gap REAL NOT NULL,
    token_ids TEXT NOT NULL,  -- JSON array
    acted_on INTEGER DEFAULT 0,  -- 1 if orders were placed
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS city_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT NOT NULL,
    window_days INTEGER NOT NULL DEFAULT 30,
    brier_score REAL NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    UNIQUE(city, window_days)
);

CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id);
CREATE INDEX IF NOT EXISTS idx_trades_city_date ON trades(city, date);
CREATE INDEX IF NOT EXISTS idx_forecasts_city_date ON forecasts(city, date);
CREATE INDEX IF NOT EXISTS idx_trades_settled ON trades(settled);
CREATE INDEX IF NOT EXISTS idx_arb_city_date ON arbitrage_opportunities(city, date);
CREATE INDEX IF NOT EXISTS idx_city_perf_city ON city_performance(city);
"""


class Database:
    def __init__(self, db_path: str = "wedge.db") -> None:
        self._path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)
        # Migration: add temp_unit column if not exists (for backward compatibility)
        try:
            await self._conn.execute(
                "ALTER TABLE trades ADD COLUMN temp_unit TEXT NOT NULL DEFAULT 'F'"
            )
            await self._conn.commit()
        except aiosqlite.OperationalError as e:
            # SQLite raises OperationalError("duplicate column name: ...") when column exists.
            # Only ignore that specific case; re-raise anything else to avoid silent corruption.
            if "duplicate column name" in str(e).lower():
                pass
            else:
                raise

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
        temp_unit: str = 'F',
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
                   (run_id, city, date, temp_f, temp_unit, strategy, entry_price, size,
                    p_model, p_market, edge, token_id, order_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, city, date, temp_f, temp_unit, strategy, entry_price, size,
                 p_model, p_market, edge, token_id, order_id, created_at),
            )
            await self.conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def delete_trade(
        self,
        *,
        run_id: str,
        city: str,
        date: str,
        temp_f: int,
        strategy: str,
    ) -> None:
        """Delete a trade row by its unique key (used to rollback failed live executions)."""
        await self.conn.execute(
            "DELETE FROM trades WHERE run_id=? AND city=? AND date=? AND temp_f=? AND strategy=?",
            (run_id, city, date, temp_f, strategy),
        )
        await self.conn.commit()

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

    async def settle_trades(
        self,
        city: str,
        date: str,
        actual_temp: int,
        fee_rate: float = 0.02,
    ) -> int:
        """Settle all unsettled trades for a city/date. Returns count settled.

        Applies fee on profits only (not on losses).

        Args:
            city: City name
            date: Date string (ISO format)
            actual_temp: Actual temperature in Fahrenheit
            fee_rate: Fee rate on profits (default 2% for Polymarket)
        """
        cursor = await self.conn.execute(
            "SELECT id, temp_f, entry_price, size FROM trades "
            "WHERE city=? AND date=? AND settled=0",
            (city, date),
        )
        rows = await cursor.fetchall()
        count = 0
        for row in rows:
            outcome = 1.0 if row["temp_f"] == actual_temp else 0.0
            # Binary option P&L: (outcome - entry_price) * size / entry_price
            pnl = (outcome - row["entry_price"]) * row["size"] / row["entry_price"]

            # Apply fee on profits only
            if pnl > 0:
                pnl *= (1.0 - fee_rate)

            # Note: fee_applied column may not exist in older databases
            # Use separate UPDATE for backward compatibility
            await self.conn.execute(
                "UPDATE trades SET settled=1, outcome=?, pnl=? WHERE id=?",
                (outcome, pnl, row["id"]),
            )
            # Optionally track fee applied (ignore error if column doesn't exist)
            try:
                await self.conn.execute(
                    "UPDATE trades SET fee_applied=? WHERE id=?",
                    (fee_rate if pnl > 0 else 0.0, row["id"]),
                )
            except aiosqlite.OperationalError as e:
                # Backward-compat: older DBs may not have fee_applied column yet.
                msg = str(e).lower()
                if "no such column" in msg and "fee_applied" in msg:
                    pass
                else:
                    raise
            count += 1
        await self.conn.commit()
        return count

    async def reconcile_positions(
        self,
        remote_positions: list[dict],
        city: str | None = None,
    ) -> dict:
        """Reconcile local positions with remote (Polymarket) positions.

        Args:
            remote_positions: List of remote positions with keys:
                - city, date, temp_f, size, entry_price
            city: Optional city filter

        Returns:
            Reconciliation report with:
                - matched: count of matched positions
                - local_only: positions only in local DB
                - remote_only: positions only in remote
                - discrepancies: positions with different size/price
        """
        # Get local unsettled positions
        if city:
            cursor = await self.conn.execute(
                """SELECT city, date, temp_f, size, entry_price
                   FROM trades WHERE settled=0 AND city=?""",
                (city,),
            )
        else:
            cursor = await self.conn.execute(
                "SELECT city, date, temp_f, size, entry_price FROM trades WHERE settled=0"
            )

        local_rows = await cursor.fetchall()
        local_positions = [
            {
                "city": row["city"],
                "date": row["date"],
                "temp_f": row["temp_f"],
                "size": row["size"],
                "entry_price": row["entry_price"],
            }
            for row in local_rows
        ]

        # Create lookup keys
        def make_key(pos: dict) -> tuple:
            return (pos["city"], pos["date"], pos["temp_f"])

        local_by_key = {make_key(p): p for p in local_positions}
        remote_by_key = {make_key(p): p for p in remote_positions}

        # Find matches and discrepancies
        matched = []
        local_only = []
        remote_only = []
        discrepancies = []

        for key, local_pos in local_by_key.items():
            if key not in remote_by_key:
                local_only.append(local_pos)
            else:
                remote_pos = remote_by_key[key]
                # Check for discrepancies
                size_diff = abs(local_pos["size"] - remote_pos.get("size", 0))
                price_diff = abs(local_pos["entry_price"] - remote_pos.get("entry_price", 0))

                if size_diff > 0.01 or price_diff > 0.001:
                    discrepancies.append({
                        "key": key,
                        "local": local_pos,
                        "remote": remote_pos,
                        "size_diff": size_diff,
                        "price_diff": price_diff,
                    })
                else:
                    matched.append(local_pos)

        for key, remote_pos in remote_by_key.items():
            if key not in local_by_key:
                remote_only.append(remote_pos)

        return {
            "matched": len(matched),
            "local_only": local_only,
            "remote_only": remote_only,
            "discrepancies": discrepancies,
        }

    async def update_forecast_actual(self, city: str, date: str, actual_temp: int) -> None:
        await self.conn.execute(
            "UPDATE forecasts SET actual_temp_f=? WHERE city=? AND date=?",
            (actual_temp, city, date),
        )
        await self.conn.commit()

    async def get_last_balance(self, default: float = 1000.0) -> float:
        """Get balance from the most recent snapshot, or default if none exist."""
        cursor = await self.conn.execute(
            "SELECT balance FROM bankroll_snapshots ORDER BY created_at DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return row["balance"] if row else default

    async def get_last_balance_snapshot(self) -> tuple[float, float] | None:
        """Get (balance, unrealized_pnl) from the most recent snapshot."""
        cursor = await self.conn.execute(
            "SELECT balance, unrealized_pnl FROM bankroll_snapshots "
            "ORDER BY created_at DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return (row["balance"], row["unrealized_pnl"]) if row else None

    async def get_unsettled_dates(self) -> list[tuple[str, str]]:
        """Get distinct (city, date) pairs with unsettled trades where date <= today."""
        cursor = await self.conn.execute(
            """SELECT DISTINCT city, date FROM trades
               WHERE settled = 0 AND date <= date('now')"""
        )
        return [(row["city"], row["date"]) for row in await cursor.fetchall()]

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

    async def get_settled_trades(self, start_date, end_date) -> list[dict]:
        """Get all settled trades in date range for backtesting."""
        cursor = await self.conn.execute(
            """SELECT city, date, temp_f, strategy, entry_price, size,
                      p_model, p_market, edge, outcome, pnl, created_at
               FROM trades
               WHERE settled = 1
               AND date >= ?
               AND date <= ?
               ORDER BY date, city""",
            (str(start_date), str(end_date)),
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_open_positions(self) -> list[dict]:
        """Get all unsettled positions."""
        cursor = await self.conn.execute(
            """SELECT city, date, temp_f AS temp_value, temp_unit,
                      strategy, entry_price, size, p_model, edge, created_at
               FROM trades
               WHERE settled = 0
               ORDER BY date, city, temp_f"""
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def has_open_position(self, city: str, date: str, temp_f: int) -> bool:
        """Check if there's already an open position for this market."""
        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM trades WHERE city=? AND date=? AND temp_f=? AND settled=0",
            (city, date, temp_f),
        )
        row = await cursor.fetchone()
        return row[0] > 0 if row else False

    async def record_arbitrage(
        self,
        run_id: str,
        city: str,
        date: str,
        bucket_count: int,
        price_sum: float,
        gap: float,
        token_ids: list[str],
        acted_on: bool = False,
    ) -> int:
        """Record a detected arbitrage opportunity."""
        import json
        from datetime import datetime, timezone
        cursor = await self.conn.execute(
            """INSERT INTO arbitrage_opportunities
               (run_id, city, date, bucket_count, price_sum, gap, token_ids, acted_on, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id, city, date, bucket_count,
                price_sum, gap, json.dumps(token_ids),
                1 if acted_on else 0,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        await self.conn.commit()
        return cursor.lastrowid

    async def update_city_performance(
        self,
        city: str,
        brier_score: float,
        sample_count: int,
        window_days: int = 30,
    ) -> None:
        """Upsert city forecast performance metrics."""
        from datetime import datetime, timezone
        await self.conn.execute(
            """INSERT INTO city_performance (city, window_days, brier_score, sample_count, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(city, window_days) DO UPDATE SET
                   brier_score=excluded.brier_score,
                   sample_count=excluded.sample_count,
                   updated_at=excluded.updated_at""",
            (city, window_days, brier_score, sample_count, datetime.now(timezone.utc).isoformat()),
        )
        await self.conn.commit()

    async def get_city_performance(self, window_days: int = 30) -> dict[str, float]:
        """Get brier scores per city. Returns {city: brier_score}."""
        cursor = await self.conn.execute(
            "SELECT city, brier_score FROM city_performance WHERE window_days=?",
            (window_days,),
        )
        return {row[0]: row[1] for row in await cursor.fetchall()}
