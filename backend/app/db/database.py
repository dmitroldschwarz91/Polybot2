"""
Persistence — trade history in SQLite via SQLAlchemy aiosqlite.

Only closed trades are persisted (the live loop keeps open positions in memory
for speed). This lets the dashboard show history across restarts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from ..domain.models import Position


class Base(DeclarativeBase):
    pass


class TradeRecord(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(String, index=True)
    asset = Column(String, index=True)
    direction = Column(String)
    entry_type = Column(String, index=True)
    entry_price = Column(Float)
    entry_size = Column(Integer)
    entry_cost = Column(Float)
    close_reason = Column(String, index=True)
    close_pnl = Column(Float)
    close_proceeds = Column(Float)
    entry_ts = Column(DateTime, index=True)
    close_ts = Column(DateTime, index=True)
    raw = Column(Text)


class Database:
    def __init__(self, url: str) -> None:
        self.url = url if url.startswith("sqlite+aiosqlite") else url.replace("sqlite://", "sqlite+aiosqlite://")
        self._engine = None
        self._sessionmaker: Optional[async_sessionmaker] = None

    async def init(self) -> None:
        self._engine = create_async_engine(self.url, echo=False)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()

    async def save_trade(self, pos: Position) -> None:
        if not self._sessionmaker:
            return
        rec = TradeRecord(
            slug=pos.slug, asset=pos.asset, direction=pos.direction,
            entry_type=pos.entry_type.value, entry_price=pos.entry_price,
            entry_size=pos.entry_size, entry_cost=pos.entry_cost,
            close_reason=pos.close_reason.value if pos.close_reason else "unknown",
            close_pnl=pos.total_pnl, close_proceeds=pos.close_proceeds,
            entry_ts=datetime.fromtimestamp(pos.entry_timestamp, tz=timezone.utc),
            close_ts=datetime.fromtimestamp(pos.close_timestamp, tz=timezone.utc) if pos.close_timestamp else None,
            raw="",
        )
        async with self._sessionmaker() as session:  # type: AsyncSession
            session.add(rec)
            await session.commit()

    async def recent_trades(self, limit: int = 100) -> List[dict]:
        if not self._sessionmaker:
            return []
        async with self._sessionmaker() as session:
            rows = (await session.execute(
                select(TradeRecord).order_by(TradeRecord.id.desc()).limit(limit)
            )).scalars().all()
            return [{
                "id": r.id, "slug": r.slug, "asset": r.asset, "direction": r.direction,
                "entry_type": r.entry_type, "entry_price": r.entry_price,
                "entry_size": r.entry_size, "close_reason": r.close_reason,
                "close_pnl": r.close_pnl, "entry_ts": r.entry_ts.isoformat() if r.entry_ts else None,
                "close_ts": r.close_ts.isoformat() if r.close_ts else None,
            } for r in rows]

    async def pnl_series(self, limit: int = 200) -> List[dict]:
        if not self._sessionmaker:
            return []
        async with self._sessionmaker() as session:
            rows = (await session.execute(
                select(TradeRecord).order_by(TradeRecord.id.asc()).limit(limit)
            )).scalars().all()
            cumulative = 0.0
            out = []
            for r in rows:
                cumulative += r.close_pnl or 0.0
                out.append({"ts": r.close_ts.isoformat() if r.close_ts else None,
                            "pnl": r.close_pnl or 0.0, "cumulative": round(cumulative, 4)})
            return out
