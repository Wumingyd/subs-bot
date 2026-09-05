from __future__ import annotations

import json
import secrets
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import aiosqlite

from config import DB_PATH


SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    expire_at INTEGER,
    traffic_used REAL,
    traffic_total REAL,
    nodes_json TEXT NOT NULL DEFAULT '[]',
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS path_maps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    node_name TEXT NOT NULL,
    remark TEXT NOT NULL DEFAULT '',
    UNIQUE(user_id, node_name)
);
CREATE TABLE IF NOT EXISTS temp_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS short_links (
    code TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    target_url TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deleted_subs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    token TEXT NOT NULL,
    expire_at INTEGER,
    traffic_used REAL,
    traffic_total REAL,
    nodes_json TEXT NOT NULL DEFAULT '[]',
    last_error TEXT,
    created_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str | None = None) -> None:
        self.path = str(path or DB_PATH)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        try:
            await db.executescript(SCHEMA)
            await db.commit()
            yield db
        finally:
            await db.close()

    async def list_subs(self, user_id: int) -> list[dict[str, Any]]:
        async with self.connection() as db:
            cur = await db.execute(
                "SELECT * FROM subscriptions WHERE user_id=? ORDER BY id ASC", (user_id,)
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_sub(self, user_id: int, sub_id: int) -> dict[str, Any] | None:
        async with self.connection() as db:
            cur = await db.execute(
                "SELECT * FROM subscriptions WHERE user_id=? AND id=?", (user_id, sub_id)
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def get_sub_by_token(self, token: str) -> dict[str, Any] | None:
        async with self.connection() as db:
            cur = await db.execute("SELECT * FROM subscriptions WHERE token=?", (token,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def add_sub(self, user_id: int, name: str, url: str) -> dict[str, Any]:
        now = int(time.time())
        token = secrets.token_urlsafe(12)
        async with self.connection() as db:
            cur = await db.execute(
                """INSERT INTO subscriptions
                (user_id,name,url,token,expire_at,traffic_used,traffic_total,nodes_json,created_at,updated_at)
                VALUES (?,?,?,?,NULL,NULL,NULL,'[]',?,?)""",
                (user_id, name, url, token, now, now),
            )
            await db.commit()
            sub_id = int(cur.lastrowid)
        sub = await self.get_sub(user_id, sub_id)
        assert sub is not None
        return sub

    async def update_sub(self, user_id: int, sub_id: int, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return await self.get_sub(user_id, sub_id)
        allowed = {
            "name", "url", "token", "expire_at", "traffic_used",
            "traffic_total", "nodes_json", "last_error", "created_at",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported subscription fields: {sorted(unknown)}")
        fields["updated_at"] = int(time.time())
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [user_id, sub_id]
        async with self.connection() as db:
            await db.execute(
                f"UPDATE subscriptions SET {cols} WHERE user_id=? AND id=?", vals
            )
            await db.commit()
        return await self.get_sub(user_id, sub_id)

    async def delete_sub(self, user_id: int, sub_id: int) -> bool:
        sub = await self.get_sub(user_id, sub_id)
        if not sub:
            return False
        now = int(time.time())
        async with self.connection() as db:
            await db.execute(
                """INSERT INTO deleted_subs
                (user_id,name,url,token,expire_at,traffic_used,traffic_total,nodes_json,last_error,created_at,deleted_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    user_id,
                    sub["name"],
                    sub["url"],
                    sub["token"],
                    sub["expire_at"],
                    sub["traffic_used"],
                    sub["traffic_total"],
                    sub["nodes_json"],
                    sub["last_error"],
                    sub["created_at"],
                    now,
                ),
            )
            cur = await db.execute(
                "DELETE FROM subscriptions WHERE user_id=? AND id=?", (user_id, sub_id)
            )
            await db.commit()
            return cur.rowcount > 0

    async def list_deleted(self, user_id: int, days: int = 30) -> list[dict[str, Any]]:
        cutoff = int(time.time()) - days * 86400
        async with self.connection() as db:
            cur = await db.execute(
                "SELECT * FROM deleted_subs WHERE user_id=? AND deleted_at>=? ORDER BY deleted_at DESC",
                (user_id, cutoff),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def restore_deleted(self, user_id: int, deleted_id: int) -> dict[str, Any] | None:
        async with self.connection() as db:
            cur = await db.execute(
                "SELECT * FROM deleted_subs WHERE user_id=? AND id=?", (user_id, deleted_id)
            )
            row = await cur.fetchone()
            if not row:
                return None
            item = dict(row)
            now = int(time.time())
            token = item["token"] or secrets.token_urlsafe(12)
            await db.execute(
                """INSERT INTO subscriptions
                (user_id,name,url,token,expire_at,traffic_used,traffic_total,nodes_json,last_error,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    user_id,
                    item["name"],
                    item["url"],
                    token,
                    item["expire_at"],
                    item["traffic_used"],
                    item["traffic_total"],
                    item["nodes_json"],
                    item["last_error"],
                    item["created_at"],
                    now,
                ),
            )
            await db.execute("DELETE FROM deleted_subs WHERE id=?", (deleted_id,))
            await db.commit()
        subs = await self.list_subs(user_id)
        return subs[-1] if subs else None

    async def renumber(self, user_id: int) -> int:
        # Display numbers are derived from ORDER BY id; never rewrite rows here.
        # Re-inserting would invalidate callback IDs and unnecessarily risk data loss.
        return len(await self.list_subs(user_id))

    async def add_imported_sub(
        self, user_id: int, name: str, nodes: list[dict[str, Any]]
    ) -> dict[str, Any]:
        now = int(time.time())
        token = secrets.token_urlsafe(12)
        local_url = f"uploaded://{secrets.token_urlsafe(8)}"
        async with self.connection() as db:
            cur = await db.execute(
                """INSERT INTO subscriptions
                (user_id,name,url,token,expire_at,traffic_used,traffic_total,nodes_json,created_at,updated_at)
                VALUES (?,?,?,?,NULL,NULL,NULL,?,?,?)""",
                (
                    user_id, name[:64] or "导入配置", local_url, token,
                    json.dumps(nodes, ensure_ascii=False), now, now,
                ),
            )
            await db.commit()
            sub_id = int(cur.lastrowid)
        sub = await self.get_sub(user_id, sub_id)
        assert sub is not None
        return sub

    async def list_path_maps(self, user_id: int) -> list[dict[str, Any]]:
        async with self.connection() as db:
            cur = await db.execute(
                "SELECT * FROM path_maps WHERE user_id=? ORDER BY id ASC", (user_id,)
            )
            return [dict(r) for r in await cur.fetchall()]

    async def upsert_path_map(self, user_id: int, node_name: str, remark: str) -> None:
        async with self.connection() as db:
            if remark == "":
                await db.execute(
                    "DELETE FROM path_maps WHERE user_id=? AND node_name=?", (user_id, node_name)
                )
            else:
                await db.execute(
                    """INSERT INTO path_maps(user_id,node_name,remark) VALUES(?,?,?)
                    ON CONFLICT(user_id,node_name) DO UPDATE SET remark=excluded.remark""",
                    (user_id, node_name, remark),
                )
            await db.commit()

    async def clear_path_maps(self, user_id: int) -> int:
        async with self.connection() as db:
            cur = await db.execute("DELETE FROM path_maps WHERE user_id=?", (user_id,))
            await db.commit()
            return cur.rowcount

    async def delete_path_map(self, user_id: int, map_id: int) -> bool:
        async with self.connection() as db:
            cur = await db.execute(
                "DELETE FROM path_maps WHERE user_id=? AND id=?", (user_id, map_id)
            )
            await db.commit()
            return cur.rowcount > 0

    async def list_temp(self, user_id: int) -> list[dict[str, Any]]:
        async with self.connection() as db:
            cur = await db.execute(
                "SELECT * FROM temp_nodes WHERE user_id=? ORDER BY id ASC", (user_id,)
            )
            return [dict(r) for r in await cur.fetchall()]

    async def add_temp(self, user_id: int, url: str, name: str) -> None:
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO temp_nodes(user_id,url,name,created_at) VALUES(?,?,?,?)",
                (user_id, url, name, int(time.time())),
            )
            await db.commit()

    async def clear_temp(self, user_id: int) -> int:
        async with self.connection() as db:
            cur = await db.execute("DELETE FROM temp_nodes WHERE user_id=?", (user_id,))
            await db.commit()
            return cur.rowcount

    async def create_short(self, user_id: int, target_url: str) -> str:
        code = secrets.token_urlsafe(6)
        async with self.connection() as db:
            await db.execute(
                "INSERT INTO short_links(code,user_id,target_url,created_at) VALUES(?,?,?,?)",
                (code, user_id, target_url, int(time.time())),
            )
            await db.commit()
        return code

    async def get_short(self, code: str) -> dict[str, Any] | None:
        async with self.connection() as db:
            cur = await db.execute("SELECT * FROM short_links WHERE code=?", (code,))
            row = await cur.fetchone()
            return dict(row) if row else None


def nodes_of(sub: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        data = json.loads(sub.get("nodes_json") or "[]")
        return data if isinstance(data, list) else []
    except Exception:
        return []
