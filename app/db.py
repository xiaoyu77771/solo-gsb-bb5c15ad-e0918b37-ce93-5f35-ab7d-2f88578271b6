"""SQLite 存储层：单连接 + 进程内写锁，启动时由版本化迁移器自动建库/升级。

连接以 autocommit 模式（isolation_level=None）创建：sqlite3 不再隐式开启事务，
所有状态变更必须经 locked_transaction 显式 BEGIN IMMEDIATE…COMMIT/ROLLBACK。
这样 4xx 校验失败或写入中途异常时，本次写入随事务回滚，不会遗留被后续请求意外
提交的半成品事务。

数据库结构由 MIGRATIONS 中的固定版本序列定义（每个版本携带不可变的 SQL 脚本与其
SHA-256）。启动时 run_migrations 在一笔 BEGIN IMMEDIATE 事务内：
  - 空库：顺序执行全部版本，逐版本登记到 schema_migrations；
  - 旧库（无 schema_migrations 表的按列探测时代产物）：逐版本探测已有结构，
    已存在的版本只登记不执行，缺失的版本补执行，最终与空库收敛到同一最新结构；
  - 已纳管库：校验已记录版本的校验和、版本连续性与版本上界后只执行新版本。
校验和变化、版本缺口、数据库版本高于代码均抛 MigrationError 拒绝启动；整轮迁移
在一个事务内提交，任何失败整体 ROLLBACK，下次启动可原样重试。
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

DB_PATH = os.environ.get("UPLOAD_DB_PATH", "./data/coordinator.db")

# 进程内写锁：SQLite 单写者，串行化所有状态变更，保证租约/完成/提交的原子性。
DB_LOCK = threading.RLock()

_conn: sqlite3.Connection | None = None

# 故障注入钩子（仅供测试）：形如 (sql_substring, remaining_matches) 时，写事务内
# 执行匹配该子串的第 (remaining+1) 条 SQL 会被中断并抛 OperationalError，模拟
# “写入中途的数据库异常”。生产代码不设置该值；set_sql_fault 会把进程级连接替换为
# _FaultableConnection 子类实例（方法在子类层重写，覆盖 sqlite3 C 类型的不可变限制），
# 因此事务内经 get_conn() 直接执行的内部辅助 SQL（如取消清理）也能被精确命中。
_fault: tuple[str, int] | None = None
# 仅在 locked_transaction 期间为真：读路径（autocommit）上的同名 SQL 不会被故障命中
_in_write_transaction = False

logger = logging.getLogger("app.db")


class MigrationError(RuntimeError):
    """数据库迁移校验/执行失败：服务必须拒绝启动（不返回部分升级的库）。"""


class _FaultableConnection(sqlite3.Connection):
    """支持故障注入的连接子类：execute/executemany 在写事务内命中断言子串时抛
    OperationalError，其余行为与普通 sqlite3.Connection 完全一致。"""

    def execute(self, sql, parameters=()):
        if _in_write_transaction:
            _maybe_fault(sql)
        return super().execute(sql, parameters)

    def executemany(self, sql, parameters):
        if _in_write_transaction:
            _maybe_fault(sql)
        return super().executemany(sql, parameters)


def _maybe_fault(sql: str) -> None:
    global _fault
    if _fault is not None and _fault[0] in sql:
        match, remaining = _fault
        if remaining > 0:
            _fault = (match, remaining - 1)
        else:
            # 一次性故障：触发后自动解除，随后的重试请求不再受影响
            _fault = None
            raise sqlite3.OperationalError(
                "injected fault for matching SQL in write transaction")


def set_sql_fault(match: str | None, allow_matching: int = 0) -> None:
    """测试辅助：设置/清除写事务内的 SQL 故障注入。

    match 为 None 时清除；否则在写事务内执行包含该子串的第
    (allow_matching+1) 条 SQL 时抛 sqlite3.OperationalError（所在事务随即由
    locked_transaction 回滚）。设置时把共享连接升级为支持故障注入的子类实例，
    连接状态（库文件/WAL/PRAGMA）保持不变。
    """
    global _conn, _fault
    _fault = None if match is None else (match, allow_matching)
    if match is not None and _conn is not None and not isinstance(
            _conn, _FaultableConnection):
        _conn = sqlite3.connect(
            DB_PATH, check_same_thread=False, isolation_level=None,
            factory=_FaultableConnection)
        _conn.row_factory = sqlite3.Row
        # foreign_keys 是连接级设置（不持久于库文件），重连后必须重新打开；
        # journal_mode=WAL 记录在数据库头中，重连后保持不变
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=5000")


@contextmanager
def locked_transaction() -> Iterator[sqlite3.Connection]:
    """调用方必须已持有 DB_LOCK：在共享连接上开一笔显式写事务。

    连接以 autocommit 模式（isolation_level=None）运行，任何状态变更的提交点都只
    能由本上下文显式给出：正常退出时 COMMIT；以任意异常（含 4xx HTTPException 与
    sqlite3 数据库异常）退出时 ROLLBACK，保证本次写入绝不会遗留成开着的事务而被后续
    无关请求意外提交。ROLLBACK 同时把出错的连接恢复到干净可用状态。
    """
    global _in_write_transaction
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    _in_write_transaction = True
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        _in_write_transaction = False


# ---------------------------------------------------------------- 版本化迁移定义
#
# 每个版本是不可变的：版本号、脚本语句与顺序一经发布就不得再改动（启动时用
# schema_migrations.checksum 校验）。需要新结构时只能追加新版本，禁止改写历史
# 版本——校验和变化会被检测到并拒绝启动。
#
# 每条语句配一个结构探测谓词：
#   - 旧库（按列探测时代创建、没有 schema_migrations 表）纳管时，探测为“已存在”
#     的语句只登记版本不执行，保证旧数据、索引与外键原样保留；
#   - 任何模式下探测都充当幂等闸门，ALTER 绝不会重复执行。
#
# 空库与旧库执行/探测完同一组版本后收敛到完全一致的最新结构。

SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,      -- 固定递增的版本号，从 1 开始且不留缺口
    name        TEXT NOT NULL,            -- 版本用途说明
    checksum    TEXT NOT NULL,            -- 该版本规范脚本的 SHA-256（64 位小写十六进制）
    applied_at  TEXT NOT NULL             -- 登记时间（UTC ISO-8601）
)
"""


def _table_exists(name: str) -> Callable[[sqlite3.Connection], bool]:
    def probe(conn: sqlite3.Connection) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone() is not None
    return probe


def _column_exists(target: str) -> Callable[[sqlite3.Connection], bool]:
    table, _, column_name = target.partition(".")

    def probe(conn: sqlite3.Connection) -> bool:
        # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)；
        # 使用位置下标以兼容未设置 row_factory 的连接。
        return any(
            row[1] == column_name
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        )
    return probe


def _index_exists(name: str) -> Callable[[sqlite3.Connection], bool]:
    def probe(conn: sqlite3.Connection) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone() is not None
    return probe


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]
    probes: tuple[Callable[[sqlite3.Connection], bool], ...]

    def __post_init__(self) -> None:
        if not self.statements or len(self.statements) != len(self.probes):
            raise ValueError(
                f"migration {self.version}: statements/probes mismatch")

    @property
    def script(self) -> str:
        """规范脚本：语句各自去首尾空白后用空行连接；校验和只依赖该文本。"""
        return "\n\n".join(stmt.strip() for stmt in self.statements)

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.script.encode("utf-8")).hexdigest()


def _migration(version: int, name: str,
               steps: list[tuple[str, Callable[[sqlite3.Connection], bool]]]
               ) -> Migration:
    statements = tuple(stmt for stmt, _ in steps)
    probes = tuple(probe for _, probe in steps)
    return Migration(version, name, statements, probes)


MIGRATIONS: tuple[Migration, ...] = (
    # 0001: 初始结构（旧库的 sessions/chunks 等表按探测结果跳过建表，
    # 仅补齐旧库缺失的 chunk_dependencies 与索引；既有数据原样保留）
    _migration(1, "initial schema", [
        (
            """
CREATE TABLE IF NOT EXISTS sessions (
    id                     TEXT PRIMARY KEY,
    total_chunks           INTEGER NOT NULL,
    file_digest            TEXT NOT NULL,
    digest_algorithm       TEXT NOT NULL DEFAULT 'sha256',
    status                 TEXT NOT NULL DEFAULT 'active',
    created_at             TEXT NOT NULL,
    finalized_at           TEXT
)
            """,
            _table_exists("sessions"),
        ),
        (
            """
CREATE TABLE IF NOT EXISTS chunks (
    session_id        TEXT NOT NULL REFERENCES sessions(id),
    chunk_index       INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    lease_token       TEXT,
    lease_expires_at  TEXT,
    chunk_digest      TEXT,
    completed_at      TEXT,
    PRIMARY KEY (session_id, chunk_index)
)
            """,
            _table_exists("chunks"),
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_chunks_session ON chunks(session_id)",
            _index_exists("idx_chunks_session"),
        ),
        (
            """
CREATE TABLE IF NOT EXISTS chunk_dependencies (
    session_id        TEXT NOT NULL,
    chunk_index       INTEGER NOT NULL,
    depends_on        INTEGER NOT NULL,
    PRIMARY KEY (session_id, chunk_index, depends_on),
    FOREIGN KEY (session_id, chunk_index) REFERENCES chunks(session_id, chunk_index)
)
            """,
            _table_exists("chunk_dependencies"),
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_chunk_deps_session"
            " ON chunk_dependencies(session_id)",
            _index_exists("idx_chunk_deps_session"),
        ),
    ]),
    # 0002: 会话级并发租约上限；旧库已存在的会话取 NULL，即保持“不限并发”
    _migration(2, "sessions.max_active_leases", [
        ("ALTER TABLE sessions ADD COLUMN max_active_leases INTEGER",
         _column_exists("sessions.max_active_leases")),
    ]),
    # 0003: 失败退避参数；旧库既有会话取服务端默认值 5/300
    _migration(3, "sessions retry backoff columns", [
        ("ALTER TABLE sessions ADD COLUMN base_retry_seconds"
         " INTEGER NOT NULL DEFAULT 5",
         _column_exists("sessions.base_retry_seconds")),
        ("ALTER TABLE sessions ADD COLUMN max_retry_seconds"
         " INTEGER NOT NULL DEFAULT 300",
         _column_exists("sessions.max_retry_seconds")),
    ]),
    # 0004: 分片级失败计数与冷却截止时间；既有分片从 0/NULL 开始
    _migration(4, "chunks failure backoff columns", [
        ("ALTER TABLE chunks ADD COLUMN failure_count"
         " INTEGER NOT NULL DEFAULT 0",
         _column_exists("chunks.failure_count")),
        ("ALTER TABLE chunks ADD COLUMN backoff_until TEXT",
         _column_exists("chunks.backoff_until")),
    ]),
    # 0005: 会话修订号；每次成功 reopen 回退递增 1，既有会话从 0 开始
    _migration(5, "sessions.revision", [
        ("ALTER TABLE sessions ADD COLUMN revision"
         " INTEGER NOT NULL DEFAULT 0",
         _column_exists("sessions.revision")),
    ]),
    # 0006: 进度分支来源会话；既有原生会话为 NULL
    _migration(6, "sessions.source_session_id", [
        ("ALTER TABLE sessions ADD COLUMN source_session_id TEXT",
         _column_exists("sessions.source_session_id")),
    ]),
    # 0007: fork 幂等记录表
    _migration(7, "fork_requests table", [
        (
            """
CREATE TABLE IF NOT EXISTS fork_requests (
    source_session_id  TEXT NOT NULL,
    request_id         TEXT NOT NULL,
    new_session_id     TEXT NOT NULL,
    expected_revision  INTEGER NOT NULL,
    reuse_chunks       TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    PRIMARY KEY (source_session_id, request_id)
)
            """,
            _table_exists("fork_requests"),
        ),
    ]),
    # 0008: 跨会话公平调度游标表，首次调度时惰性写入游标
    _migration(8, "dispatch_cursors table", [
        (
            """
CREATE TABLE IF NOT EXISTS dispatch_cursors (
    queue_key        TEXT PRIMARY KEY,
    last_session_id  TEXT NOT NULL,
    updated_at       TEXT NOT NULL
)
            """,
            _table_exists("dispatch_cursors"),
        ),
    ]),
    # 0009: 交付凭证表，仅保存令牌哈希、会话、到期时间与状态
    _migration(9, "delivery_tickets table", [
        (
            """
CREATE TABLE IF NOT EXISTS delivery_tickets (
    token_hash  TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL REFERENCES sessions(id),
    expires_at  TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active'
)
            """,
            _table_exists("delivery_tickets"),
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_delivery_tickets_session"
            " ON delivery_tickets(session_id)",
            _index_exists("idx_delivery_tickets_session"),
        ),
    ]),
    # 0010: 交付凭证兑换幂等记录表；保存首次兑换响应，重放原样返回
    _migration(10, "delivery_ticket_redemptions table", [
        (
            """
CREATE TABLE IF NOT EXISTS delivery_ticket_redemptions (
    token_hash    TEXT NOT NULL REFERENCES delivery_tickets(token_hash),
    request_id    TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (token_hash, request_id)
)
            """,
            _table_exists("delivery_ticket_redemptions"),
        ),
    ]),
    # 0011: 分片验收队列开关；旧库既有会话取 0（关闭），行为完全不变
    _migration(11, "sessions.verification_required", [
        ("ALTER TABLE sessions ADD COLUMN verification_required"
         " INTEGER NOT NULL DEFAULT 0",
         _column_exists("sessions.verification_required")),
    ]),
    # 0012: 分片验收租约表，仅保存令牌哈希、分片与到期时间
    _migration(12, "verification_leases table", [
        (
            """
CREATE TABLE IF NOT EXISTS verification_leases (
    token_hash    TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL,
    chunk_index   INTEGER NOT NULL,
    issued_at     TEXT NOT NULL,
    expires_at    TEXT NOT NULL,
    UNIQUE (session_id, chunk_index),
    FOREIGN KEY (session_id, chunk_index) REFERENCES chunks(session_id, chunk_index)
)
            """,
            _table_exists("verification_leases"),
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_verification_leases_pending"
            " ON verification_leases(session_id, chunk_index)",
            _index_exists("idx_verification_leases_pending"),
        ),
    ]),
    # 0013: 验收决定幂等记录表；重放原样返回、冲突决定 409
    _migration(13, "verification_decisions table", [
        (
            """
CREATE TABLE IF NOT EXISTS verification_decisions (
    token_hash    TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL,
    chunk_index   INTEGER NOT NULL,
    decision      TEXT NOT NULL,
    decided_at    TEXT NOT NULL,
    FOREIGN KEY (session_id, chunk_index) REFERENCES chunks(session_id, chunk_index)
)
            """,
            _table_exists("verification_decisions"),
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_verification_decisions_chunk"
            " ON verification_decisions(session_id, chunk_index)",
            _index_exists("idx_verification_decisions_chunk"),
        ),
    ]),
    # 0014: 租户容量预留列。tenant_id 在新库可为空（旧库既有行无租户归属，
    # 不参与账务聚合）；应用创建的会话始终显式写入租户
    _migration(14, "sessions tenant reservation columns", [
        ("ALTER TABLE sessions ADD COLUMN tenant_id TEXT",
         _column_exists("sessions.tenant_id")),
        ("ALTER TABLE sessions ADD COLUMN declared_bytes"
         " INTEGER NOT NULL DEFAULT 0",
         _column_exists("sessions.declared_bytes")),
        ("ALTER TABLE sessions ADD COLUMN reservation_expires_at TEXT",
         _column_exists("sessions.reservation_expires_at")),
        ("ALTER TABLE sessions ADD COLUMN cancelled_at TEXT",
         _column_exists("sessions.cancelled_at")),
    ]),
    # 0015: 租户硬配额表，首次 PUT /tenants/{id}/quota 时惰性写入
    _migration(15, "tenants table", [
        (
            """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id    TEXT PRIMARY KEY,
    quota_bytes  INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
)
            """,
            _table_exists("tenants"),
        ),
    ]),
    # 0016: 租户容量账务聚合索引（依赖 0014 的 tenant_id 列，故单独成版本）
    _migration(16, "sessions tenant state index", [
        (
            "CREATE INDEX IF NOT EXISTS idx_sessions_tenant_state"
            " ON sessions(tenant_id, status, reservation_expires_at)",
            _index_exists("idx_sessions_tenant_state"),
        ),
    ]),
    # 0017: 验收决定首次响应快照列。approve/reject 成功时把首次响应的
    # status/chunk_digest 与分片状态变更写入同一事务，使同一令牌同一决定在
    # reopen、取消或预留到期等后续状态变化后仍能返回首次结果（200）。
    # 旧库既有的决定行没有快照：启动迁移时按决定类型回填最佳近似——
    # approve 回填 completed 与该分片当前摘要（若摘要已被后续 reopen 清除则为 NULL，
    # 这是旧数据信息已丢失时的保守回填），reject 回填 pending/NULL。
    _migration(17, "verification_decisions first-response snapshot columns", [
        ("ALTER TABLE verification_decisions ADD COLUMN result_status TEXT",
         _column_exists("verification_decisions.result_status")),
        ("ALTER TABLE verification_decisions ADD COLUMN result_chunk_digest TEXT",
         _column_exists("verification_decisions.result_chunk_digest")),
        (
            """
UPDATE verification_decisions
    SET result_status = CASE decision
            WHEN 'approve' THEN 'completed'
            WHEN 'reject' THEN 'pending'
        END,
        result_chunk_digest = CASE
            WHEN decision = 'approve' THEN (
                SELECT c.chunk_digest FROM chunks c
                WHERE c.session_id = verification_decisions.session_id
                  AND c.chunk_index = verification_decisions.chunk_index
            )
            ELSE NULL
        END
    WHERE result_status IS NULL
            """,
            # 探测：不存在“尚未回填”的决定行时视为已完成（空表/重跑均跳过本语句）
            lambda conn: conn.execute(
                "SELECT 1 FROM verification_decisions"
                " WHERE result_status IS NULL LIMIT 1"
            ).fetchone() is None,
        ),
    ]),
)

LATEST_VERSION = MIGRATIONS[-1].version
_MIGRATIONS_BY_VERSION = {m.version: m for m in MIGRATIONS}

# 并发启动时等待其它进程释放写锁的最长时间（总等待的硬上限）：超时仍拿不到锁
# 则按明确错误失败，由编排层（restart: unless-stopped / 人工）重试。
# 连接的 busy_timeout 不参与这段等待：抢锁期间它被临时降为 0，由重试循环自己
# 按固定间隔控制节奏，保证配置值小于 busy_timeout 时也能按配置及时失败。
MIGRATION_LOCK_TIMEOUT_SECONDS = float(
    os.environ.get("MIGRATION_LOCK_TIMEOUT_SECONDS", "30"))
_MIGRATION_LOCK_RETRY_INTERVAL_SECONDS = 0.1


def _recorded_versions(conn: sqlite3.Connection) -> dict[int, str]:
    """读取已登记版本 -> 脚本校验和（在调用方持有的迁移事务内执行）。"""
    return {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT version, checksum FROM schema_migrations ORDER BY version"
        ).fetchall()
    }


def _verify_migration_history(recorded: dict[int, str]) -> None:
    """对已纳管库做三项启动校验，任一不满足即抛 MigrationError 拒绝服务。"""
    if not recorded:
        return
    max_recorded = max(recorded)

    future_versions = sorted(v for v in recorded if v > LATEST_VERSION)
    if future_versions:
        raise MigrationError(
            "数据库迁移版本高于当前代码：数据库已记录到版本 "
            f"{max(future_versions)}，而本代码支持的最新版本为 {LATEST_VERSION}"
            f"（{MIGRATIONS[-1].name}）。请先升级应用，或恢复与该代码版本匹配的"
            "数据库备份；服务拒绝启动。")

    missing = [v for v in range(1, max_recorded + 1) if v not in recorded]
    if missing:
        raise MigrationError(
            "数据库迁移历史存在版本缺口：已记录到版本 "
            f"{max_recorded}，但缺少版本 {missing}。迁移必须自版本 1 起连续应用，"
            "请恢复完整一致的数据库备份，或改用匹配的代码版本；服务拒绝启动。")

    mismatches = []
    for version in sorted(recorded):
        migration = _MIGRATIONS_BY_VERSION[version]
        actual = recorded[version]
        expected = migration.checksum
        if actual != expected:
            mismatches.append((version, migration.name, actual, expected))
    if mismatches:
        version, name, actual, expected = mismatches[0]
        raise MigrationError(
            f"已记录版本 {version}（{name}）的迁移脚本校验和发生变化："
            f"数据库记录 SHA-256={actual}，当前代码 SHA-256={expected}。"
            "已发布的迁移脚本不可修改；请核对代码版本或从备份恢复数据库，"
            "服务拒绝启动。")


def _apply_migrations_in_tx(conn: sqlite3.Connection) -> list[Migration]:
    """在已开启的迁移事务内完成全部校验、升级与登记（不自行提交/回滚）。

    返回本次新应用的版本列表（旧库纳管时只补登记、未实际执行的版本不在其中）。
    """
    conn.execute(SCHEMA_MIGRATIONS_DDL)
    recorded = _recorded_versions(conn)
    _verify_migration_history(recorded)

    applied: list[Migration] = []
    applied_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for migration in MIGRATIONS:
        if migration.version in recorded:
            continue
        for statement, probe in zip(migration.statements, migration.probes):
            try:
                if not probe(conn):
                    conn.execute(statement)
            except sqlite3.Error as exc:
                raise MigrationError(
                    f"执行版本 {migration.version}（{migration.name}）迁移失败："
                    f"{exc}。本轮迁移已整体回滚，数据库保持启动前状态；"
                    "修复问题后重新启动即可重试。") from exc
        conn.execute(
            "INSERT INTO schema_migrations(version, name, checksum, applied_at)"
            " VALUES (?, ?, ?, ?)",
            (migration.version, migration.name, migration.checksum, applied_at),
        )
        applied.append(migration)
    return applied


def _acquire_migration_lock(conn: sqlite3.Connection) -> None:
    """在 MIGRATION_LOCK_TIMEOUT_SECONDS 硬上限内取得 BEGIN IMMEDIATE 保留锁。

    连接级 busy_timeout（共享连接上为 5000ms）会让 BEGIN IMMEDIATE 在 SQLite
    内部阻塞到 busy_timeout 耗尽才报 locked，配置值更小的等待上限会被它吞掉。
    因此抢锁期间先把 busy_timeout 临时降为 0，让锁冲突立即失败，由本循环按
    固定间隔重试、以 monotonic 时钟控制总时长（每次休眠不超过剩余预算）；
    无论成败都在返回前恢复连接原 busy_timeout，超时抛 MigrationError 时连接
    不在任何事务中，数据库保持原状态。
    """
    original_busy_timeout = int(
        conn.execute("PRAGMA busy_timeout").fetchone()[0])
    conn.execute("PRAGMA busy_timeout=0")
    try:
        deadline = time.monotonic() + MIGRATION_LOCK_TIMEOUT_SECONDS
        while True:
            try:
                conn.execute("BEGIN IMMEDIATE")
                return
            except sqlite3.OperationalError as exc:
                # 另一进程正在迁移：按既有间隔重试；抢到锁后由调用方在同一事务内
                # 重新走“读历史→复核”全流程，已登记的版本只探测不重复执行。
                remaining = deadline - time.monotonic()
                if "locked" not in str(exc).lower() or remaining <= 0:
                    raise MigrationError(
                        "无法在 "
                        f"{MIGRATION_LOCK_TIMEOUT_SECONDS:g} 秒内取得数据库写锁"
                        "（可能有进程长时间占用），本次启动放弃迁移；"
                        "请确认其它实例已完成迁移后重启。") from exc
                time.sleep(
                    min(_MIGRATION_LOCK_RETRY_INTERVAL_SECONDS, remaining))
    finally:
        conn.execute(f"PRAGMA busy_timeout={original_busy_timeout}")


def run_migrations(conn: sqlite3.Connection) -> None:
    """在 BEGIN IMMEDIATE 事务中把数据库收敛到 LATEST_VERSION。

    可安全并发调用：SQLite 的 BEGIN IMMEDIATE 保证同一时刻只有一个进程/线程
    取得保留锁执行迁移；后来者以 MIGRATION_LOCK_TIMEOUT_SECONDS 为硬上限等待
    （连接 busy_timeout 被临时绕过，不参与计时），拿到锁后在同一函数内重新读取
    schema_migrations 复核结果——发现版本已登记则只探测不执行，绝不重复 ALTER。
    整轮迁移单事务提交：中途任何失败整体 ROLLBACK（连 schema_migrations 的建表
    也回滚），下次启动从同一状态重试。
    """
    with DB_LOCK:
        # 外部连接可能处于 sqlite3 隐式事务模式且残留未提交状态；先提交到干净点，
        # 以便下面显式 BEGIN IMMEDIATE（autocommit 连接上 commit() 为空操作）。
        conn.commit()
        _acquire_migration_lock(conn)
        try:
            applied = _apply_migrations_in_tx(conn)
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
        if applied:
            logger.info(
                "数据库迁移完成：新应用版本 %s，当前最新版本 %04d",
                ", ".join(f"{m.version:04d}" for m in applied),
                LATEST_VERSION,
            )


def get_conn() -> sqlite3.Connection:
    """返回进程级共享连接（惰性创建，首次调用时由版本化迁移器建库/升级）。

    isolation_level=None 即 SQLite autocommit 模式：Python sqlite3 不再在首条 DML
    时隐式开事务，所有写事务都必须经 locked_transaction 显式
    BEGIN...COMMIT/ROLLBACK，避免 4xx 或数据库异常后遗留的隐式事务被后续请求提交。
    """
    global _conn
    if _conn is None:
        with DB_LOCK:
            if _conn is None:
                Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(
                    DB_PATH, check_same_thread=False, isolation_level=None)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA foreign_keys=ON")
                conn.execute("PRAGMA busy_timeout=5000")
                # 空库与旧库在此收敛到同一最新结构；失败抛 MigrationError，
                # 不发布半成品连接。
                run_migrations(conn)
                _conn = conn
    return _conn


def init_db() -> None:
    """启动时显式初始化/迁移数据库；失败抛 MigrationError 拒绝服务。

    不吞异常：由调用方（应用 lifespan）把错误暴露到启动日志并终止服务，
    绝不在结构未确认的数据库上对外提供服务。
    """
    get_conn()
