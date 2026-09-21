"""版本化迁移器测试：空库/旧库收敛、校验和与版本防护、回滚可重试、并发启动。

这些用例一律在 tmp_path 下的独立数据库文件上用外部 sqlite3 连接调用
app.db.run_migrations，不触碰进程级共享连接（UPLOAD_DB_PATH）。
"""
import hashlib
import os
import sqlite3
import threading
import time

import pytest

import app.db as db

# 最早的线上结构：只有 sessions/chunks/chunk_dependencies 三张表与基础列
LEGACY_SCHEMA = """
CREATE TABLE sessions (
    id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
    file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL, finalized_at TEXT
);
CREATE TABLE chunks (
    session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
    lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
    PRIMARY KEY (session_id, chunk_index)
);
CREATE TABLE chunk_dependencies (
    session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
    depends_on INTEGER NOT NULL,
    PRIMARY KEY (session_id, chunk_index, depends_on),
    FOREIGN KEY (session_id, chunk_index)
        REFERENCES chunks(session_id, chunk_index)
);
INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                      status, created_at)
    VALUES ('legacy-sid', 2, 'dig', 'sha256', 'active',
            '2026-01-01T00:00:00+00:00');
INSERT INTO chunks (session_id, chunk_index)
    VALUES ('legacy-sid', 0), ('legacy-sid', 1);
INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on)
    VALUES ('legacy-sid', 0, 1);
"""

# 更老的结构：连 chunk_dependencies 表都没有
OLDEST_SCHEMA = """
CREATE TABLE sessions (
    id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
    file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL, finalized_at TEXT
);
CREATE TABLE chunks (
    session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
    lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
    PRIMARY KEY (session_id, chunk_index)
);
INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                      status, created_at)
    VALUES ('old-sid', 3, 'd', 'sha256', 'active',
            '2025-01-01T00:00:00+00:00');
"""


def _connect(path):
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrate(path):
    conn = _connect(path)
    db.run_migrations(conn)
    return conn


def _recorded(conn):
    return {row[0]: row[1] for row in conn.execute(
        "SELECT version, checksum FROM schema_migrations ORDER BY version")}


def _structure(conn):
    """返回 (业务表集合, 索引集合, {表: 排序列表})，用于比较空库/旧库最终结构。"""
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
        " AND name NOT LIKE 'sqlite_%'")}
    indexes = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    columns = {
        t: sorted(r[1] for r in conn.execute(f"PRAGMA table_info({t})"))
        for t in tables - {"schema_migrations"}
    }
    return tables, indexes, columns


# ---------------------------------------------------------------- 空库与旧库收敛

def test_fresh_database_applies_every_version_in_order(tmp_path):
    conn = _migrate(tmp_path / "fresh.db")
    assert list(_recorded(conn)) == list(range(1, db.LATEST_VERSION + 1))

    # 每个版本登记的校验和等于代码内规范脚本的 SHA-256
    for migration in db.MIGRATIONS:
        assert _recorded(conn)[migration.version] == migration.checksum
        assert migration.checksum == hashlib.sha256(
            migration.script.encode("utf-8")).hexdigest()

    # applied_at 非空，且记录了版本用途
    names = dict(conn.execute("SELECT version, name FROM schema_migrations")
                 .fetchall())
    assert names[1] == "initial schema"
    assert all(row[0] for row in conn.execute(
        "SELECT applied_at FROM schema_migrations"))
    conn.close()


def test_legacy_database_is_adopted_without_losing_data(tmp_path):
    path = tmp_path / "legacy.db"
    seed = sqlite3.connect(path)
    seed.executescript(LEGACY_SCHEMA)
    seed.commit()
    seed.close()

    conn = _migrate(path)
    assert list(_recorded(conn)) == list(range(1, db.LATEST_VERSION + 1))

    # 既有行原样保留，补列取约定默认值
    row = conn.execute(
        "SELECT total_chunks, max_active_leases, base_retry_seconds,"
        " max_retry_seconds, revision, source_session_id,"
        " verification_required, tenant_id, declared_bytes,"
        " reservation_expires_at, cancelled_at"
        " FROM sessions WHERE id='legacy-sid'").fetchone()
    assert tuple(row) == (2, None, 5, 300, 0, None, 0, None, 0, None, None)
    assert conn.execute(
        "SELECT count(*) FROM chunks WHERE session_id='legacy-sid'").fetchone()[0] == 2
    row = conn.execute(
        "SELECT failure_count, backoff_until FROM chunks"
        " WHERE session_id='legacy-sid' AND chunk_index=0").fetchone()
    assert tuple(row) == (0, None)
    assert conn.execute(
        "SELECT count(*) FROM chunk_dependencies"
        " WHERE session_id='legacy-sid' AND chunk_index=0"
        " AND depends_on=1").fetchone()[0] == 1
    conn.close()


def test_oldest_legacy_database_missing_chunk_dependencies_converges(tmp_path):
    path = tmp_path / "oldest.db"
    seed = sqlite3.connect(path)
    seed.executescript(OLDEST_SCHEMA)
    seed.commit()
    seed.close()

    conn = _migrate(path)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "chunk_dependencies" in tables
    # 既有数据保留
    assert conn.execute(
        "SELECT total_chunks FROM sessions WHERE id='old-sid'").fetchone()[0] == 3
    conn.close()


def test_fresh_and_legacy_databases_converge_to_same_structure(tmp_path):
    fresh = _migrate(tmp_path / "fresh.db")
    path = tmp_path / "legacy.db"
    seed = sqlite3.connect(path)
    seed.executescript(LEGACY_SCHEMA)
    seed.commit()
    seed.close()
    legacy = _migrate(path)

    assert _structure(fresh) == _structure(legacy)

    # 索引齐备（含旧库按列探测路径曾经缺失的租户账务索引）
    _, indexes, _ = _structure(legacy)
    assert {
        "idx_chunks_session",
        "idx_chunk_deps_session",
        "idx_delivery_tickets_session",
        "idx_verification_leases_pending",
        "idx_verification_decisions_chunk",
        "idx_sessions_tenant_state",
    } <= indexes

    # 外键：新库 chunks -> sessions 存在；旧库既有外键
    # （chunk_dependencies -> chunks）原样保留，迁移新建表自带外键
    assert fresh.execute(
        "SELECT count(*) FROM pragma_foreign_key_list('chunks')").fetchone()[0] >= 1
    assert {row[2] for row in legacy.execute(
        "PRAGMA foreign_key_list(chunk_dependencies)")} >= {"chunks"}
    assert {row[2] for row in legacy.execute(
        "PRAGMA foreign_key_list(verification_leases)")} >= {"chunks"}
    fresh.close()
    legacy.close()


def test_v17_backfills_decision_snapshots_for_legacy_decisions(tmp_path):
    """旧库的 verification_decisions 行没有首次响应快照列：0017 追加列后按决定
    类型回填——approve -> completed + 分片当前摘要；reject -> pending/NULL；
    迁移幂等，且既有决定行原样保留、服务可在迁移后启动。"""
    import hashlib

    path = tmp_path / "legacy-decisions.db"
    seed = sqlite3.connect(path)
    seed.executescript(
        """
CREATE TABLE sessions (
    id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
    file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL, finalized_at TEXT
);
CREATE TABLE chunks (
    session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
    lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
    PRIMARY KEY (session_id, chunk_index)
);
CREATE TABLE verification_decisions (
    token_hash TEXT PRIMARY KEY, session_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL, decision TEXT NOT NULL, decided_at TEXT NOT NULL,
    FOREIGN KEY (session_id, chunk_index)
        REFERENCES chunks(session_id, chunk_index)
);
INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status, created_at)
    VALUES ('s1', 2, 'dig', 'sha256', 'active', '2026-01-01T00:00:00+00:00');
INSERT INTO chunks (session_id, chunk_index, status, chunk_digest, completed_at)
    VALUES ('s1', 0, 'completed', 'aa-digest', '2026-01-02T00:00:00+00:00');
INSERT INTO chunks (session_id, chunk_index, status, chunk_digest, completed_at)
    VALUES ('s1', 1, 'pending', NULL, NULL);
INSERT INTO verification_decisions
    (token_hash, session_id, chunk_index, decision, decided_at) VALUES
    ('th-approve', 's1', 0, 'approve', '2026-01-03T00:00:00+00:00'),
    ('th-reject', 's1', 1, 'reject', '2026-01-04T00:00:00+00:00');
        """
    )
    seed.commit()
    seed.close()

    conn = _migrate(path)
    assert db.LATEST_VERSION >= 17
    assert 17 in _recorded(conn)

    rows = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT token_hash, result_status, result_chunk_digest"
        " FROM verification_decisions ORDER BY token_hash")}
    assert rows["th-approve"] == ("completed", "aa-digest")
    assert rows["th-reject"] == ("pending", None)

    # 回填的列可直接服务重放查询；再次启动幂等、不重复回填、不报错
    conn.execute(
        "UPDATE chunks SET chunk_digest = 'changed' WHERE session_id = 's1'"
        " AND chunk_index = 0")
    conn.commit()
    db.run_migrations(conn)
    snap = conn.execute(
        "SELECT result_status, result_chunk_digest FROM verification_decisions"
        " WHERE token_hash = 'th-approve'").fetchone()
    assert tuple(snap) == ("completed", "aa-digest")  # 回填值不被二次迁移覆盖
    conn.close()


def test_v17_snapshot_columns_exist_on_fresh_database(tmp_path):
    conn = _migrate(tmp_path / "fresh-v17.db")
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(verification_decisions)")}
    assert {"result_status", "result_chunk_digest"} <= cols
    # 空库上回填语句为 0 行更新，版本仍正常登记
    assert _recorded(conn)[17] == db._MIGRATIONS_BY_VERSION[17].checksum
    assert db._MIGRATIONS_BY_VERSION[17].checksum == hashlib.sha256(
        db._MIGRATIONS_BY_VERSION[17].script.encode("utf-8")).hexdigest()
    conn.close()


def test_migration_is_idempotent_on_repeated_startup(tmp_path):
    conn = _migrate(tmp_path / "idem.db")
    db.run_migrations(conn)
    db.run_migrations(conn)
    assert conn.execute(
        "SELECT count(*) FROM schema_migrations").fetchone()[0] == \
        db.LATEST_VERSION
    # 列未被重复添加
    cols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)")]
    for duplicated in ("max_active_leases", "revision", "tenant_id"):
        assert cols.count(duplicated) == 1
    conn.close()


# ---------------------------------------------------------------- 启动防护

def test_checksum_change_of_recorded_version_is_rejected(tmp_path):
    conn = _migrate(tmp_path / "tamper.db")
    conn.execute("UPDATE schema_migrations SET checksum=? WHERE version=2",
                 ("0" * 64,))
    conn.commit()
    conn.close()

    recheck = _connect(tmp_path / "tamper.db")
    with pytest.raises(db.MigrationError, match="校验和"):
        db.run_migrations(recheck)
    # 拒绝后历史不被改写
    assert recheck.execute(
        "SELECT checksum FROM schema_migrations WHERE version=2").fetchone()[0] \
        == "0" * 64
    recheck.close()


def test_version_gap_is_rejected(tmp_path):
    conn = _migrate(tmp_path / "gap.db")
    conn.execute("DELETE FROM schema_migrations WHERE version=3")
    conn.commit()
    conn.close()

    recheck = _connect(tmp_path / "gap.db")
    with pytest.raises(db.MigrationError, match="缺口"):
        db.run_migrations(recheck)
    # 缺失版本不会被“顺手补上”：拒绝启动时不得继续向前迁移
    assert 3 not in _recorded(recheck)
    recheck.close()


def test_database_version_newer_than_code_is_rejected(tmp_path):
    conn = _migrate(tmp_path / "future.db")
    conn.execute(
        "INSERT INTO schema_migrations(version, name, checksum, applied_at)"
        " VALUES (?, 'future', ?, '2026-09-20T00:00:00+00:00')",
        (db.LATEST_VERSION + 1, "a" * 64))
    conn.commit()
    conn.close()

    recheck = _connect(tmp_path / "future.db")
    with pytest.raises(db.MigrationError, match="高于当前代码"):
        db.run_migrations(recheck)
    recheck.close()


# ---------------------------------------------------------------- 失败回滚与重试

def test_mid_migration_failure_rolls_back_entirely_and_retries(tmp_path,
                                                               monkeypatch):
    path = tmp_path / "fail.db"
    m2 = db._MIGRATIONS_BY_VERSION[2]
    original_probe = m2.probes[0]

    def failing_probe(_conn):
        raise sqlite3.OperationalError("simulated failure before v2")

    # frozen dataclass：仅在本用例内替换探测谓词
    object.__setattr__(m2, "probes", (failing_probe,))
    try:
        first = _connect(path)
        with pytest.raises(db.MigrationError, match="版本 2"):
            db.run_migrations(first)

        # 整轮回滚：建表/登记都未留下
        assert first.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
            " AND name='schema_migrations'").fetchone()[0] == 0
        assert first.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
            " AND name='sessions'").fetchone()[0] == 0
        first.close()
    finally:
        object.__setattr__(m2, "probes", (original_probe,))

    # 下次启动从同一状态重试并成功
    retried = _migrate(path)
    assert retried.execute(
        "SELECT max(version) FROM schema_migrations").fetchone()[0] == \
        db.LATEST_VERSION
    retried.close()


def test_sql_error_inside_version_rolls_back_and_keeps_data(tmp_path):
    """旧库纳管时某版本 DDL 失败（此处模拟缺少依赖列的极端情况）：
    整体回滚，旧数据不动，修复后重试可成功。"""
    path = tmp_path / "sqlerr.db"
    seed = sqlite3.connect(path)
    seed.executescript(LEGACY_SCHEMA)
    seed.commit()
    seed.close()

    m16 = db._MIGRATIONS_BY_VERSION[16]
    original_statements = m16.statements

    object.__setattr__(
        m16, "statements",
        ("CREATE INDEX idx_broken ON no_such_table(x)",))
    try:
        broken = _connect(path)
        with pytest.raises(db.MigrationError, match="版本 16"):
            db.run_migrations(broken)
        broken.close()

        # 旧数据完好；schema_migrations 建表也已回滚（库仍是未纳管旧库）
        check = sqlite3.connect(path)
        assert check.execute(
            "SELECT count(*) FROM sessions WHERE id='legacy-sid'").fetchone()[0] == 1
        assert check.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
            " AND name='schema_migrations'").fetchone()[0] == 0
        check.close()
    finally:
        object.__setattr__(m16, "statements", original_statements)

    fixed = _migrate(path)
    assert fixed.execute(
        "SELECT max(version) FROM schema_migrations").fetchone()[0] == \
        db.LATEST_VERSION
    fixed.close()


# ---------------------------------------------------------------- 并发启动

def test_concurrent_startups_migrate_once(tmp_path):
    path = tmp_path / "concurrent.db"
    seed = sqlite3.connect(path)
    seed.executescript(OLDEST_SCHEMA)
    seed.commit()
    seed.close()

    errors = []
    barrier = threading.Barrier(2)

    def worker():
        try:
            conn = _connect(path)
            barrier.wait(timeout=10)
            db.run_migrations(conn)
            conn.close()
        except Exception as exc:  # noqa: BLE001 - 断言在主线程进行
            errors.append(exc)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join(30)
    t2.join(30)

    assert errors == []
    result = sqlite3.connect(path)
    assert [r[0] for r in result.execute(
        "SELECT version FROM schema_migrations ORDER BY version")] == \
        list(range(1, db.LATEST_VERSION + 1))
    # 没有重复 ALTER：每个补列只出现一次
    cols = [r[1] for r in result.execute("PRAGMA table_info(sessions)")]
    for name in ("tenant_id", "revision", "verification_required"):
        assert cols.count(name) == 1
    assert result.execute(
        "SELECT total_chunks FROM sessions WHERE id='old-sid'").fetchone()[0] == 3
    result.close()


def test_waiter_revalidates_after_other_process_finished(tmp_path, monkeypatch):
    """后启动方等待并复核：即便它已先读到旧结构，迁移结束后也不得重复执行。"""
    path = tmp_path / "waiter.db"
    seed = sqlite3.connect(path)
    seed.executescript(OLDEST_SCHEMA)
    seed.commit()
    seed.close()

    # 缩短等待重试间隔，让用例快速覆盖“抢锁失败 -> 等待 -> 复核”路径
    monkeypatch.setattr(db, "_MIGRATION_LOCK_RETRY_INTERVAL_SECONDS", 0.01)

    first_holds = threading.Event()
    release = threading.Event()
    errors = []

    def holder():
        conn = _connect(path)
        # 手工占有与迁移事务相同的保留锁
        conn.execute("BEGIN IMMEDIATE")
        first_holds.set()
        release.wait(timeout=10)
        conn.rollback()
        # 释放后立刻完成迁移
        db.run_migrations(conn)
        conn.close()

    def waiter():
        first_holds.wait(timeout=10)
        try:
            conn = _connect(path)
            db.run_migrations(conn)
            conn.close()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    th = threading.Thread(target=holder)
    tw = threading.Thread(target=waiter)
    th.start()
    tw.start()
    first_holds.wait(timeout=10)
    release.set()
    tw.join(30)
    th.join(30)

    assert errors == []
    result = sqlite3.connect(path)
    assert result.execute(
        "SELECT max(version) FROM schema_migrations").fetchone()[0] == \
        db.LATEST_VERSION
    result.close()


# ---------------------------------------------------------------- 迁移锁等待上限

def test_lock_wait_is_capped_by_configured_timeout(tmp_path, monkeypatch):
    """配置的超时小于连接 busy_timeout 时，总等待以配置为硬上限及时失败。"""
    path = tmp_path / "capped.db"
    _migrate(path).close()  # 已纳管库：排除迁移执行耗时，只测抢锁等待

    monkeypatch.setattr(db, "MIGRATION_LOCK_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(db, "_MIGRATION_LOCK_RETRY_INTERVAL_SECONDS", 0.01)

    holder = _connect(path)
    holder.execute("BEGIN IMMEDIATE")  # 模拟另一进程长期占有写锁
    waiter = _connect(path)            # busy_timeout=30s，远大于配置的 0.5s
    try:
        started = time.monotonic()
        with pytest.raises(db.MigrationError,
                           match=r"无法在 0\.5 秒内取得数据库写锁"):
            db.run_migrations(waiter)
        elapsed = time.monotonic() - started
        # 硬上限生效：确实等到了配置时长，且远小于连接 busy_timeout（30s）
        # ——修复前 BEGIN IMMEDIATE 会在 SQLite 内部阻塞约 30s 才报错
        assert 0.5 <= elapsed < 5
        # 连接状态复原：busy_timeout 恢复原值、未遗留事务
        assert waiter.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert not waiter.in_transaction
    finally:
        waiter.close()
        holder.rollback()
        holder.close()

    # 原始库状态未被改动：仍是全部版本已登记的已纳管库
    check = sqlite3.connect(path)
    assert check.execute(
        "SELECT count(*) FROM schema_migrations").fetchone()[0] == \
        db.LATEST_VERSION
    check.close()


def test_lock_released_within_timeout_migration_succeeds(tmp_path, monkeypatch):
    """持锁方在配置窗口内释放：等待方按既有间隔重试，抢到锁后复核并完成迁移。"""
    path = tmp_path / "released.db"
    seed = sqlite3.connect(path)
    seed.executescript(OLDEST_SCHEMA)
    seed.commit()
    seed.close()

    monkeypatch.setattr(db, "MIGRATION_LOCK_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(db, "_MIGRATION_LOCK_RETRY_INTERVAL_SECONDS", 0.01)

    holder = _connect(path)
    holder.execute("BEGIN IMMEDIATE")
    errors = []

    def waiter():
        try:
            conn = _connect(path)
            db.run_migrations(conn)
            conn.close()
        except Exception as exc:  # noqa: BLE001 - 断言在主线程进行
            errors.append(exc)

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.3)    # 让等待方先经历若干次抢锁失败（走的是重试循环）
    holder.rollback()  # 在超时窗口内释放写锁
    holder.close()
    t.join(30)

    assert errors == []
    result = sqlite3.connect(path)
    assert [r[0] for r in result.execute(
        "SELECT version FROM schema_migrations ORDER BY version")] == \
        list(range(1, db.LATEST_VERSION + 1))
    # 抢到锁后重新读取并校验迁移历史：已登记版本未重复执行，每个补列只出现一次
    cols = [r[1] for r in result.execute("PRAGMA table_info(sessions)")]
    for name in ("tenant_id", "revision", "verification_required"):
        assert cols.count(name) == 1
    assert result.execute(
        "SELECT total_chunks FROM sessions WHERE id='old-sid'").fetchone()[0] == 3
    result.close()


# ---------------------------------------------------------------- 进程级启动路径

def test_get_conn_rejects_startup_on_future_version(tmp_path, monkeypatch):
    """init_db/get_conn 在发现数据库版本高于代码时不发布连接。"""
    path = tmp_path / "startup.db"
    conn = _migrate(path)
    conn.execute(
        "INSERT INTO schema_migrations(version, name, checksum, applied_at)"
        " VALUES (?, 'x', ?, '2026-09-20T00:00:00+00:00')",
        (db.LATEST_VERSION + 1, "b" * 64))
    conn.commit()
    conn.close()

    monkeypatch.setattr(db, "DB_PATH", str(path))
    monkeypatch.setattr(db, "_conn", None)
    try:
        with pytest.raises(db.MigrationError, match="高于当前代码"):
            db.init_db()
        assert db._conn is None  # 失败不发布半成品连接
    finally:
        monkeypatch.setattr(db, "_conn", None)
