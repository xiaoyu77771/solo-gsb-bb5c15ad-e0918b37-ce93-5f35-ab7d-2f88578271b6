"""统一事务边界测试：故障注入与 4xx/异常回滚、惰性取消独立持久化。

覆盖两类保证：
1. 每个写接口（单片/批量领取、跨会话调度、完成、失败、回退、提交、分支、
   交付凭证签发/兑换/撤销、配额、取消、验收领取/决定）在写入中途抛出数据库
   异常时整体回滚：连接不残留打开的事务，后续请求可继续使用，账务（租户
   used/reserved）、分片状态、调度游标均无半成品。
2. 普通 4xx/404 校验失败不打开任何写事务；预留到期触发的惰性取消即使最终
   返回 409（或调度返回 granted=null）也作为独立事务完整持久化，并立即
   清除上传租约与验收租约。
"""
import os
import tempfile

import pytest

# 与 test_api.py 一致：导入 app 前指定测试数据库（两个模块共用进程级单连接，
# 仅首个被收集的模块设置的路径生效；各自使用独立租户避免串扰）
os.environ.setdefault("UPLOAD_DB_PATH", os.path.join(tempfile.mkdtemp(), "test-tx.db"))
os.environ.setdefault("DEFAULT_LEASE_TTL_SECONDS", "300")
os.environ.setdefault("DEFAULT_RESERVATION_TTL_SECONDS", "3600")

from fastapi.testclient import TestClient  # noqa: E402

from app.db import DB_LOCK, get_conn, set_sql_fault  # noqa: E402
from app.main import app  # noqa: E402

# 故障以 500 透出，故关闭 TestClient 的服务端异常抛出，断言 HTTP 层面行为
client = TestClient(app, raise_server_exceptions=False)
get_conn()  # 显式建库建表

TENANT = "tenant-tx-boundary"
DECLARED = 1024

_r = client.put(f"/tenants/{TENANT}/quota", json={"quota_bytes": 10 ** 15})
assert _r.status_code == 200, _r.text


# ---------------------------------------------------------------- 工具函数

def set_quota(tenant_id, quota_bytes):
    return client.put(f"/tenants/{tenant_id}/quota", json={"quota_bytes": quota_bytes})


def quota_view(tenant_id):
    return client.get(f"/tenants/{tenant_id}/quota").json()


def make_session(total_chunks=3, tenant_id=TENANT, declared_bytes=DECLARED,
                 reservation_ttl_seconds=None, verification_required=False,
                 dependencies=None, file_digest="f" * 64):
    body = {
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256",
        "tenant_id": tenant_id,
        "declared_bytes": declared_bytes,
        "verification_required": verification_required,
    }
    if reservation_ttl_seconds is not None:
        body["reservation_ttl_seconds"] = reservation_ttl_seconds
    if dependencies is not None:
        body["dependencies"] = {str(k): v for k, v in dependencies.items()}
    resp = client.post("/sessions", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease(session_id, index):
    return client.post(f"/sessions/{session_id}/chunks/{index}/lease", json={})


def complete(session_id, index, token, digest="a" * 64):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/complete",
        json={"lease_token": token, "chunk_digest": digest},
    )


def fail_report(session_id, index, token):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/fail",
        json={"lease_token": token},
    )


def batch_lease(session_id, count):
    return client.post(f"/sessions/{session_id}/leases/batch", json={"count": count})


def dispatch(queue_key, session_ids):
    return client.post("/dispatch/lease",
                       json={"queue_key": queue_key, "session_ids": session_ids})


def cancel(session_id):
    return client.post(f"/sessions/{session_id}/cancel")


def session_row(session_id):
    return get_conn().execute(
        "SELECT * FROM sessions WHERE id = ?", (session_id,)
    ).fetchone()


def chunk_row(session_id, index):
    return get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
        (session_id, index),
    ).fetchone()


def count_sessions(predicate_sql="", params=()):
    return get_conn().execute(
        f"SELECT COUNT(*) AS n FROM sessions {predicate_sql}", params
    ).fetchone()["n"]


def drain_global_verification_queue():
    """其他测试模块可能在全局验收队列遗留待验收分片，本模块用例前清空。"""
    conn = get_conn()
    with DB_LOCK:
        conn.execute("DELETE FROM verification_leases")
        conn.execute("DELETE FROM verification_decisions")
        conn.execute(
            "UPDATE chunks SET status = 'completed' WHERE status = 'awaiting_verification'"
        )


def lease_complete_awaiting(session_id, index, digest="a" * 64):
    token = lease(session_id, index).json()["lease_token"]
    resp = complete(session_id, index, token, digest)
    assert resp.status_code == 200, resp.text
    return resp


def verification_lease():
    return client.post("/verifications/lease", json={})


def decide(session_id, index, token, decision):
    return client.post(
        f"/verifications/{session_id}/{index}/decide",
        json={"verification_token": token, "decision": decision},
    )


def expire_reservation(session_id, when="2000-01-01T00:00:00+00:00"):
    conn = get_conn()
    with DB_LOCK:
        conn.execute(
            "UPDATE sessions SET reservation_expires_at = ? WHERE id = ?",
            (when, session_id),
        )


# ---------------------------------------------------------------- 故障注入器


class SqlFaultInjector:
    """通过 db 层测试钩子注入一次性故障：匹配 SQL 子串的第 (allow+1) 次 DML
    调用（仅在显式写事务内）抛 OperationalError，触发后自动解除。

    经 app.db.set_sql_fault 把共享连接升级为 _FaultableConnection 子类实例实现
    （Python 3.11 的 sqlite3.Connection 为不可变类型，无法直接在类/实例上替换
    execute；子类方法可覆盖），事务内由内部辅助函数直接执行的 SQL 也能命中。
    """

    def __init__(self, match: str, allow_matching: int = 0):
        self.match = match
        self.allow = allow_matching

    def install(self):
        # 通过 db 层钩子升级共享连接并设置故障；用例结束由 autouse fixture
        # _reset_sql_fault 统一清除，避免已消耗的计数/可故障连接泄漏
        set_sql_fault(self.match, self.allow)


@pytest.fixture(autouse=True)
def _reset_sql_fault():
    yield
    set_sql_fault(None)


def assert_connection_clean():
    """故障/4xx 之后：连接上不存在遗留的打开事务，可立即继续服务。"""
    assert get_conn().in_transaction is False


def assert_health_ok():
    r = client.get("/healthz")
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------- 4xx 不遗留事务

@pytest.mark.parametrize("setup_kind,expect_detail_substr", [
    ("lease_completed", "already completed"),
    ("complete_bad_token", "lease token mismatch"),
    ("fail_bad_token", "lease token mismatch"),
    ("reopen_bad_credentials", "lease token mismatch"),
    ("finalize_incomplete", "upload incomplete"),
    ("fork_not_completed", "completed chunks only"),
    ("quota_exceeded", "quota exceeded"),
    ("decide_bad_token", "verification token mismatch"),
])
def test_4xx_responses_leave_no_open_transaction(setup_kind, expect_detail_substr):
    s = make_session(total_chunks=3)
    sid = s["id"]

    if setup_kind == "lease_completed":
        t = lease(sid, 0).json()["lease_token"]
        assert complete(sid, 0, t).status_code == 200
        r = lease(sid, 0)
    elif setup_kind == "complete_bad_token":
        lease(sid, 0)
        r = complete(sid, 0, "wrong-token")
    elif setup_kind == "fail_bad_token":
        lease(sid, 0)
        r = fail_report(sid, 0, "wrong-token")
    elif setup_kind == "reopen_bad_credentials":
        # 未完成分片先被“chunk is not completed”拦截
        r = client.post(f"/sessions/{sid}/chunks/0/reopen",
                        json={"lease_token": "x", "expected_digest": "d"})
        expect_detail_substr = "not completed"
    elif setup_kind == "finalize_incomplete":
        r = client.post(f"/sessions/{sid}/finalize")
    elif setup_kind == "fork_not_completed":
        r = client.post(f"/sessions/{sid}/fork", json={
            "expected_revision": 0, "request_id": f"4xx-{setup_kind}",
            "reuse_chunks": [0]})
    elif setup_kind == "quota_exceeded":
        r = client.post("/sessions", json={
            "total_chunks": 1, "file_digest": "f" * 64,
            "tenant_id": TENANT, "declared_bytes": 10 ** 18})
    else:  # decide_bad_token
        drain_global_verification_queue()
        vs = make_session(total_chunks=1, verification_required=True)
        lease_complete_awaiting(vs["id"], 0)
        r = decide(vs["id"], 0, "no-such-verification-token", "approve")

    assert r.status_code == 409, r.text
    assert expect_detail_substr in r.json()["detail"]
    assert_connection_clean()

    # 关键回归点：后续无关请求的提交不会把前一个失败请求的任何半成品带上。
    # 一次成功的领取必须只产生它自己的一条租约。
    before = get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunks WHERE status = 'leased'"
    ).fetchone()["n"]
    ok = batch_lease(sid, 1)
    assert ok.status_code == 200, ok.text
    after = get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunks WHERE status = 'leased'"
    ).fetchone()["n"]
    assert after == before + 1
    assert_connection_clean()


def test_404_responses_leave_no_open_transaction():
    r = lease("no-such-session", 0)
    assert r.status_code == 404
    assert_connection_clean()
    s = make_session(total_chunks=1)
    # 分片 99 不存在：chunk 级 404
    assert client.post(
        f"/sessions/{s['id']}/chunks/99/complete",
        json={"lease_token": "t", "chunk_digest": "d"}).status_code == 404
    assert_connection_clean()
    assert_health_ok()


# ---------------------------------------------------------------- 故障注入：逐写路径回滚

def test_fault_during_create_session_rolls_back_everything():
    SqlFaultInjector("INSERT INTO sessions (").install()
    before = count_sessions()
    reserved_before = quota_view(TENANT)["reserved_bytes"]

    r = client.post("/sessions", json={
        "total_chunks": 3, "file_digest": "f" * 64,
        "tenant_id": TENANT, "declared_bytes": DECLARED})
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert count_sessions() == before  # 会话与分片行全部回滚
    assert quota_view(TENANT)["reserved_bytes"] == reserved_before  # 账务无半成品

    # 连接继续可用：随后正常创建成功并提交
    s = make_session(total_chunks=2)
    assert session_row(s["id"]) is not None
    assert lease(s["id"], 0).status_code == 200
    assert_health_ok()


def test_fault_during_single_lease_rolls_back():
    s = make_session(total_chunks=1)
    SqlFaultInjector("UPDATE chunks SET status = 'leased'").install()
    r = lease(s["id"], 0)
    assert r.status_code == 500, r.text
    assert_connection_clean()
    row = chunk_row(s["id"], 0)
    assert row["status"] == "pending" and row["lease_token"] is None

    # 旧请求失败后连接仍可用：重新领取成功
    r2 = lease(s["id"], 0)
    assert r2.status_code == 200, r2.text
    assert chunk_row(s["id"], 0)["status"] == "leased"


def test_fault_mid_batch_rolls_back_whole_batch():
    s = make_session(total_chunks=4)
    # 允许第 1 条 UPDATE 成功，第 2 条抛错：整批必须回滚，不能只发出前 1 条
    SqlFaultInjector("UPDATE chunks SET status = 'leased'",
                     allow_matching=1).install()
    r = batch_lease(s["id"], 3)
    assert r.status_code == 500, r.text
    assert_connection_clean()
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND status = 'leased'",
        (s["id"],),
    ).fetchall()
    assert rows == []  # 第 1 条随事务整体回滚

    body = batch_lease(s["id"], 3).json()
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1, 2]
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["active_leases"] == 3


def test_fault_during_dispatch_grant_rolls_back_lease_and_cursor():
    s = make_session(total_chunks=1)
    qkey = "q-tx-fault"
    SqlFaultInjector("INSERT INTO dispatch_cursors").install()
    r = dispatch(qkey, [s["id"]])
    assert r.status_code == 500, r.text
    assert_connection_clean()

    # 分片租约与调度游标同事务回滚：无半成品
    assert chunk_row(s["id"], 0)["status"] == "pending"
    cursor = get_conn().execute(
        "SELECT * FROM dispatch_cursors WHERE queue_key = ?", (qkey,)
    ).fetchone()
    assert cursor is None

    # 连接继续可用：重试调度成功，游标正确建立
    body = dispatch(qkey, [s["id"]]).json()
    assert body["granted"]["chunk_index"] == 0
    assert body["cursor_session_id"] == s["id"]


def test_fault_during_complete_rolls_back():
    s = make_session(total_chunks=1)
    token = lease(s["id"], 0).json()["lease_token"]
    SqlFaultInjector("status = ?, chunk_digest = ?").install()
    r = complete(s["id"], 0, token)
    assert r.status_code == 500, r.text
    assert_connection_clean()
    row = chunk_row(s["id"], 0)
    assert row["status"] == "leased"  # 未变成 completed
    assert row["chunk_digest"] is None

    r2 = complete(s["id"], 0, token)
    assert r2.status_code == 200, r2.text
    assert chunk_row(s["id"], 0)["status"] == "completed"


def test_fault_during_fail_rolls_back():
    s = make_session(total_chunks=1)
    token = lease(s["id"], 0).json()["lease_token"]
    SqlFaultInjector("failure_count = ?").install()
    r = fail_report(s["id"], 0, token)
    assert r.status_code == 500, r.text
    assert_connection_clean()
    row = chunk_row(s["id"], 0)
    assert row["status"] == "leased" and row["lease_token"] == token
    assert row["failure_count"] == 0  # 计数未递增

    r2 = fail_report(s["id"], 0, token)
    assert r2.status_code == 200, r2.text
    assert r2.json()["failure_count"] == 1  # 不是 2：失败的那次已回滚
    assert chunk_row(s["id"], 0)["failure_count"] == 1


def test_fault_during_reopen_rolls_back_all_resets():
    s = make_session(total_chunks=2, dependencies={1: [0]})
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200

    # 分片批量重置先执行、revision UPDATE 最后抛错：全部回滚
    SqlFaultInjector("UPDATE sessions SET revision = revision + 1").install()
    r = client.post(f"/sessions/{s['id']}/chunks/0/reopen",
                    json={"lease_token": t0, "expected_digest": "0" * 64})
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert session_row(s["id"])["revision"] == 0
    assert chunk_row(s["id"], 0)["status"] == "completed"
    assert chunk_row(s["id"], 1)["status"] == "completed"
    assert chunk_row(s["id"], 0)["chunk_digest"] == "0" * 64

    r2 = client.post(f"/sessions/{s['id']}/chunks/0/reopen",
                     json={"lease_token": t0, "expected_digest": "0" * 64})
    assert r2.status_code == 200, r2.text
    assert session_row(s["id"])["revision"] == 1
    assert chunk_row(s["id"], 1)["status"] == "pending"


def test_fault_during_finalize_keeps_session_active():
    tid = "tenant-tx-finalize-fault"
    set_quota(tid, 10 ** 9)
    s = make_session(total_chunks=1, tenant_id=tid, declared_bytes=3000)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200

    SqlFaultInjector("UPDATE sessions SET status = 'finalized'").install()
    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert session_row(s["id"])["status"] == "active"
    view = quota_view(tid)
    assert view["used_bytes"] == 0 and view["reserved_bytes"] == 3000  # 未转量

    r2 = client.post(f"/sessions/{s['id']}/finalize")
    assert r2.status_code == 200, r2.text
    view2 = quota_view(tid)
    assert view2["used_bytes"] == 3000 and view2["reserved_bytes"] == 0


def test_fault_during_fork_rolls_back_copied_session():
    tid = "tenant-tx-fork-fault"
    set_quota(tid, 10 ** 9)
    s = make_session(total_chunks=1, tenant_id=tid, declared_bytes=2000)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    reserved_before = quota_view(tid)["reserved_bytes"]

    # fork_requests 是复制事务里最后一条语句：失败后新会话/分片/依赖/预留全回滚
    SqlFaultInjector("INSERT INTO fork_requests").install()
    r = client.post(f"/sessions/{s['id']}/fork", json={
        "expected_revision": 0, "request_id": "fork-fault", "reuse_chunks": [0]})
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert count_sessions("WHERE source_session_id = ?", (s["id"],)) == 0
    assert quota_view(tid)["reserved_bytes"] == reserved_before  # 未多预留

    r2 = client.post(f"/sessions/{s['id']}/fork", json={
        "expected_revision": 0, "request_id": "fork-retry", "reuse_chunks": [0]})
    assert r2.status_code == 201, r2.text
    assert quota_view(tid)["reserved_bytes"] == reserved_before + 2000


def test_fault_during_ticket_issue_rolls_back():
    s = make_session(total_chunks=1)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200

    SqlFaultInjector("INSERT INTO delivery_tickets (").install()
    r = client.post(f"/sessions/{s['id']}/delivery-tickets",
                    json={"expires_in_seconds": 600})
    assert r.status_code == 500, r.text
    assert_connection_clean()
    n = get_conn().execute("SELECT COUNT(*) AS n FROM delivery_tickets").fetchone()["n"]
    # 不新增凭证（其它测试可能有遗留行，用前后差值更稳妥——这里取本会话计数为 0）
    before_retry = n
    r2 = client.post(f"/sessions/{s['id']}/delivery-tickets",
                     json={"expires_in_seconds": 600})
    assert r2.status_code == 201, r2.text
    after = get_conn().execute(
        "SELECT COUNT(*) AS n FROM delivery_tickets").fetchone()["n"]
    assert after == before_retry + 1


def test_fault_during_redeem_keeps_ticket_active():
    s = make_session(total_chunks=1)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
    ticket = client.post(f"/sessions/{s['id']}/delivery-tickets",
                         json={"expires_in_seconds": 600}).json()["ticket_token"]
    token_hash = __import__("hashlib").sha256(ticket.encode()).hexdigest()

    # 消费 UPDATE 先成功、幂等记录插入最后抛错：同事务回滚 => 凭证仍 active
    SqlFaultInjector("INSERT INTO delivery_ticket_redemptions").install()
    r = client.post("/delivery-tickets/redeem",
                    json={"token": ticket, "request_id": "redeem-fault"})
    assert r.status_code == 500, r.text
    assert_connection_clean()
    row = get_conn().execute(
        "SELECT status FROM delivery_tickets WHERE token_hash = ?", (token_hash,)
    ).fetchone()
    assert row["status"] == "active"
    n = get_conn().execute(
        "SELECT COUNT(*) AS n FROM delivery_ticket_redemptions"
        " WHERE request_id = 'redeem-fault'").fetchone()["n"]
    assert n == 0

    # 同一 request_id 仍可首次兑换成功（失败未留下幂等记录）
    r2 = client.post("/delivery-tickets/redeem",
                     json={"token": ticket, "request_id": "redeem-fault"})
    assert r2.status_code == 200, r2.text
    assert r2.json()["request_id"] == "redeem-fault"


def test_fault_during_verification_lease_rolls_back():
    drain_global_verification_queue()
    s = make_session(total_chunks=1, verification_required=True)
    lease_complete_awaiting(s["id"], 0)

    SqlFaultInjector("INSERT INTO verification_leases").install()
    r = verification_lease()
    assert r.status_code == 500, r.text
    assert_connection_clean()
    n = get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],)).fetchone()["n"]
    assert n == 0
    assert chunk_row(s["id"], 0)["status"] == "awaiting_verification"

    r2 = verification_lease()
    assert r2.status_code == 201, r2.text
    assert r2.json()["chunk_index"] == 0


def test_fault_during_verification_decide_rolls_back():
    drain_global_verification_queue()
    s = make_session(total_chunks=1, verification_required=True)
    lease_complete_awaiting(s["id"], 0)
    vtoken = verification_lease().json()["verification_token"]

    # 决定顺序：分片 UPDATE -> 决定 INSERT -> 验收租约 DELETE；在最后一步炸
    SqlFaultInjector("DELETE FROM verification_leases").install()
    r = decide(s["id"], 0, vtoken, "approve")
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert chunk_row(s["id"], 0)["status"] == "awaiting_verification"
    n_dec = get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_decisions").fetchone()["n"]
    n_lease = get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],)).fetchone()["n"]
    assert n_dec == 0 and n_lease == 1

    # 重试：同一令牌仍有效（无决定记录、租约未删），决定成功
    r2 = decide(s["id"], 0, vtoken, "approve")
    assert r2.status_code == 200, r2.text
    assert chunk_row(s["id"], 0)["status"] == "completed"


def test_fault_on_decision_insert_rolls_back_chunk_and_keeps_snapshot_atomic():
    """快照 INSERT（决定记录）在分片 UPDATE 之后执行：若该步失败，分片状态变更
    必须随同一事务整体回滚；重试成功后快照与分片状态一并落库，重放读取到首次快照。"""
    drain_global_verification_queue()
    s = make_session(total_chunks=1, verification_required=True)
    lease_complete_awaiting(s["id"], 0)
    vtoken = verification_lease().json()["verification_token"]

    # 在决定记录 INSERT（携带快照列）上注入故障：前置的分片 UPDATE 一并回滚
    SqlFaultInjector("INSERT INTO verification_decisions").install()
    r = decide(s["id"], 0, vtoken, "approve")
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert chunk_row(s["id"], 0)["status"] == "awaiting_verification"
    assert get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_decisions").fetchone()["n"] == 0

    # 重试成功：分片转 completed，决定记录携带与首次响应一致的快照
    r2 = decide(s["id"], 0, vtoken, "approve")
    assert r2.status_code == 200, r2.text
    snap = get_conn().execute(
        "SELECT result_status, result_chunk_digest, decided_at"
        " FROM verification_decisions").fetchone()
    assert snap["result_status"] == "completed"
    assert snap["result_chunk_digest"] == "a" * 64
    # 重放读取快照
    r3 = decide(s["id"], 0, vtoken, "approve")
    assert r3.status_code == 200
    b3 = r3.json()
    assert b3["idempotent_replay"] is True
    assert b3["status"] == "completed"
    assert b3["chunk_digest"] == "a" * 64


def test_fault_during_quota_put_rolls_back():
    tid = "tenant-tx-quota-fault"
    assert client.get(f"/tenants/{tid}/quota").status_code == 404
    SqlFaultInjector("INSERT INTO tenants").install()
    r = set_quota(tid, 1234)
    assert r.status_code == 500, r.text
    assert_connection_clean()
    assert client.get(f"/tenants/{tid}/quota").status_code == 404  # 未落库

    r2 = set_quota(tid, 1234)
    assert r2.status_code == 200, r2.text
    assert client.get(f"/tenants/{tid}/quota").json()["quota_bytes"] == 1234


def test_fault_during_cancel_rolls_back_all_cleanup():
    drain_global_verification_queue()
    tid = "tenant-tx-cancel-fault"
    set_quota(tid, 10 ** 9)
    s = make_session(total_chunks=2, tenant_id=tid, declared_bytes=500,
                     verification_required=True)
    lease_complete_awaiting(s["id"], 0)            # 待验收 + 验收租约
    vg = verification_lease()
    assert vg.status_code == 201
    up_token = lease(s["id"], 1).json()["lease_token"]  # 在途上传租约
    reserved_before = quota_view(tid)["reserved_bytes"]

    # 取消事务最后一条（会话置 cancelled）抛错：前面的租约/验收清理全部回滚
    SqlFaultInjector("cancelled_at = ?").install()
    r = cancel(s["id"])
    assert r.status_code == 500, r.text
    assert_connection_clean()

    row = session_row(s["id"])
    assert row["status"] == "active" and row["cancelled_at"] is None
    assert chunk_row(s["id"], 0)["status"] == "awaiting_verification"
    c1 = chunk_row(s["id"], 1)
    assert c1["status"] == "leased" and c1["lease_token"] == up_token
    assert get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],)).fetchone()["n"] == 1
    assert quota_view(tid)["reserved_bytes"] == reserved_before  # 预留未释放

    # 重试取消成功：账务、上传租约、验收租约全部正确清理
    r2 = cancel(s["id"])
    assert r2.status_code == 200, r2.text
    assert r2.json()["released_bytes"] == 500
    assert session_row(s["id"])["status"] == "cancelled"
    assert chunk_row(s["id"], 1)["status"] == "pending"
    assert chunk_row(s["id"], 1)["lease_token"] is None
    assert chunk_row(s["id"], 0)["status"] == "pending"
    assert chunk_row(s["id"], 0)["chunk_digest"] is None
    assert get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],)).fetchone()["n"] == 0
    assert quota_view(tid)["reserved_bytes"] == reserved_before - 500


# ---------------------------------------------------------------- 惰性取消的独立持久化

def test_lazy_cancel_on_409_persists_and_clears_both_lease_kinds():
    drain_global_verification_queue()
    tid = "tenant-tx-lazy-409"
    set_quota(tid, 10 ** 9)
    s = make_session(total_chunks=2, tenant_id=tid, declared_bytes=700,
                     verification_required=True)
    lease_complete_awaiting(s["id"], 0)                  # 待验收分片
    vg = verification_lease().json()                    # 持有效验收令牌
    up_token = lease(s["id"], 1).json()["lease_token"]  # 持有效上传租约

    expire_reservation(s["id"])
    # 到期后容量在读口径立即释放
    view = quota_view(tid)
    assert view["reserved_bytes"] == 0

    # 对到期会话的写请求：惰性取消独立提交，随后返回 409
    r = lease(s["id"], 1)
    assert r.status_code == 409, r.text
    assert_connection_clean()

    row = session_row(s["id"])
    assert row["status"] == "cancelled"
    assert row["cancelled_at"] is not None
    # 上传租约立即清除
    c1 = chunk_row(s["id"], 1)
    assert c1["status"] == "pending"
    assert c1["lease_token"] is None and c1["lease_expires_at"] is None
    # 待验收分片按 reject 同口径清除，验收租约行删除
    c0 = chunk_row(s["id"], 0)
    assert c0["status"] == "pending"
    assert c0["chunk_digest"] is None and c0["completed_at"] is None
    assert c0["lease_token"] is None
    assert get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],)).fetchone()["n"] == 0
    # 旧验收令牌与旧上传令牌立即失效
    assert decide(s["id"], 0, vg["verification_token"], "approve").status_code == 409
    assert complete(s["id"], 1, up_token, "1" * 64).status_code == 409
    # 账务：预留已释放且不重复释放，used 仍为 0
    view2 = quota_view(tid)
    assert view2["reserved_bytes"] == 0 and view2["used_bytes"] == 0

    # 失败请求之后连接继续可用：其它会话的写正常提交
    other = make_session(total_chunks=1, tenant_id=tid, declared_bytes=100)
    assert lease(other["id"], 0).status_code == 200
    assert quota_view(tid)["reserved_bytes"] == 100


def test_lazy_cancel_via_decide_and_finalize_persists_on_409():
    drain_global_verification_queue()
    s = make_session(total_chunks=1, verification_required=True)
    lease_complete_awaiting(s["id"], 0)
    vtoken = verification_lease().json()["verification_token"]
    expire_reservation(s["id"])

    # decide 路径：先惰性取消（独立提交），再以 409 拒绝
    r = decide(s["id"], 0, vtoken, "approve")
    assert r.status_code == 409, r.text
    assert_connection_clean()
    assert session_row(s["id"])["status"] == "cancelled"
    assert chunk_row(s["id"], 0)["status"] == "pending"
    assert chunk_row(s["id"], 0)["chunk_digest"] is None
    assert get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],)).fetchone()["n"] == 0

    # finalize 路径：另一会话到期后提交同样 409，但取消已落库
    s2 = make_session(total_chunks=1)
    t = lease(s2["id"], 0).json()["lease_token"]
    assert complete(s2["id"], 0, t, "0" * 64).status_code == 200
    expire_reservation(s2["id"])
    r2 = client.post(f"/sessions/{s2['id']}/finalize")
    assert r2.status_code == 409
    assert_connection_clean()
    assert session_row(s2["id"])["status"] == "cancelled"
    assert session_row(s2["id"])["finalized_at"] is None  # 未被错误提交
    assert_health_ok()


def test_lazy_cancel_via_batch_and_fail_persists_on_409():
    # 覆盖惰性取消的另外两个写入口：批量领取与失败上报
    s = make_session(total_chunks=2)
    up_token = lease(s["id"], 0).json()["lease_token"]
    expire_reservation(s["id"])
    r = batch_lease(s["id"], 2)
    assert r.status_code == 409, r.text
    assert_connection_clean()
    assert session_row(s["id"])["status"] == "cancelled"
    c0 = chunk_row(s["id"], 0)
    assert c0["status"] == "pending" and c0["lease_token"] is None
    # 旧令牌立即失效
    assert fail_report(s["id"], 0, up_token).status_code == 409
    assert complete(s["id"], 0, up_token, "0" * 64).status_code == 409

    s2 = make_session(total_chunks=1)
    t = lease(s2["id"], 0).json()["lease_token"]
    expire_reservation(s2["id"])
    r2 = fail_report(s2["id"], 0, t)
    assert r2.status_code == 409, r2.text
    assert_connection_clean()
    assert session_row(s2["id"])["status"] == "cancelled"
    c0b = chunk_row(s2["id"], 0)
    assert c0b["status"] == "pending" and c0b["lease_token"] is None
    assert c0b["failure_count"] == 0  # 失败上报被取消拦截，未递增
    assert_health_ok()


def test_lazy_cancel_during_dispatch_scan_persists_without_cursor_change():
    s = make_session(total_chunks=1)
    up_token = lease(s["id"], 0).json()["lease_token"]
    expire_reservation(s["id"])
    qkey = "q-tx-lazy-dispatch"

    # 调度扫描命中到期会话：响应 200 且 granted=null，但取消已独立提交，
    # 不与游标共事务（游标不应被建立）
    r = dispatch(qkey, [s["id"]])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["granted"] is None
    assert body["cursor_session_id"] is None
    assert body["unavailable"][0]["reason"] == "reservation_expired"
    assert_connection_clean()

    assert session_row(s["id"])["status"] == "cancelled"
    c0 = chunk_row(s["id"], 0)
    assert c0["status"] == "pending"
    assert c0["lease_token"] is None and c0["lease_expires_at"] is None
    assert complete(s["id"], 0, up_token, "0" * 64).status_code == 409
    assert get_conn().execute(
        "SELECT COUNT(*) AS n FROM dispatch_cursors WHERE queue_key = ?", (qkey,)
    ).fetchone()["n"] == 0


def test_lazy_cancel_then_later_request_does_not_roll_it_back():
    """回归点：惰性取消在请求 A 内独立提交；请求 B 的失败绝不能把 A 的取消回滚。"""
    s = make_session(total_chunks=1)
    lease(s["id"], 0)
    expire_reservation(s["id"])
    assert lease(s["id"], 0).status_code == 409  # A：惰性取消并提交
    assert session_row(s["id"])["status"] == "cancelled"

    # B：同一已取消会话再次写入，仍是 409；A 的取消状态不受影响
    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert session_row(s["id"])["status"] == "cancelled"
    assert_connection_clean()

    # C：对另一个正常会话注入故障；回滚只影响 C 自己
    other = make_session(total_chunks=1)
    set_sql_fault("UPDATE chunks SET status = 'leased'")
    try:
        rc = lease(other["id"], 0)
        assert rc.status_code == 500
    finally:
        set_sql_fault(None)
    assert_connection_clean()
    # A 仍为 cancelled；C 的分片未领取
    assert session_row(s["id"])["status"] == "cancelled"
    assert chunk_row(other["id"], 0)["status"] == "pending"
    # 连接仍可继续服务
    assert lease(other["id"], 0).status_code == 200
