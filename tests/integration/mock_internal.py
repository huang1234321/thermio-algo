"""中台 internal 面测试替身（CODE-TST-02：fake 外部服务）。

实现 platform.md §11 中 algo 白名单的四个端点（GET asset-snapshot /
POST fdd/findings / POST fdd/reports / GET fdd/findings），行为对齐
M6-fdd §3.2/§6：
- Bearer 校验（断言 algo 客户端真的带了凭证头）；
- findings 提交按活跃 upsert 语义落内存（同 (equipment, rule_key) 活跃行刷新）；
- GET findings 返回 FddFindingListItem 形状（M6 §4.2，extra 字段不裁剪语义）。
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


class InternalState:
    """mock 的内存落库面（线程锁保护；测试直接断言此对象）。"""

    def __init__(self, snapshot: dict[str, Any], svc_token: str) -> None:
        self.snapshot = snapshot
        self.svc_token = svc_token
        self.findings_posts: list[dict[str, Any]] = []  # 原始报文留档
        self.report_posts: list[dict[str, Any]] = []
        self.proposal_posts: list[dict[str, Any]] = []  # IMPL-19：optimizer 信封留档
        self.active: dict[tuple[str, str], dict[str, Any]] = {}  # (eq, rule) → 行
        self.resolved: list[dict[str, Any]] = []
        self.snapshot_requests: list[dict[str, str]] = []
        self._lock = threading.Lock()

    def apply_findings(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self.findings_posts.append(payload)
            eq_meta = {e["equipment_id"]: e for e in self.snapshot["equipments"]}
            for hit in payload.get("hits", []):
                key = (hit["equipment_id"], hit["rule_key"])
                if key in self.active:  # 持续命中 = 刷新（不产生新行）
                    row = self.active[key]
                    row["last_detected_at"] = hit["last_detected_at"]
                    row["severity"] = hit["severity"]
                    row["title"] = hit["title"]
                else:
                    eq = eq_meta.get(hit["equipment_id"], {})
                    self.active[key] = {
                        "id": str(uuid.uuid4()),
                        "building_id": eq.get("building_id"),
                        "equipment": {
                            "id": hit["equipment_id"],
                            "name": eq.get("name") or eq.get("local_id") or hit["equipment_id"],
                            "local_id": eq.get("local_id"),
                            "equipment_type": eq.get("equipment_type", "unknown"),
                        },
                        "rule_key": hit["rule_key"],
                        "severity": hit["severity"],
                        "status": "open",
                        "title": hit["title"],
                        "suggested_action": hit.get("suggested_action"),
                        "algo_version": payload["algo_version"],
                        "first_detected_at": hit["first_detected_at"],
                        "last_detected_at": hit["last_detected_at"],
                        "resolved_at": None,
                        "ignored_at": None,
                        "review": None,
                        "created_at": hit["first_detected_at"],
                    }
            for cleared in payload.get("cleared", []):
                key = (cleared["equipment_id"], cleared["rule_key"])
                row = self.active.pop(key, None)
                if row is not None:  # 无活跃行则忽略（幂等）
                    row["status"] = "resolved"
                    row["resolved_at"] = cleared["cleared_at"]
                    self.resolved.append(row)

    def list_findings(self) -> list[dict[str, Any]]:
        with self._lock:
            return [*self.active.values(), *self.resolved]


def make_server(state: InternalState) -> tuple[ThreadingHTTPServer, str]:
    """起 mock；返回 (server, base_url)。测试结束后 server.shutdown()。"""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # 静默访问日志
            return

        def _json(self, code: int, body: Any) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _authed(self) -> bool:
            got = self.headers.get("authorization", "")
            if got != f"Bearer {state.svc_token}":
                self._json(401, {"reason_code": "auth.service_unauthorized"})
                return False
            return True

        def do_GET(self) -> None:
            u = urlparse(self.path)
            if not self._authed():
                return
            if u.path == "/internal/algo/asset-snapshot":
                q = parse_qs(u.query)
                state.snapshot_requests.append(
                    {"updated_since": (q.get("updated_since") or [""])[0]}
                )
                self._json(200, state.snapshot)
                return
            if u.path == "/internal/fdd/findings":
                items = state.list_findings()
                self._json(200, {"items": items, "next_cursor": None})
                return
            self._json(404, {"reason_code": "common.not_found"})

        def do_POST(self) -> None:
            if not self._authed():
                return
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/internal/proposals":
                # M5 §3.8 最小行为：必填校验 → 201（client_ref 回显；幂等去重不模拟——
                # R2 过渡态，api 侧同样不去重）
                required = (
                    "proposal_id",
                    "algo",
                    "algo_version",
                    "target",
                    "action",
                    "previous_value",
                    "rationale",
                    "expected_saving_kw",
                    "confidence",
                    "evidence",
                    "expires_at",
                )
                missing = [k for k in required if payload.get(k) is None]
                if missing:
                    self._json(
                        422,
                        {
                            "reason_code": "proposal.payload_invalid",
                            "details": {"cause": "required", "fields": missing},
                        },
                    )
                    return
                with state._lock:
                    state.proposal_posts.append(payload)
                self._json(
                    201,
                    {
                        "proposal_id": f"srv-{len(state.proposal_posts)}",
                        "client_ref": payload["proposal_id"],
                        "status": "pending",
                        "expires_at": payload["expires_at"],
                    },
                )
                return
            if self.path == "/internal/fdd/findings":
                state.apply_findings(payload)
                self._json(200, {"ok": True})
                return
            if self.path == "/internal/fdd/reports":
                state.report_posts.append(payload)
                self._json(201, {"ok": True})
                return
            self._json(404, {"reason_code": "common.not_found"})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    return server, base


__all__ = ["UTC", "InternalState", "datetime", "make_server"]
