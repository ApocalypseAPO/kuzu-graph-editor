from __future__ import annotations

import json
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from kuzu_client import KuzuClient, KuzuClientError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INDEX_FILE = PROJECT_ROOT / "index.html"
DIST_DIR = PROJECT_ROOT / "dist"


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()
    handler.wfile.write(data)


def text_response(handler: BaseHTTPRequestHandler, status: int, content: bytes, content_type: str) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(content)))
    handler.end_headers()
    handler.wfile.write(content)


class AppHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/" or path == "/index.html":
                self._serve_index()
                return

            if path.startswith("/dist/"):
                self._serve_dist(path)
                return

            if path == "/api/graph":
                self._api_graph()
                return

            if path == "/api/debug/graph":
                self._api_debug_graph()
                return

            if path == "/api/schema":
                self._api_schema()
                return

            if path == "/api/node":
                self._api_get_node(parsed.query)
                return

            if path == "/api/search":
                self._api_search(parsed.query)
                return

            json_response(self, 404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            traceback.print_exc()
            json_response(self, 500, {"ok": False, "error": str(e)})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/api/node/update":
                self._api_update_node()
                return

            if path == "/api/node/create":
                self._api_create_node()
                return

            if path == "/api/node/delete":
                self._api_delete_node()
                return

            if path == "/api/relation/create":
                self._api_create_relation()
                return

            if path == "/api/relation/delete":
                self._api_delete_relation()
                return

            json_response(self, 404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            traceback.print_exc()
            json_response(self, 500, {"ok": False, "error": str(e)})

    def _serve_index(self):
        if not INDEX_FILE.exists():
            json_response(self, 404, {"ok": False, "error": "index.html 不存在"})
            return

        content = INDEX_FILE.read_bytes()
        text_response(self, 200, content, "text/html; charset=utf-8")

    def _serve_dist(self, path: str):
        rel = path.removeprefix("/dist/")
        file_path = DIST_DIR / rel

        if not file_path.exists() or not file_path.is_file():
            json_response(self, 404, {"ok": False, "error": f"静态文件不存在: {path}"})
            return

        suffix = file_path.suffix.lower()
        if suffix == ".js":
            content_type = "application/javascript; charset=utf-8"
        elif suffix == ".css":
            content_type = "text/css; charset=utf-8"
        else:
            content_type = "application/octet-stream"

        text_response(self, 200, file_path.read_bytes(), content_type)

    # -------------------------
    # API
    # -------------------------
    def _api_graph(self):
        client = KuzuClient()
        data = client.get_graph_snapshot()
        json_response(self, 200, {"ok": True, "data": data})

    def _api_debug_graph(self):
        client = KuzuClient()
        data = client.get_debug_snapshot()
        json_response(self, 200, {"ok": True, "data": data})

    def _api_schema(self):
        client = KuzuClient()
        data = client.get_schema_summary()
        json_response(self, 200, {"ok": True, "data": data})

    def _api_get_node(self, query: str):
        params = parse_qs(query)
        table = self._get_required_query_param(params, "table")
        node_id = self._get_required_query_param(params, "id")

        client = KuzuClient()
        data = client.get_node_by_id(table, node_id)

        if data is None:
            json_response(self, 404, {"ok": False, "error": "节点不存在"})
            return

        json_response(self, 200, {"ok": True, "data": data})

    def _api_search(self, query: str):
        params = parse_qs(query)
        keyword = self._get_required_query_param(params, "keyword")

        client = KuzuClient()
        data = client.search_nodes_by_name(keyword)
        json_response(self, 200, {"ok": True, "data": data})

    def _api_update_node(self):
        body = self._read_json_body()

        table = body.get("table")
        node_id = body.get("id")
        data = body.get("data")

        if not isinstance(table, str) or not table:
            raise KuzuClientError("table 必须是非空字符串。")
        if not isinstance(node_id, str) or not node_id:
            raise KuzuClientError("id 必须是非空字符串。")
        if not isinstance(data, dict):
            raise KuzuClientError("data 必须是对象。")
        if data.get("id") != node_id:
            raise KuzuClientError("不允许修改节点 id。")

        update_data = dict(data)
        update_data.pop("id", None)
        update_data.pop("table", None)
        update_data.pop("entityType", None)

        client = KuzuClient()
        client.update_node_properties(table, node_id, update_data)

        latest = client.get_node_by_id(table, node_id)
        json_response(self, 200, {"ok": True, "data": latest})

    def _api_create_node(self):
        body = self._read_json_body()

        table = body.get("table")
        data = body.get("data")

        if not isinstance(table, str) or not table:
            raise KuzuClientError("table 必须是非空字符串。")
        if not isinstance(data, dict):
            raise KuzuClientError("data 必须是对象。")

        client = KuzuClient()
        node_id = client.create_node(table, data)
        latest = client.get_node_by_id(table, node_id)

        json_response(self, 200, {
            "ok": True,
            "data": {
                "message": "节点创建成功",
                "id": node_id,
                "node": latest
            }
        })

    def _api_delete_node(self):
        body = self._read_json_body()

        table = body.get("table")
        node_id = body.get("id")

        if not isinstance(table, str) or not table:
            raise KuzuClientError("table 必须是非空字符串。")
        if not isinstance(node_id, str) or not node_id:
            raise KuzuClientError("id 必须是非空字符串。")

        client = KuzuClient()
        client.delete_node(table, node_id)
        json_response(self, 200, {"ok": True, "data": {"message": "节点删除成功"}})

    def _api_create_relation(self):
        body = self._read_json_body()

        from_table = body.get("fromTable")
        from_id = body.get("fromId")
        rel_table = body.get("relTable")
        to_table = body.get("toTable")
        to_id = body.get("toId")

        client = KuzuClient()
        client.create_relation(from_table, from_id, rel_table, to_table, to_id)
        json_response(self, 200, {"ok": True, "data": {"message": "关系创建成功"}})

    def _api_delete_relation(self):
        body = self._read_json_body()

        from_table = body.get("fromTable")
        from_id = body.get("fromId")
        rel_table = body.get("relTable")
        to_table = body.get("toTable")
        to_id = body.get("toId")

        client = KuzuClient()
        client.delete_relation(from_table, from_id, rel_table, to_table, to_id)
        json_response(self, 200, {"ok": True, "data": {"message": "关系删除成功"}})

    # -------------------------
    # 工具方法
    # -------------------------
    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            raise KuzuClientError("请求体不是合法 JSON。")

    def _get_required_query_param(self, params: dict, key: str) -> str:
        values = params.get(key)
        if not values or not isinstance(values[0], str) or not values[0]:
            raise KuzuClientError(f"缺少参数: {key}")
        return values[0]

    def log_message(self, format, *args):
        print(f"[HTTP] {self.address_string()} - {format % args}")


def run_server(host: str = "127.0.0.1", port: int = 8000):
    server = ThreadingHTTPServer((host, port), AppHandler)
    print(f"本地服务已启动: http://{host}:{port}")
    print(f"调试接口: http://{host}:{port}/api/debug/graph")
    print("按 Ctrl+C 停止服务。")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
