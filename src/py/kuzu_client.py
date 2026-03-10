from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid


PrimitiveValue = Optional[int | float | bool | str]


class KuzuClientError(Exception):
    """KuzuClient 业务异常。"""
    pass


class KuzuClient:
    def __init__(self) -> None:
        self.project_root = Path(__file__).resolve().parents[2]
        self.db_path = self.project_root / "db"

        import kuzu

        self.db = kuzu.Database(str(self.db_path))
        self.conn = kuzu.Connection(self.db)

    # -------------------------
    # 基础查询
    # -------------------------
    def execute(self, cypher: str):
        return self.conn.execute(cypher)

    def fetch_all_dict(self, cypher: str) -> List[Dict[str, Any]]:
        result = self.conn.execute(cypher)
        column_names = result.get_column_names()
        rows: List[Dict[str, Any]] = []

        while result.has_next():
            row = result.get_next()
            rows.append(dict(zip(column_names, row)))

        return rows

    # -------------------------
    # 元信息
    # -------------------------
    def show_tables(self) -> List[Dict[str, Any]]:
        return self.fetch_all_dict("CALL SHOW_TABLES() RETURN *;")

    def _extract_table_name(self, row: Dict[str, Any]) -> Optional[str]:
        for key in ("name", "table_name", "table"):
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return None

    def _extract_table_type(self, row: Dict[str, Any]) -> str:
        for key in ("type", "table_type"):
            value = row.get(key)
            if value is not None:
                return str(value).upper()
        return ""

    def get_node_tables(self) -> List[str]:
        tables = self.show_tables()
        names: List[str] = []

        for row in tables:
            table_name = self._extract_table_name(row)
            table_type = self._extract_table_type(row)

            if table_name and "NODE" in table_type:
                names.append(table_name)

        return sorted(names)

    def get_rel_tables(self) -> List[str]:
        tables = self.show_tables()
        names: List[str] = []

        for row in tables:
            table_name = self._extract_table_name(row)
            table_type = self._extract_table_type(row)

            if table_name and ("REL" in table_type or "EDGE" in table_type):
                names.append(table_name)

        return sorted(names)

    def table_info(self, table_name: str) -> List[Dict[str, Any]]:
        self._validate_identifier(table_name)
        return self.fetch_all_dict(f"CALL TABLE_INFO('{table_name}') RETURN *;")

    def get_table_columns(self, table_name: str) -> List[str]:
        info = self.table_info(table_name)
        columns: List[str] = []

        for row in info:
            col = row.get("name")
            if isinstance(col, str) and col.strip():
                columns.append(col)

        return columns

    def get_schema_summary(self) -> Dict[str, Any]:
        node_tables = []
        rel_tables = []

        for table in self.get_node_tables():
            node_tables.append({
                "name": table,
                "columns": self.get_table_columns(table)
            })

        for table in self.get_rel_tables():
            rel_tables.append({
                "name": table,
                "columns": self.get_table_columns(table)
            })

        return {
            "nodeTables": node_tables,
            "relTables": rel_tables
        }

    # -------------------------
    # 整图读取
    # -------------------------
    def get_graph_snapshot(self) -> Dict[str, Any]:
        node_tables = self.get_node_tables()
        rel_tables = self.get_rel_tables()

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []

        for table in node_tables:
            columns = self.get_table_columns(table)
            if "id" not in columns:
                continue

            select_fields = ", ".join([f"n.{c} AS {c}" for c in columns])
            cypher = f"MATCH (n:{table}) RETURN {select_fields};"
            rows = self.fetch_all_dict(cypher)

            for row in rows:
                node_id = row.get("id")
                if not isinstance(node_id, str) or not node_id.strip():
                    continue

                label = str(row.get("name") or row.get("title") or node_id)
                nodes.append({
                    "id": node_id,
                    "label": label,
                    "table": table,
                    "data": {
                        "entityType": "node",
                        "table": table,
                        **row
                    }
                })

        node_id_set = {node["id"] for node in nodes}

        for table in rel_tables:
            cypher = (
                f"MATCH (a)-[r:{table}]->(b) "
                f"RETURN a.id AS from_id, b.id AS to_id;"
            )
            rows = self.fetch_all_dict(cypher)

            for index, row in enumerate(rows):
                from_id = row.get("from_id")
                to_id = row.get("to_id")

                if not isinstance(from_id, str) or not from_id.strip():
                    continue
                if not isinstance(to_id, str) or not to_id.strip():
                    continue
                if from_id not in node_id_set or to_id not in node_id_set:
                    continue

                edge_id = f"{table}::{from_id}::{to_id}::{index}"
                edges.append({
                    "id": edge_id,
                    "from": from_id,
                    "to": to_id,
                    "label": table,
                    "table": table
                })

        return {
            "nodes": nodes,
            "edges": edges
        }

    # -------------------------
    # 节点
    # -------------------------
    def get_node_by_id(self, table_name: str, node_id: str) -> Optional[Dict[str, Any]]:
        self._validate_identifier(table_name)
        self._validate_id(node_id)

        columns = self.get_table_columns(table_name)
        if "id" not in columns:
            raise KuzuClientError(f"节点表 {table_name} 不包含 id 字段。")

        select_fields = ", ".join([f"n.{c} AS {c}" for c in columns])
        cypher = (
            f"MATCH (n:{table_name} {{id: {self._to_cypher_value(node_id)}}}) "
            f"RETURN {select_fields};"
        )
        rows = self.fetch_all_dict(cypher)

        if not rows:
            return None

        return {
            "entityType": "node",
            "table": table_name,
            **rows[0]
        }

    def create_node(self, table_name: str, properties: Dict[str, PrimitiveValue]) -> str:
        self._validate_identifier(table_name)

        if not isinstance(properties, dict):
            raise KuzuClientError("properties 必须是 dict。")

        if "id" in properties:
            raise KuzuClientError("创建节点时不允许手动传入 id，系统会自动生成。")

        self._validate_properties(properties, require_id=False)

        node_id = str(uuid.uuid4())
        final_properties = {"id": node_id, **properties}

        props_str = self._dict_to_cypher_map(final_properties)
        cypher = f"CREATE (n:{table_name} {props_str});"
        self.execute(cypher)

        return node_id

    def update_node_properties(
        self,
        table_name: str,
        node_id: str,
        properties: Dict[str, PrimitiveValue]
    ) -> None:
        self._validate_identifier(table_name)
        self._validate_id(node_id)
        self._validate_properties(properties, require_id=False)

        if "id" in properties:
            raise KuzuClientError("不允许更新节点主键 id。")
        if "table" in properties or "entityType" in properties:
            raise KuzuClientError("不允许更新节点元字段 table/entityType。")
        if not properties:
            return

        set_clauses = []
        for key, value in properties.items():
            self._validate_identifier(key)
            set_clauses.append(f"n.{key} = {self._to_cypher_value(value)}")

        cypher = (
            f"MATCH (n:{table_name} {{id: {self._to_cypher_value(node_id)}}}) "
            f"SET {', '.join(set_clauses)};"
        )
        self.execute(cypher)

    def delete_node(self, table_name: str, node_id: str) -> None:
        self._validate_identifier(table_name)
        self._validate_id(node_id)

        cypher = (
            f"MATCH (n:{table_name} {{id: {self._to_cypher_value(node_id)}}}) "
            f"DETACH DELETE n;"
        )
        self.execute(cypher)

    # -------------------------
    # 关系
    # -------------------------
    def create_relation(
        self,
        from_table: str,
        from_id: str,
        rel_table: str,
        to_table: str,
        to_id: str
    ) -> None:
        self._validate_identifier(from_table)
        self._validate_identifier(rel_table)
        self._validate_identifier(to_table)
        self._validate_id(from_id)
        self._validate_id(to_id)

        cypher = (
            f"MATCH (a:{from_table} {{id: {self._to_cypher_value(from_id)}}}), "
            f"(b:{to_table} {{id: {self._to_cypher_value(to_id)}}}) "
            f"CREATE (a)-[r:{rel_table}]->(b);"
        )
        self.execute(cypher)

    def delete_relation(
        self,
        from_table: str,
        from_id: str,
        rel_table: str,
        to_table: str,
        to_id: str
    ) -> None:
        self._validate_identifier(from_table)
        self._validate_identifier(rel_table)
        self._validate_identifier(to_table)
        self._validate_id(from_id)
        self._validate_id(to_id)

        cypher = (
            f"MATCH (a:{from_table} {{id: {self._to_cypher_value(from_id)}}})"
            f"-[r:{rel_table}]->"
            f"(b:{to_table} {{id: {self._to_cypher_value(to_id)}}}) "
            f"DELETE r;"
        )
        self.execute(cypher)

    # -------------------------
    # 搜索
    # -------------------------
    def search_nodes_by_name(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        if not isinstance(keyword, str) or not keyword.strip():
            return []

        keyword = keyword.strip()
        results: List[Dict[str, Any]] = []

        for table in self.get_node_tables():
            columns = self.get_table_columns(table)
            if "id" not in columns or "name" not in columns:
                continue

            cypher = f"MATCH (n:{table}) RETURN n.id AS id, n.name AS name;"
            rows = self.fetch_all_dict(cypher)

            for row in rows:
                node_id = row.get("id")
                name = row.get("name")

                if not isinstance(node_id, str):
                    continue
                if not isinstance(name, str):
                    continue

                distance = self._levenshtein(keyword.lower(), name.lower())
                results.append({
                    "id": node_id,
                    "name": name,
                    "table": table,
                    "distance": distance
                })

        results.sort(key=lambda x: (x["distance"], len(x["name"]), x["name"]))
        return results[:limit]

    def _levenshtein(self, a: str, b: str) -> int:
        if a == b:
            return 0
        if len(a) == 0:
            return len(b)
        if len(b) == 0:
            return len(a)

        prev = list(range(len(b) + 1))
        for i, ca in enumerate(a, start=1):
            curr = [i]
            for j, cb in enumerate(b, start=1):
                cost = 0 if ca == cb else 1
                curr.append(min(
                    prev[j] + 1,
                    curr[j - 1] + 1,
                    prev[j - 1] + cost
                ))
            prev = curr
        return prev[-1]

    # -------------------------
    # 调试辅助
    # -------------------------
    def get_debug_snapshot(self) -> Dict[str, Any]:
        return {
            "show_tables": self.show_tables(),
            "schema": self.get_schema_summary(),
            "graph": self.get_graph_snapshot()
        }

    # -------------------------
    # 工具方法
    # -------------------------
    def _validate_properties(
        self,
        properties: Dict[str, PrimitiveValue],
        require_id: bool
    ) -> None:
        if not isinstance(properties, dict):
            raise KuzuClientError("properties 必须是 dict。")

        if require_id:
            if "id" not in properties:
                raise KuzuClientError("属性必须包含字符串主键 id。")
            if not isinstance(properties["id"], str):
                raise KuzuClientError("主键 id 必须是字符串。")

        for key, value in properties.items():
            self._validate_identifier(key)
            self._validate_property_value(value)

    def _validate_property_value(self, value: PrimitiveValue) -> None:
        if value is None:
            return
        if isinstance(value, (bool, int, float, str)):
            return
        raise KuzuClientError("属性值仅允许 int、float、bool、str 或 None，不允许复杂结构。")

    def _validate_id(self, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise KuzuClientError("id 必须是非空字符串。")

    def _validate_identifier(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise KuzuClientError("标识符必须是非空字符串。")
        if not (name[0].isalpha() or name[0] == "_"):
            raise KuzuClientError(f"非法标识符: {name}")
        for ch in name:
            if not (ch.isalnum() or ch == "_"):
                raise KuzuClientError(f"非法标识符: {name}")

    def _escape_string(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _to_cypher_value(self, value: PrimitiveValue) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            return f'"{self._escape_string(value)}"'
        raise KuzuClientError(f"不支持的属性值类型: {type(value)}")

    def _dict_to_cypher_map(self, data: Dict[str, PrimitiveValue]) -> str:
        items = []
        for key, value in data.items():
            self._validate_identifier(key)
            self._validate_property_value(value)
            items.append(f"{key}: {self._to_cypher_value(value)}")
        return "{ " + ", ".join(items) + " }"
