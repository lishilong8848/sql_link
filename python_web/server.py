from __future__ import annotations

import argparse
import atexit
import datetime as dt
import json
import logging
import os
import socket
import sys
import threading
import time
import webbrowser
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from flask import Flask, g, jsonify, request, send_from_directory
from pymysql.cursors import DictCursor
from werkzeug.exceptions import HTTPException


EDITABLE_EXCLUDED_FIELDS = {"guid", "event_time"}
TRANSACTIONAL_ENGINES = {"INNODB", "NDB", "NDBCLUSTER"}


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


@dataclass
class ConnectionConfig:
    label: str
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class FieldMeta:
    name: str
    dbType: str
    nullable: bool
    editable: bool
    inputKind: str
    warning: str = ""


class RuntimeState:
    def __init__(self) -> None:
        self.connection_slots: Dict[str, Optional[ConnectionConfig]] = {str(index): None for index in range(1, 6)}
        self.active_connection_id: Optional[str] = None

    def get_saved_slot_count(self) -> int:
        return sum(1 for config in self.connection_slots.values() if config is not None)

    def to_storage_payload(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "connectionSlots": {
                slot_id: asdict(config) if config else None
                for slot_id, config in self.connection_slots.items()
            },
        }

    def load_from_disk(self, path: Path) -> None:
        self.connection_slots = {str(index): None for index in range(1, 6)}
        self.active_connection_id = None
        if not path.exists():
            return

        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
            raw_slots = raw_data.get("connectionSlots") or {}
            for slot_index in range(1, 6):
                raw_connection = raw_slots.get(str(slot_index))
                if not raw_connection:
                    continue
                self.connection_slots[str(slot_index)] = normalize_connection_payload(raw_connection)
            log_info("已加载本地连接配置: file=%s saved_slots=%s", path, self.get_saved_slot_count())
        except Exception as exc:
            log_warning("加载本地连接配置失败: file=%s error=%s", path, exc)

    def save_to_disk(self, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_suffix(path.suffix + ".tmp")
            temp_path.write_text(json.dumps(self.to_storage_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            temp_path.replace(path)
            log_info("已保存本地连接配置: file=%s saved_slots=%s", path, self.get_saved_slot_count())
        except Exception:
            log_exception("保存本地连接配置失败: file=%s", path)

    def to_payload(self) -> Dict[str, Any]:
        active_connection = None
        if self.active_connection_id and self.connection_slots.get(self.active_connection_id):
            active_connection = {
                "slotId": int(self.active_connection_id),
                **asdict(self.connection_slots[self.active_connection_id]),
            }

        return {
            "activeConnectionId": int(self.active_connection_id) if self.active_connection_id else None,
            "activeConnection": active_connection,
            "slots": [
                {
                    "slotId": slot_id,
                    "connection": asdict(self.connection_slots[str(slot_id)]) if self.connection_slots[str(slot_id)] else None,
                }
                for slot_id in range(1, 6)
            ],
        }


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("alarm-db-console")
    if logger.handlers:
        return logger

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


LOGGER = configure_logging()


def log_info(message: str, *args: Any) -> None:
    LOGGER.info(message, *args)


def log_warning(message: str, *args: Any) -> None:
    LOGGER.warning(message, *args)


def log_exception(message: str, *args: Any) -> None:
    LOGGER.exception(message, *args)


def get_resource_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS"))
    return Path(__file__).resolve().parent


def get_state_file_path() -> Path:
    custom_path = os.environ.get("ALARM_DB_STATE_FILE", "").strip()
    if custom_path:
        return Path(custom_path).expanduser()

    appdata = os.environ.get("APPDATA", "").strip()
    if appdata:
        return Path(appdata) / "AlarmDbWebConsole" / "runtime_state.json"

    xdg_config_home = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg_config_home:
        return Path(xdg_config_home) / "alarm-db-console" / "runtime_state.json"

    return Path.home() / ".alarm-db-console" / "runtime_state.json"


RESOURCE_ROOT = get_resource_root()
WEB_ROOT = RESOURCE_ROOT / "webapp"
STATE_FILE = get_state_file_path()


def normalize_slot_id(value: Any) -> str:
    try:
        slot_id = int(value)
    except Exception as exc:
        raise ApiError("slotId 不合法。") from exc
    if slot_id < 1 or slot_id > 5:
        raise ApiError("slotId 必须在 1 到 5 之间。")
    return str(slot_id)


def normalize_connection_payload(payload: Dict[str, Any]) -> ConnectionConfig:
    payload = payload or {}
    label = str(payload.get("label", "")).strip()
    host = str(payload.get("host", "")).strip()
    database = str(payload.get("database", "")).strip()
    user = str(payload.get("user", "")).strip()
    password = str(payload.get("password", ""))
    port_raw = str(payload.get("port", "")).strip()

    if not label or not host or not database or not user or not port_raw:
        raise ApiError("连接信息未填写完整。")

    try:
        port = int(port_raw)
    except Exception as exc:
        raise ApiError("端口必须是数字。") from exc

    if port < 1 or port > 65535:
        raise ApiError("端口范围必须在 1 到 65535 之间。")

    return ConnectionConfig(
        label=label,
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


STATE = RuntimeState()
STATE.load_from_disk(STATE_FILE)


def get_row_value(row: Optional[Dict[str, Any]], *candidate_keys: str) -> Any:
    if not row:
        return None

    lower_key_map = {str(key).lower(): value for key, value in row.items()}
    for key in candidate_keys:
        if key in row:
            return row[key]
        lowered = str(key).lower()
        if lowered in lower_key_map:
            return lower_key_map[lowered]
    return None


class DatabaseService:
    def connect(self, config: ConnectionConfig, autocommit: bool = True):
        return pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            charset="utf8mb4",
            autocommit=autocommit,
            cursorclass=DictCursor,
        )

    def test_connection(self, config: ConnectionConfig) -> None:
        log_info("测试数据库连接: label=%s host=%s port=%s database=%s user=%s", config.label, config.host, config.port, config.database, config.user)
        connection = self.connect(config)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            log_info("数据库连接成功: label=%s database=%s", config.label, config.database)
        finally:
            connection.close()

    def get_field_metadata(self, config: ConnectionConfig) -> List[FieldMeta]:
        connection = self.connect(config)
        try:
            latest_table = self.get_latest_event_table(connection, config.database)
            if not latest_table:
                log_warning("未找到任何 event_YYYY_MM 表: database=%s", config.database)
                return []

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name, column_type, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (config.database, latest_table),
                )
                rows = cursor.fetchall()

            metadata = [
                FieldMeta(
                    name=str(get_row_value(row, "column_name", "COLUMN_NAME", "Column_name") or ""),
                    dbType=str(get_row_value(row, "column_type", "COLUMN_TYPE", "Column_type") or ""),
                    nullable=str(get_row_value(row, "is_nullable", "IS_NULLABLE", "Is_nullable") or "").upper() == "YES",
                    editable=str(get_row_value(row, "column_name", "COLUMN_NAME", "Column_name") or "") not in EDITABLE_EXCLUDED_FIELDS,
                    inputKind=self.get_input_kind(
                        str(get_row_value(row, "column_name", "COLUMN_NAME", "Column_name") or ""),
                        str(get_row_value(row, "data_type", "DATA_TYPE", "Data_type") or ""),
                        str(get_row_value(row, "column_type", "COLUMN_TYPE", "Column_type") or ""),
                    ),
                    warning=(
                        "设置 is_recover = 1 可能触发恢复逻辑，并导致 mem_event 中的对应记录被删除。"
                        if str(get_row_value(row, "column_name", "COLUMN_NAME", "Column_name") or "") == "is_recover"
                        else ""
                    ),
                )
                for row in rows
            ]
            log_info("读取字段元数据成功: database=%s reference_table=%s column_count=%s", config.database, latest_table, len(metadata))
            return metadata
        finally:
            connection.close()

    def query_events(self, config: ConnectionConfig, start_time: int, end_time: int, page: int, page_size: int) -> Dict[str, Any]:
        connection = self.connect(config)
        try:
            candidate_tables = self.enumerate_tables(start_time, end_time)
            existing_tables = self.get_existing_tables(connection, config.database, candidate_tables)
            if not existing_tables:
                log_info(
                    "查询事件结果为空: database=%s start=%s end=%s page=%s page_size=%s candidate_tables=%s",
                    config.database,
                    start_time,
                    end_time,
                    page,
                    page_size,
                    ",".join(candidate_tables),
                )
                return {"rows": [], "total": 0, "tables": [], "page": page, "pageSize": page_size}

            union_sql = " UNION ALL ".join(
                [
                    "SELECT %s AS _table, src.* FROM `{}` AS src WHERE src.event_time BETWEEN %s AND %s".format(table_name)
                    for table_name in existing_tables
                ]
            )
            base_params: List[Any] = []
            for table_name in existing_tables:
                base_params.extend([table_name, start_time, end_time])

            offset = (page - 1) * page_size
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) AS total FROM ({}) AS combined".format(union_sql), base_params)
                count_row = cursor.fetchone()
                total = int(get_row_value(count_row, "total", "TOTAL", "count(*)", "COUNT(*)") or 0)
                cursor.execute(
                    "SELECT * FROM ({}) AS combined ORDER BY event_time DESC LIMIT %s OFFSET %s".format(union_sql),
                    base_params + [page_size, offset],
                )
                rows = cursor.fetchall()
            log_info(
                "查询事件成功: database=%s start=%s end=%s page=%s page_size=%s tables=%s total=%s rows=%s",
                config.database,
                start_time,
                end_time,
                page,
                page_size,
                ",".join(existing_tables),
                total,
                len(rows),
            )
            return {"rows": rows, "total": total, "tables": existing_tables, "page": page, "pageSize": page_size}
        finally:
            connection.close()

    def batch_update_events(self, config: ConnectionConfig, targets: List[Dict[str, str]], updates: Dict[str, Any]) -> Dict[str, Any]:
        if not updates:
            raise ApiError("至少需要提供一个要修改的字段。")

        field_metadata = self.get_field_metadata(config)
        editable_fields = {field.name for field in field_metadata if field.editable}
        for field_name in updates.keys():
            if field_name not in editable_fields:
                raise ApiError("字段 {} 不允许修改。".format(field_name))

        grouped: Dict[str, List[str]] = {}
        deduped: Dict[str, Dict[str, str]] = {}
        for target in targets:
            table_name = str(target.get("table", "")).strip()
            guid = str(target.get("guid", "")).strip()
            if not table_name or not guid:
                raise ApiError("目标记录不完整。")
            if not self.is_valid_event_table(table_name):
                raise ApiError("非法表名: {}".format(table_name))
            deduped["{}:{}".format(table_name, guid)] = {"table": table_name, "guid": guid}

        for target in deduped.values():
            grouped.setdefault(target["table"], []).append(target["guid"])

        connection = self.connect(config, autocommit=True)
        try:
            existing_tables = set(self.get_existing_tables(connection, config.database, list(grouped.keys())))
            for table_name in grouped.keys():
                if table_name not in existing_tables:
                    raise ApiError("目标表不存在: {}".format(table_name))

            mem_event_engine = self.get_table_engine(connection, config.database, "mem_event")
            has_nontransactional_mem_event = bool(mem_event_engine) and str(mem_event_engine).upper() not in TRANSACTIONAL_ENGINES
            affected_rows = 0
            skipped_targets: List[Dict[str, str]] = []
            with connection.cursor() as cursor:
                for table_name, guids in grouped.items():
                    safe_guids = list(guids)
                    if has_nontransactional_mem_event:
                        safe_guids, risky_guids = self.split_gtid_sensitive_guids(connection, table_name, guids, updates)
                        skipped_targets.extend(
                            [
                                {
                                    "table": table_name,
                                    "guid": guid,
                                    "reason": "gtid_recover_trigger",
                                }
                                for guid in risky_guids
                            ]
                        )

                    if not safe_guids:
                        continue

                    set_clause = ", ".join(["`{}` = %s".format(field_name) for field_name in updates.keys()])
                    where_clause = ", ".join(["%s"] * len(safe_guids))
                    sql = "UPDATE `{}` SET {} WHERE guid IN ({})".format(table_name, set_clause, where_clause)
                    try:
                        affected_rows += cursor.execute(sql, list(updates.values()) + safe_guids)
                    except pymysql.err.OperationalError as exc:
                        if exc.args and int(exc.args[0]) == 1785:
                            log_warning(
                                "检测到 GTID 限制，降级为逐条更新: database=%s table=%s guid_count=%s",
                                config.database,
                                table_name,
                                len(safe_guids),
                            )
                            for guid in safe_guids:
                                single_sql = "UPDATE `{}` SET {} WHERE guid = %s".format(table_name, set_clause)
                                try:
                                    affected_rows += cursor.execute(single_sql, list(updates.values()) + [guid])
                                except pymysql.err.OperationalError as single_exc:
                                    if single_exc.args and int(single_exc.args[0]) == 1785:
                                        skipped_targets.append(
                                            {
                                                "table": table_name,
                                                "guid": guid,
                                                "reason": "gtid_runtime_trigger",
                                            }
                                        )
                                        continue
                                    raise
                        else:
                            raise

            warning = self.build_batch_update_warning(skipped_targets, mem_event_engine)
            log_info(
                "批量修改成功: database=%s tables=%s target_count=%s fields=%s affected_rows=%s skipped_count=%s autocommit=%s",
                config.database,
                ",".join(sorted(grouped.keys())),
                len(deduped),
                ",".join(updates.keys()),
                affected_rows,
                len(skipped_targets),
                True,
            )
            return {
                "affectedRows": affected_rows,
                "skippedCount": len(skipped_targets),
                "skippedTargets": skipped_targets,
                "warning": warning,
            }
        except pymysql.err.OperationalError as exc:
            if exc.args and int(exc.args[0]) == 1785:
                log_exception(
                    "批量修改失败: database=%s target_count=%s fields=%s reason=gtid_consistency",
                    config.database,
                    len(deduped),
                    ",".join(updates.keys()),
                )
                raise ApiError(
                    "数据库拒绝了本次批量修改。当前库启用了 GTID 一致性限制，并且相关触发器涉及非事务表。"
                    " 系统已经改为单语句自动提交模式；如果仍失败，请检查触发器关联表的存储引擎配置。"
                ) from exc
            log_exception("批量修改失败: database=%s target_count=%s fields=%s", config.database, len(deduped), ",".join(updates.keys()))
            raise
        except Exception:
            log_exception("批量修改失败: database=%s target_count=%s fields=%s", config.database, len(deduped), ",".join(updates.keys()))
            raise
        finally:
            connection.close()

    def split_gtid_sensitive_guids(self, connection, table_name: str, guids: List[str], updates: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        if not guids:
            return [], []

        recover_map = self.get_guid_recover_map(connection, table_name, guids)
        safe_guids: List[str] = []
        risky_guids: List[str] = []
        for guid in guids:
            current_recover = recover_map.get(guid)
            final_recover = self.resolve_final_is_recover(current_recover, updates)
            if final_recover == 1:
                risky_guids.append(guid)
            else:
                safe_guids.append(guid)
        return safe_guids, risky_guids

    def get_guid_recover_map(self, connection, table_name: str, guids: List[str]) -> Dict[str, Optional[int]]:
        if not guids:
            return {}

        placeholders = ", ".join(["%s"] * len(guids))
        sql = "SELECT guid, is_recover FROM `{}` WHERE guid IN ({})".format(table_name, placeholders)
        with connection.cursor() as cursor:
            cursor.execute(sql, guids)
            rows = cursor.fetchall()

        recover_map: Dict[str, Optional[int]] = {}
        for row in rows:
            guid = str(get_row_value(row, "guid", "GUID", "Guid") or "")
            if not guid:
                continue
            recover_map[guid] = self.to_optional_int(get_row_value(row, "is_recover", "IS_RECOVER", "Is_recover"))
        return recover_map

    def resolve_final_is_recover(self, current_value: Optional[int], updates: Dict[str, Any]) -> Optional[int]:
        if "is_recover" not in updates:
            return current_value
        return self.to_optional_int(updates.get("is_recover"))

    def to_optional_int(self, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except Exception:
            return None

    def build_batch_update_warning(self, skipped_targets: List[Dict[str, str]], mem_event_engine: Optional[str]) -> str:
        if not skipped_targets:
            return ""

        gtid_related = [item for item in skipped_targets if item.get("reason", "").startswith("gtid_")]
        if not gtid_related:
            return "有部分记录未更新，请查看服务端日志。"

        if not mem_event_engine:
            return (
                "已跳过 {} 条记录。当前数据库存在 GTID 一致性限制，且这些记录触发了不兼容的更新逻辑。"
                " 如果必须修改这类记录，需要在数据库侧调整相关触发器或关联表的存储引擎。"
            ).format(len(gtid_related))

        return (
            "已跳过 {} 条记录。当前数据库的恢复类触发器会联动 mem_event".format(len(gtid_related))
            + ("（{}）".format(mem_event_engine) if mem_event_engine else "")
            + "，而这些记录更新后会处于 is_recover = 1 状态；在 GTID 一致性开启时，MySQL 会拒绝这类更新。"
            " 如果必须修改这类记录，需要在数据库侧把 mem_event 调整为事务表，或修改相关触发器逻辑。"
        )

    def get_existing_tables(self, connection, database: str, table_names: List[str]) -> List[str]:
        if not table_names:
            return []
        placeholders = ", ".join(["%s"] * len(table_names))
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name IN ({})
                """.format(placeholders),
                [database] + table_names,
            )
            rows = cursor.fetchall()
        existing_tables: List[str] = []
        for row in rows:
            table_name = get_row_value(row, "table_name", "TABLE_NAME", "Table_name")
            if table_name and self.is_valid_event_table(str(table_name)):
                existing_tables.append(str(table_name))
        return sorted(existing_tables)

    def get_table_engine(self, connection, database: str, table_name: str) -> Optional[str]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT engine
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
                LIMIT 1
                """,
                [database, table_name],
            )
            row = cursor.fetchone()
        engine = get_row_value(row, "engine", "ENGINE", "Engine")
        return str(engine) if engine else None

    def get_latest_event_table(self, connection, database: str) -> Optional[str]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name REGEXP %s
                ORDER BY table_name DESC
                LIMIT 1
                """,
                (database, "^event_[0-9]{4}_[0-9]{2}$"),
            )
            row = cursor.fetchone()
        table_name = get_row_value(row, "table_name", "TABLE_NAME", "Table_name")
        if row and not table_name:
            log_warning("最新月份表查询返回了无法识别的字段结构: keys=%s", ",".join(sorted([str(key) for key in row.keys()])))
        return str(table_name) if table_name else None

    def enumerate_tables(self, start_time: int, end_time: int) -> List[str]:
        start = dt.datetime.fromtimestamp(start_time).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = dt.datetime.fromtimestamp(end_time).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        tables: List[str] = []
        cursor = start
        while cursor <= end:
            tables.append("event_{:04d}_{:02d}".format(cursor.year, cursor.month))
            if cursor.month == 12:
                cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                cursor = cursor.replace(month=cursor.month + 1)
        return tables

    def get_input_kind(self, name: str, data_type: str, column_type: str) -> str:
        data_type = str(data_type).lower()
        column_type = str(column_type).lower()
        if data_type in {"tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double"}:
            return "number"
        if (
            data_type in {"text", "mediumtext", "longtext"}
            or "text" in column_type
            or name
            in {
                "content",
                "event_snapshot",
                "recover_snapshot",
                "recover_description",
                "confirm_description",
                "accept_description",
                "event_location",
                "event_source",
            }
        ):
            return "textarea"
        return "text"

    def is_valid_event_table(self, table_name: str) -> bool:
        parts = table_name.split("_")
        return len(parts) == 3 and parts[0] == "event" and parts[1].isdigit() and len(parts[1]) == 4 and parts[2].isdigit() and len(parts[2]) == 2


SERVICE = DatabaseService()
APP = Flask(__name__, static_folder=None)


@APP.before_request
def before_request_logging():
    g.request_started_at = time.perf_counter()
    request_id = "{} {}".format(request.method, request.path)
    if request.query_string:
        request_id += "?{}".format(request.query_string.decode("utf-8", errors="replace"))
    log_info("收到请求: %s from=%s", request_id, request.remote_addr or "-")


@APP.after_request
def after_request_logging(response):
    started_at = getattr(g, "request_started_at", None)
    duration_ms = 0.0
    if started_at is not None:
        duration_ms = (time.perf_counter() - started_at) * 1000
    log_info(
        "请求完成: method=%s path=%s status=%s duration_ms=%.1f",
        request.method,
        request.path,
        response.status_code,
        duration_ms,
    )
    return response


def get_active_connection() -> ConnectionConfig:
    if not STATE.active_connection_id:
        log_warning("请求活动连接失败: 当前未进入任何数据库")
        raise ApiError("当前未进入任何数据库。", 401)
    connection = STATE.connection_slots.get(STATE.active_connection_id)
    if not connection:
        STATE.active_connection_id = None
        log_warning("请求活动连接失败: 活动连接丢失，已重置 active_connection_id")
        raise ApiError("当前数据库连接不存在，请重新进入数据库。", 401)
    return connection


def parse_query_payload(payload: Dict[str, Any]) -> Dict[str, int]:
    now = int(dt.datetime.now().timestamp())
    month_start = dt.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_time = int(payload.get("startTime") or int(month_start.timestamp()))
    end_time = int(payload.get("endTime") or now)
    page = int(payload.get("page") or 1)
    page_size = int(payload.get("pageSize") or 50)
    if page < 1:
        raise ApiError("页码必须大于等于 1。")
    if page_size < 1 or page_size > 200:
        raise ApiError("pageSize 必须在 1 到 200 之间。")
    if start_time > end_time:
        raise ApiError("开始时间不能晚于结束时间。")
    return {"startTime": start_time, "endTime": end_time, "page": page, "pageSize": page_size}


@APP.errorhandler(ApiError)
def handle_api_error(error: ApiError):
    log_warning("接口错误: status=%s message=%s", error.status_code, error.message)
    return jsonify({"message": error.message}), error.status_code


@APP.errorhandler(HTTPException)
def handle_http_error(error: HTTPException):
    status_code = int(error.code or 500)
    log_warning("HTTP 异常: status=%s path=%s message=%s", status_code, request.path, error.description)
    return jsonify({"message": error.description or "请求失败。"}), status_code


@APP.errorhandler(Exception)
def handle_unexpected_error(error: Exception):
    log_exception("未处理异常")
    return jsonify({"message": str(error) or "服务器内部错误。"}), 500


@APP.get("/")
def index():
    return send_from_directory(WEB_ROOT, "index.html")


@APP.get("/favicon.ico")
def favicon():
    return ("", 204)


@APP.get("/styles.css")
def styles():
    return send_from_directory(WEB_ROOT, "styles.css")


@APP.get("/app.js")
def app_js():
    return send_from_directory(WEB_ROOT, "app.js")


@APP.get("/api/health")
def health():
    return jsonify({"ok": True})


@APP.get("/api/session/state")
def session_state():
    log_info("返回会话状态: active_connection_id=%s", STATE.active_connection_id or "None")
    return jsonify(STATE.to_payload())


@APP.post("/api/session/connections/test")
def test_connection():
    config = normalize_connection_payload(request.get_json(silent=True) or {})
    SERVICE.test_connection(config)
    return jsonify({"success": True, "message": "数据库连接成功。"})


@APP.post("/api/session/connections")
def save_connection():
    payload = request.get_json(silent=True) or {}
    slot_id = normalize_slot_id(payload.get("slotId"))
    config = normalize_connection_payload(payload)
    SERVICE.test_connection(config)
    STATE.connection_slots[slot_id] = config
    STATE.save_to_disk(STATE_FILE)
    log_info("保存连接卡槽成功: slot_id=%s label=%s host=%s database=%s", slot_id, config.label, config.host, config.database)
    return jsonify(STATE.to_payload())


@APP.delete("/api/session/connections/<slot_id>")
def clear_connection(slot_id: str):
    slot_key = normalize_slot_id(slot_id)
    STATE.connection_slots[slot_key] = None
    if STATE.active_connection_id == slot_key:
        STATE.active_connection_id = None
    STATE.save_to_disk(STATE_FILE)
    log_info("清空连接卡槽成功: slot_id=%s", slot_key)
    return jsonify(STATE.to_payload())


@APP.post("/api/session/active-connection")
def activate_connection():
    payload = request.get_json(silent=True) or {}
    slot_id = normalize_slot_id(payload.get("slotId"))
    config = STATE.connection_slots.get(slot_id)
    if not config:
        raise ApiError("该卡槽还没有保存数据库连接。", 404)
    SERVICE.test_connection(config)
    STATE.active_connection_id = slot_id
    log_info("进入数据库成功: slot_id=%s label=%s host=%s database=%s", slot_id, config.label, config.host, config.database)
    return jsonify(STATE.to_payload())


@APP.post("/api/session/logout-current")
def logout_current():
    log_info("退出当前数据库: previous_slot_id=%s", STATE.active_connection_id or "None")
    STATE.active_connection_id = None
    return jsonify(STATE.to_payload())


@APP.get("/api/event/fields")
def event_fields():
    fields = SERVICE.get_field_metadata(get_active_connection())
    log_info("返回字段元数据: count=%s", len(fields))
    return jsonify({"columns": [asdict(field) for field in fields]})


@APP.post("/api/events/query")
def query_events():
    payload = parse_query_payload(request.get_json(silent=True) or {})
    result = SERVICE.query_events(
        get_active_connection(),
        payload["startTime"],
        payload["endTime"],
        payload["page"],
        payload["pageSize"],
    )
    log_info("返回查询结果: total=%s rows=%s page=%s page_size=%s", result["total"], len(result["rows"]), result["page"], result["pageSize"])
    return jsonify(result)


@APP.post("/api/events/batch-update")
def batch_update():
    payload = request.get_json(silent=True) or {}
    targets = payload.get("targets") or []
    updates = payload.get("updates") or {}
    if not isinstance(targets, list) or not targets:
        raise ApiError("至少勾选一条记录。")
    if not isinstance(updates, dict) or not updates:
        raise ApiError("至少需要填写一个修改字段。")
    result = SERVICE.batch_update_events(get_active_connection(), targets, updates)
    log_info(
        "返回批量修改结果: affected_rows=%s skipped_count=%s",
        result["affectedRows"],
        result["skippedCount"],
    )
    return jsonify({"success": True, **result})


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def open_browser_later(port: int, enabled: bool = True) -> None:
    if not enabled or os.environ.get("ALARM_DB_NO_BROWSER") == "1":
        log_info("已禁用自动打开浏览器: port=%s", port)
        return

    def _open():
        log_info("准备打开浏览器: url=http://127.0.0.1:%s/", port)
        webbrowser.open("http://127.0.0.1:{}/".format(port))

    threading.Timer(1.2, _open).start()


def parse_runtime_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="本地告警数据库网页工具")
    parser.add_argument("port", nargs="?", type=int, help="监听端口，不传则自动分配")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--no-browser", action="store_true", help="启动后不自动打开浏览器")
    args = parser.parse_args(argv)
    if args.port is not None and not 1 <= args.port <= 65535:
        parser.error("端口范围必须在 1 到 65535 之间。")
    return args


def run_server(host: str = "127.0.0.1", port: Optional[int] = None, auto_open_browser: bool = True) -> None:
    listen_port = port or find_free_port()
    log_info("=" * 72)
    log_info("Alarm DB Console 启动中")
    log_info("资源目录: %s", RESOURCE_ROOT)
    log_info("静态网页目录: %s", WEB_ROOT)
    log_info("配置持久化文件: %s", STATE_FILE)
    log_info("本地服务地址: http://%s:%s/", host, listen_port)
    log_info("关闭这个黑窗口，服务也会一起退出。")
    log_info("=" * 72)
    open_browser_later(listen_port, enabled=auto_open_browser)
    APP.run(host=host, port=listen_port, debug=False, use_reloader=False, threaded=True)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_runtime_args(argv)
    run_server(host=args.host, port=args.port, auto_open_browser=not args.no_browser)


if __name__ == "__main__":
    atexit.register(lambda: log_info("Alarm DB Console 已退出。"))
    main()
