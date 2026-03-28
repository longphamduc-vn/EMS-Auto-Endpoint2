"""
engine.py – WorkflowWorker: QThread chạy toàn bộ pipeline step tuần tự.

Phát các pyqtSignal để cập nhật UI mà không block main thread:
  step_started(step_name)
  step_completed(step_name, result_dict, cache_used)
  log_signal(message)
  failed(error_message)
  all_done(final_context)
"""

import hashlib
import html
import json
import os
import re
import traceback
import unicodedata
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from PyQt6.QtCore import QThread, pyqtSignal

from .constants import ACCUMULATED_FILE, CACHE_DIR
from .nexacro import nexacro_xml_to_json, payload_to_nexacro_xml
from .utils import (
    duplicate_rows,
    dict_of_lists_to_records,
    evaluate_calc_expression,
    flatten_single,
    group_records,
    jsonpath_values,
    load_json,
    merge_records_inner,
    merge_record_sets,
    normalize_extracts,
    records_to_dict_of_lists,
    rows_from_any,
    save_json,
    sort_records,
    pivot_records,
)


class WorkflowWorker(QThread):
    step_started = pyqtSignal(str)
    step_completed = pyqtSignal(str, dict, bool)
    log_signal = pyqtSignal(str)
    failed = pyqtSignal(str)
    all_done = pyqtSignal(dict)

    def __init__(
        self,
        task: Dict[str, Any],
        input_data: Dict[str, List[str]],
        base_dir: str,
    ) -> None:
        super().__init__()
        self.task = task
        self.input_data = input_data
        self.base_dir = base_dir
        self.http_session = requests.Session()

    # ------------------------------------------------------------------
    # Logging helper
    # ------------------------------------------------------------------

    def log(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_signal.emit(f"[{ts}] {message}")

    def _mask_value(self, key: str, value: Any) -> Any:
        lower_key = key.lower()
        if any(token in lower_key for token in ("token", "password", "secret", "auth")):
            return "***MASKED***"
        return value

    def _sanitize(self, data: Any) -> Any:
        if isinstance(data, dict):
            return {k: self._sanitize(self._mask_value(k, v)) for k, v in data.items()}
        if isinstance(data, list):
            return [self._sanitize(v) for v in data]
        return data

    def _to_json_preview(self, data: Any, limit: int = 500) -> str:
        try:
            raw = json.dumps(self._sanitize(data), ensure_ascii=False)
        except Exception:
            raw = str(data)
        if len(raw) <= limit:
            return raw
        return raw[:limit] + "..."

    def _payload_summary(self, payload: Dict[str, Any]) -> str:
        datasets = payload.get("datasets", []) or []
        ds_parts: List[str] = []
        for ds in datasets:
            fields = ds.get("fields", []) or []
            field_names = [str(f.get("name", "")) for f in fields]
            row_count = 1
            for f in fields:
                v = f.get("value")
                if isinstance(v, list):
                    row_count = max(row_count, len(v))
            ds_parts.append(f"{ds.get('id', 'dataset')} rows={row_count} fields={field_names}")
        return "; ".join(ds_parts)

    # ------------------------------------------------------------------
    # Dynamic JSONPath value resolution
    # ------------------------------------------------------------------

    def resolve_value(
        self,
        value: Any,
        context: Dict[str, Any],
        loop_idx: Optional[int],
    ) -> Any:
        """
        Resolve một giá trị trong payload config:
          - Nếu là dict/list thì đệ quy.
          - Nếu là chuỗi bắt đầu bằng "$." thì tra cứu JSONPath từ context.
          - loop_idx dùng để chọn phần tử khi đang lặp qua loopOver.
        """
        if isinstance(value, dict):
            return {k: self.resolve_value(v, context, loop_idx) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve_value(v, context, loop_idx) for v in value]
        if not isinstance(value, str) or not value.startswith("$."):
            return value

        values = jsonpath_values(context, value)
        if not values:
            return None

        if len(values) == 1:
            single = values[0]
            if isinstance(single, list):
                # Nếu đang trong vòng lặp, lấy phần tử tương ứng
                if loop_idx is not None and loop_idx < len(single):
                    return single[loop_idx]
                return single
            return single

        return values

    def resolve_payload(
        self,
        payload: Dict[str, Any],
        context: Dict[str, Any],
        loop_idx: Optional[int],
    ) -> Dict[str, Any]:
        """
        Deep-copy payload rồi resolve tất cả tham chiếu JSONPath
        trong parameters và datasets[].fields[].value|source.
        """
        resolved = deepcopy(payload)

        params = resolved.get("parameters", {}) or {}
        for k, v in list(params.items()):
            params[k] = self.resolve_value(v, context, loop_idx)

        for ds in resolved.get("datasets", []) or []:
            for field in ds.get("fields", []) or []:
                # Hỗ trợ cả key "value" và "source" trong config
                src_key = "source" if "source" in field else "value"
                field[src_key] = self.resolve_value(field.get(src_key), context, loop_idx)
                if src_key == "source":
                    field["value"] = field.pop("source")

        return resolved

    # ------------------------------------------------------------------
    # Extract result from parsed response JSON
    # ------------------------------------------------------------------

    def apply_extracts(
        self,
        extracts: List[Dict[str, Any]],
        source: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Trích xuất các trường cần thiết từ response JSON đã được parse
        theo danh sách extracts trong config.
        Trả về dict columnar {field: [values...]}.
        """
        result: Dict[str, Any] = {}
        response_rows = source.get("response")

        if isinstance(response_rows, list):
            calc_contexts = [{"response": row} for row in response_rows]
        elif "response" in source:
            calc_contexts = [{"response": response_rows}]
        else:
            calc_contexts = [{"response": source}]

        for ext in extracts:
            name = ext.get("name")
            if not name:
                continue
            ext_type = ext.get("type", "JSON_PATH")

            if ext_type == "JSON_PATH":
                vals = jsonpath_values(source, ext.get("value", ""))
                if not vals and 'reponse' in ext.get("value", ""):
                    corrected_path = ext.get("value", "").replace('reponse', 'response')
                    vals = jsonpath_values(source, corrected_path)

                # Unwrap list-of-list khi JSONPath trả về [[...]]
                if len(vals) == 1 and isinstance(vals[0], list):
                    vals = vals[0]
                result[name] = vals
            elif ext_type == "CALCULATION":
                result[name] = [
                    evaluate_calc_expression(ext.get("value", ""), calc_context)
                    for calc_context in calc_contexts
                ]
        return result

    def _empty_extracts_result(self, extracts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Tạo result rỗng theo schema extracts để UI vẫn hiển thị đúng cột."""
        result: Dict[str, Any] = {}
        for ext in extracts:
            name = ext.get("name")
            if isinstance(name, str) and name:
                result[name] = []
        return result

    def _response_error_info(self, parsed_response: Dict[str, Any]) -> Tuple[Optional[int], str]:
        """Đọc ErrorCode/ErrorMsg từ Parameters theo nhiều biến thể key."""
        params = parsed_response.get("parameters", {})
        if not isinstance(params, dict):
            return None, ""

        error_code_raw = params.get("ErrorCode")
        if error_code_raw is None:
            error_code_raw = params.get("errorCode")
        if error_code_raw is None:
            error_code_raw = params.get("errorcode")

        error_msg = params.get("ErrorMsg")
        if error_msg is None:
            error_msg = params.get("errorMsg")
        if error_msg is None:
            error_msg = params.get("errormsg")

        error_code: Optional[int]
        try:
            if error_code_raw in (None, ""):
                error_code = None
            else:
                error_code = int(error_code_raw)
        except (ValueError, TypeError):
            error_code = None

        return error_code, "" if error_msg is None else html.unescape(str(error_msg)).strip()

    def _normalize_error_text(self, text: str) -> str:
        """Chuẩn hoá ErrorMsg để so khớp token ổn định hơn."""
        if not text:
            return ""
        normalized = html.unescape(text)
        normalized = unicodedata.normalize("NFKC", normalized)
        normalized = normalized.lower()
        # Giữ chữ/số và khoảng trắng, bỏ ký tự phân tách như '-', ':', '.'...
        normalized = re.sub(r"[^\w\s가-힣]", " ", normalized)
        normalized = " ".join(normalized.split())
        return normalized

    def _is_no_data_response(self, error_code: Optional[int], error_msg: str) -> bool:
        """
        Nhận diện phản hồi 'không có dữ liệu'.
        Không phụ thuộc duy nhất vào một mã cụ thể (ví dụ -1).
        """
        normalized = self._normalize_error_text(error_msg)
        message_indicates_no_data = any(
            token in normalized
            for token in (
                "데이터 없음",
                "데이터없음",
                "no data",
                "not found",
                "khong co du lieu",
                "không có dữ liệu",
                "ko co du lieu",
            )
        )

        if message_indicates_no_data:
            return True

        if error_code is not None and error_code != 0 and not error_msg:
            # Một số API chỉ trả mã lỗi khi không có dữ liệu,
            # nhưng quyết định cuối cùng cần kết hợp với nội dung response.
            return True

        return False

    def _response_has_data(self, parsed_response: Dict[str, Any]) -> bool:
        """Kiểm tra response có dữ liệu thực hay không để tránh classify nhầm no-data."""
        payload = parsed_response.get("response")
        if isinstance(payload, list):
            return len(payload) > 0
        if isinstance(payload, dict):
            if not payload:
                return False
            for value in payload.values():
                if isinstance(value, list) and len(value) > 0:
                    return True
                if isinstance(value, dict) and len(value) > 0:
                    return True
                if value not in (None, "", []):
                    return True
            return False
        return payload not in (None, "", [], {})

    # ------------------------------------------------------------------
    # HTTP request with retry
    # ------------------------------------------------------------------

    def request_with_retry(self, method: str, url: str, xml_payload: bytes) -> str:
        """
        Gửi HTTP request với Nexacro XML payload.
        Retry tối đa 3 lần nếu gặp lỗi network hoặc lỗi HTTP.
        """
        last_error: Optional[Exception] = None
        headers = {"Content-Type": "text/xml; charset=UTF-8"}

        for attempt in range(1, 4):
            try:
                response = self.http_session.request(
                    method=method,
                    url=url,
                    data=xml_payload,
                    headers=headers,
                    timeout=30,
                )
                response.raise_for_status()
                content = response.content or b""
                if not content:
                    return ""

                # Nexacro phản hồi thường là UTF-8 theo XML declaration.
                for encoding in ("utf-8", response.encoding, "cp949", "euc-kr"):
                    if not encoding:
                        continue
                    try:
                        return content.decode(encoding)
                    except (LookupError, UnicodeDecodeError):
                        continue

                return response.text
            except requests.RequestException as ex:
                last_error = ex
                self.log(f"Retry {attempt}/3 — Network/HTTP Error: {ex}")

        raise RuntimeError(f"Request failed after 3 retries: {last_error}")

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_path(self, key: str) -> str:
        return os.path.join(self.base_dir, CACHE_DIR, f"{key}.json")

    def read_cache(self, key: str) -> Optional[Dict[str, Any]]:
        path = self._cache_path(key)
        if os.path.exists(path):
            return load_json(path, None)
        return None

    def write_cache(self, key: str, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.join(self.base_dir, CACHE_DIR), exist_ok=True)
        save_json(self._cache_path(key), data)

    # ------------------------------------------------------------------
    # Accumulation helper
    # ------------------------------------------------------------------

    def append_accumulated(self, step_name: str, data: Dict[str, Any]) -> None:
        """
        Nối (append) kết quả của step vào accumulated_data.json.
        File được giữ hợp lệ JSON sau mỗi lần ghi:
          { "step_name": [ {...row1...}, {...row2...}, ... ] }
        """
        path = os.path.join(self.base_dir, ACCUMULATED_FILE)
        current = self._load_accumulated_store()
        current_rows = current.setdefault(step_name, [])
        if not isinstance(current_rows, list):
            current_rows = self._records_from_step_data(current_rows)
            current[step_name] = current_rows

        new_rows = self._records_from_step_data(data)
        before_count = len(current_rows)
        current_rows.extend(new_rows)
        after_count = len(current_rows)
        save_json(path, current)
        self.log(
            f"[DEBUG] accumulation step={step_name} appended={after_count - before_count} total={after_count} file={path}"
        )

    def _load_accumulated_store(self) -> Dict[str, Any]:
        path = os.path.join(self.base_dir, ACCUMULATED_FILE)
        current = load_json(path, {})
        if isinstance(current, dict):
            return current
        self.log(
            f"[DEBUG] accumulation invalid_store_type={type(current).__name__} reset_to_empty path={path}"
        )
        return {}

    def _rows_to_result(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        return records_to_dict_of_lists(rows)

    def _flatten_accumulation_values(self, value: Any) -> List[Any]:
        if isinstance(value, list):
            flattened: List[Any] = []
            for item in value:
                flattened.extend(self._flatten_accumulation_values(item))
            return flattened
        return [value]

    def _records_from_step_data(self, step_data: Any) -> List[Dict[str, Any]]:
        if isinstance(step_data, list):
            return [row for row in step_data if isinstance(row, dict)]
        if not isinstance(step_data, dict):
            return []

        return dict_of_lists_to_records(step_data)

    def _resolve_accumulation_context_values(
        self,
        context: Dict[str, Any],
        json_path: str,
    ) -> List[Any]:
        values = jsonpath_values(context, json_path)
        if values:
            flattened_values: List[Any] = []
            for value in values:
                flattened_values.extend(self._flatten_accumulation_values(value))
            return flattened_values

        match = re.fullmatch(
            r"\$\.([A-Za-z0-9_]+)(?:\.items)?\[\*\]\.([A-Za-z0-9_]+)",
            json_path.strip(),
        )
        if match:
            step_name, field_name = match.groups()
            step_data = context.get(step_name)
            extracted = []
            for item in self._records_from_step_data(step_data):
                if field_name in item:
                    extracted.append(item.get(field_name))
            if extracted:
                return extracted

        return []

    def _parse_accumulation_expression(
        self,
        expression: str,
    ) -> Optional[Tuple[str, str, Optional[str], Optional[str]]]:
        prefix = "$.accumulated_data."
        if not expression.startswith(prefix):
            return None

        remainder = expression[len(prefix):].strip()
        if "." not in remainder:
            return None

        task_name, step_part = remainder.split(".", 1)
        task_name = task_name.strip()
        step_part = step_part.strip()
        if not task_name or not step_part:
            return None

        filter_start = step_part.find("[?(")
        if filter_start == -1:
            return task_name, step_part, None, None

        step_name = step_part[:filter_start].strip()
        filter_suffix = step_part[filter_start:]
        if not step_name or not filter_suffix.endswith(")]"):
            return None

        filter_body = filter_suffix[3:-2].strip()
        match = re.fullmatch(r"@\.([A-Za-z0-9_]+)\s+in\s+(.+)", filter_body)
        if not match:
            return None

        row_field, context_path = match.groups()
        context_path = context_path.strip()
        if not context_path:
            return None

        return task_name, step_name, row_field, context_path

    def resolve_accumulated_display(
        self,
        step: Dict[str, Any],
        context: Dict[str, Any],
        live_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        accumulation = step.get("accumulation", False)
        if accumulation is not True and not isinstance(accumulation, str):
            return live_result

        if accumulation is True:
            return live_result

        expression = accumulation.strip()
        parsed = self._parse_accumulation_expression(expression)
        if parsed is None:
            self.log(
                f"[DEBUG] accumulation step={step.get('name', 'step')} unsupported_expression={expression}"
            )
            return live_result

        task_name, accumulated_step_name, row_field, context_path = parsed
        current_task_name = str(self.task.get("name", ""))
        if task_name != current_task_name:
            self.log(
                f"[DEBUG] accumulation step={step.get('name', 'step')} task_mismatch={task_name}!={current_task_name}"
            )

        accumulated_data = self._load_accumulated_store()
        rows_raw = accumulated_data.get(accumulated_step_name, [])
        rows = self._records_from_step_data(rows_raw)
        live_rows = self._records_from_step_data(live_result)

        if not row_field or not context_path:
            self.log(
                f"[DEBUG] accumulation step={step.get('name', 'step')} loaded_rows={len(rows)} without_filter"
            )
            if rows:
                return self._rows_to_result(rows)
            return self._rows_to_result(live_rows)

        allowed_values = {
            value
            for value in self._resolve_accumulation_context_values(context, context_path)
            if value is not None and not isinstance(value, dict)
        }
        if not allowed_values:
            self.log(
                f"[DEBUG] accumulation step={step.get('name', 'step')} empty_filter_values accumulated_rows={len(rows)} fallback_to_live={len(live_rows)}"
            )
            if rows:
                return self._rows_to_result(rows)
            return self._rows_to_result(live_rows)

        filtered_rows = [row for row in rows if row.get(row_field) in allowed_values]
        if not filtered_rows and rows:
            self.log(
                f"[DEBUG] accumulation step={step.get('name', 'step')} filter_miss fallback_to_accumulated={len(rows)}"
            )
            return self._rows_to_result(rows)

        if not filtered_rows and live_rows:
            filtered_rows = [row for row in live_rows if row.get(row_field) in allowed_values]
        self.log(
            f"[DEBUG] accumulation step={step.get('name', 'step')} loaded_rows={len(rows)} filtered_rows={len(filtered_rows)} context_values={len(allowed_values)}"
        )
        return self._rows_to_result(filtered_rows)

    def _resolve_source_rows(
        self,
        context: Dict[str, Any],
        source: Any,
        current_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        if source in (None, "", "$current", "current"):
            return list(current_rows or [])

        if isinstance(source, (dict, list)):
            return rows_from_any(source)

        if not isinstance(source, str):
            return rows_from_any(source)

        if source.startswith("$.accumulated_data."):
            resolved = self.resolve_accumulated_display(
                {"name": "transform", "accumulation": source},
                context,
                {},
            )
            return self._records_from_step_data(resolved)

        if source in context:
            return rows_from_any(context[source])

        values = jsonpath_values(context, source)
        if values:
            return rows_from_any(flatten_single(values))
        return []

    def _normalize_sort_keys(self, config: Any) -> List[Dict[str, Any]]:
        if not isinstance(config, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for item in config:
            if isinstance(item, str):
                normalized.append({"field": item, "direction": "asc"})
            elif isinstance(item, dict):
                normalized.append(item)
        return normalized

    def _apply_transform_operation(
        self,
        step_name: str,
        operation: Dict[str, Any],
        context: Dict[str, Any],
        current_rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        raw_type = str(operation.get("type") or operation.get("name") or "")
        op_type = raw_type.strip().upper().replace("-", "_").replace(" ", "_")
        if not op_type:
            return current_rows

        if op_type == "ACCUMULATE":
            mode = str(operation.get("mode") or "").lower()
            source = operation.get("source")
            target_step = str(operation.get("target") or step_name)

            if mode in {"load", "read"} or source:
                loaded_rows = self._resolve_source_rows(context, source, current_rows)
                if operation.get("appendCurrent"):
                    return list(current_rows) + loaded_rows
                return loaded_rows

            rows_to_store = list(current_rows)
            if rows_to_store:
                self.append_accumulated(target_step, records_to_dict_of_lists(rows_to_store))
            if operation.get("reload"):
                store = self._load_accumulated_store()
                return rows_from_any(store.get(target_step, []))
            return current_rows

        if op_type == "MERGE":
            left_rows = self._resolve_source_rows(
                context,
                operation.get("leftSource", "$current"),
                current_rows,
            )
            right_rows = self._resolve_source_rows(
                context,
                operation.get("rightSource") or operation.get("source") or operation.get("with"),
                current_rows,
            )
            join_keys = operation.get("joinKeys", []) or []
            join_type = str(operation.get("joinType") or "INNER_JOIN")
            merged_rows = merge_record_sets(
                left_rows,
                right_rows,
                join_keys,
                join_type,
                str(operation.get("leftPrefix", "left.")),
                str(operation.get("rightPrefix", "right.")),
            )
            self.log(
                f"[DEBUG] transform step={step_name} op=MERGE left={len(left_rows)} right={len(right_rows)} merged={len(merged_rows)}"
            )
            return merged_rows

        if op_type == "DUPLICATE":
            copies = operation.get("copies")
            if not copies and operation.get("field") and operation.get("targetField"):
                copies = [
                    {
                        "field": operation.get("field"),
                        "target": operation.get("targetField"),
                        "default": operation.get("default"),
                    }
                ]
            distinct_keys = operation.get("distinctKeys")
            if not distinct_keys and operation.get("distinct"):
                distinct_keys = operation.get("keys") or []
            return duplicate_rows(
                current_rows,
                copies=copies if isinstance(copies, list) else None,
                times=max(int(operation.get("times", 1)), 1),
                distinct_keys=distinct_keys if isinstance(distinct_keys, list) else None,
            )

        if op_type in {"GROUP_BY", "GROUP"}:
            group_keys = operation.get("keys") or operation.get("groupBy") or []
            return group_records(
                current_rows,
                list(group_keys) if isinstance(group_keys, list) else [],
                operation.get("aggregations") if isinstance(operation.get("aggregations"), list) else None,
            )

        if op_type == "PIVOT":
            index_fields = operation.get("index") or operation.get("keys") or []
            return pivot_records(
                current_rows,
                list(index_fields) if isinstance(index_fields, list) else [],
                str(operation.get("columnField") or operation.get("column") or ""),
                str(operation.get("valueField") or operation.get("value") or ""),
                str(operation.get("aggregator") or operation.get("op") or "first"),
                operation.get("fillValue"),
                str(operation.get("columnPrefix") or ""),
            )

        if op_type == "SORT":
            return sort_records(current_rows, self._normalize_sort_keys(operation.get("keys") or operation.get("sortBy")))

        self.log(f"[DEBUG] transform step={step_name} unsupported_op={raw_type}")
        return current_rows

    def apply_step_transforms(
        self,
        step: Dict[str, Any],
        context: Dict[str, Any],
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        transforms = step.get("transforms") or step.get("pipeline") or []
        if not isinstance(transforms, list) or not transforms:
            return result

        step_name = str(step.get("name", "transform"))
        rows = self._records_from_step_data(result)
        for operation in transforms:
            if not isinstance(operation, dict):
                continue
            rows = self._apply_transform_operation(step_name, operation, context, rows)
        self.log(
            f"[DEBUG] transform step={step_name} pipeline_ops={len(transforms)} rows={len(rows)}"
        )
        return records_to_dict_of_lists(rows)

    def run_transform_step(
        self,
        step: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        base_rows = self._resolve_source_rows(context, step.get("source"), [])
        transforms = step.get("transforms") or step.get("pipeline") or []
        step_name = str(step.get("name", "transform"))
        rows = list(base_rows)
        for operation in transforms:
            if not isinstance(operation, dict):
                continue
            rows = self._apply_transform_operation(step_name, operation, context, rows)
        self.log(
            f"[DEBUG] DATA_TRANSFORM step={step_name} base_rows={len(base_rows)} result_rows={len(rows)}"
        )
        return records_to_dict_of_lists(rows)

    # ------------------------------------------------------------------
    # Step runners
    # ------------------------------------------------------------------

    def run_http_step(
        self,
        step: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Chạy một HTTP_REQUEST step:
          1. Resolve payload từ context.
          2. Build Nexacro XML.
          3. Kiểm tra cache (nếu cache=true).
          4. Gửi request (với retry) nếu không có cache.
          5. Parse XML response → JSON → apply extracts.
          6. Nếu loopOver, lặp qua tất cả items và gộp kết quả.
        """
        step_name = step.get("name", "http_step")
        payload_template = step.get("payload", {})
        url = step.get("url", "")
        method = str(step.get("method", "POST")).upper()
        extracts = normalize_extracts(step.get("extracts"))
        use_cache = bool(step.get("cache", False))

        loop_over = step.get("loopOver")
        if loop_over:
            loop_items = flatten_single(jsonpath_values(context, loop_over))
            if not isinstance(loop_items, list):
                loop_items = []
        else:
            loop_items = [None]  # Chạy đúng 1 lần, không loop

        self.log(
            f"[DEBUG] HTTP step={step_name} method={method} loop_count={len(loop_items)} extracts={len(extracts)}"
        )

        aggregate_rows: List[Dict[str, Any]] = []
        aggregate_cols: Dict[str, List[Any]] = {}
        cache_used = False

        for idx, _ in enumerate(loop_items):
            resolved = self.resolve_payload(
                payload_template, context, idx if loop_over else None
            )
            xml_payload = payload_to_nexacro_xml(resolved)
            self.log(
                f"[DEBUG] HTTP step={step_name} iteration={idx + 1}/{len(loop_items)} payload={self._payload_summary(resolved)}"
            )

            # Cache key = MD5(task_name | step_name | xml_payload)
            cache_key = hashlib.md5(
                f"{self.task.get('name')}|{step_name}|"
                f"{xml_payload.decode('utf-8', errors='ignore')}".encode("utf-8")
            ).hexdigest()
            self.log(
                f"[DEBUG] HTTP step={step_name} cache_key={cache_key[:12]} payload_size={len(xml_payload)}"
            )

            step_data: Optional[Dict[str, Any]] = None
            if use_cache:
                step_data = self.read_cache(cache_key)
                if step_data is not None:
                    cache_used = True
                    self.log(
                        f"Cache hit: {step_name} "
                        f"({idx + 1}/{len(loop_items)})"
                    )
                    self.log(
                        f"[DEBUG] HTTP step={step_name} cached_items={len(self._records_from_step_data(step_data))}"
                    )

            self.log(
                f"[DEBUG--] HTTP step={step_name} xml_payload={xml_payload}"
            )
            if step_data is None:
                resp_xml = self.request_with_retry(method, url, xml_payload)

                resp_json = nexacro_xml_to_json(resp_xml)

                self.log(
                    f"[DEBUG] HTTP step={step_name} parsed_response={self._to_json_preview(resp_json, 400)}"
                )

                error_code, error_msg = self._response_error_info(resp_json)
                has_error_code = error_code is not None and error_code != 0
                response_has_data = self._response_has_data(resp_json)
                if has_error_code or error_msg:
                    if self._is_no_data_response(error_code, error_msg) and not response_has_data:
                        self.log(
                            f"[DEBUG] HTTP step={step_name} no_data_response error_code={error_code} error_msg={error_msg}"
                        )
                        step_data = self._empty_extracts_result(extracts)
                    elif has_error_code:
                        if response_has_data:
                            self.log(
                                f"[DEBUG] HTTP step={step_name} nonzero_error_but_has_data error_code={error_code} error_msg={error_msg}"
                            )
                            step_data = self.apply_extracts(extracts, resp_json)
                        else:
                            raise RuntimeError(
                                f"Nexacro error at step '{step_name}': ErrorCode={error_code}, ErrorMsg={error_msg}"
                            )
                    else:
                        step_data = self.apply_extracts(extracts, resp_json)
                else:
                    step_data = self.apply_extracts(extracts, resp_json)

                self.log(
                    f"[DEBUG] HTTP step={step_name} iteration={idx + 1}/{len(loop_items)}"
                )
                if use_cache:
                    self.write_cache(cache_key, step_data)


            # Gộp kết quả vào aggregate
            for row in self._records_from_step_data(step_data):
                aggregate_rows.append(row)
            for k, v in step_data.items():
                if isinstance(v, list):
                    aggregate_cols.setdefault(k, []).extend(v)

        result = dict(aggregate_cols)
        if not result and aggregate_rows:
            result = records_to_dict_of_lists(aggregate_rows)
        self.log(
            f"[DEBUG] HTTP step={step_name} aggregate_items={len(aggregate_rows)}"
        )
        return result, cache_used

    def run_mapping_step(
        self,
        step: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Chạy DATA_MAPPING step:
          - Đọc sourceA, sourceB từ context theo JSONPath.
          - Thực hiện INNER_JOIN theo joinKeys.
          - Áp dụng extracts (JSON_PATH hoặc CALCULATION) trên mỗi cặp row.
        """

        inputs = step.get("inputs", {})
        mapping = step.get("mapping", {})
        extracts = normalize_extracts(step.get("extracts"))

        src_a = flatten_single(jsonpath_values(context, inputs.get("sourceA", "")))
        src_b = flatten_single(jsonpath_values(context, inputs.get("sourceB", "")))

        self.log(
            f"[DEBUG] MAP step={step.get('name', 'mapping')} sourceA_type={type(src_a).__name__} sourceB_type={type(src_b).__name__}"
        )

        # Chuẩn hoá thành list of row-dicts
        left = (
            src_a if isinstance(src_a, list) and src_a and isinstance(src_a[0], dict)
            else dict_of_lists_to_records(src_a) if isinstance(src_a, dict)
            else []
        )
        right = (
            src_b if isinstance(src_b, list) and src_b and isinstance(src_b[0], dict)
            else dict_of_lists_to_records(src_b) if isinstance(src_b, dict)
            else []
        )

       

        join_type = mapping.get("joinType", "INNER_JOIN")
        join_keys = mapping.get("joinKeys", [])
        merged = merge_records_inner(left, right, join_keys) if join_type == "INNER_JOIN" else []
        self.log(
            f"[DEBUG] MAP step={step.get('name', 'mapping')} left={len(left)} right={len(right)} merged={len(merged)}"
        )

        out_rows: List[Dict[str, Any]] = []
        for pair in merged:
            row: Dict[str, Any] = {}
            for ext in extracts:
                name = ext.get("name")
                if not name:
                    continue
                ext_type = ext.get("type", "JSON_PATH")
                value = ext.get("value", "")

                if ext_type == "JSON_PATH":
                    temp_ctx = {"sourceA": pair["sourceA"], "sourceB": pair["sourceB"]}
                    vals = jsonpath_values(temp_ctx, value)
                    row[name] = vals[0] if vals else None
                elif ext_type == "CALCULATION":
                    row[name] = evaluate_calc_expression(
                        value,
                        {"sourceA": pair["sourceA"], "sourceB": pair["sourceB"]},
                    )
            out_rows.append(row)

        if out_rows:
            self.log(
                f"[DEBUG] MAP step={step.get('name', 'mapping')} first_row={self._to_json_preview(out_rows[0], 300)}"
            )

        result = records_to_dict_of_lists(out_rows)
        self.log(
            f"[DEBUG] MAP step={step.get('name', 'mapping')} output_items={len(out_rows)}"
        )
        return result

    # ------------------------------------------------------------------
    # Main run loop (QThread.run)
    # ------------------------------------------------------------------

    def run(self) -> None:
        try:
            context: Dict[str, Any] = {}
            context["session"] = (
                (self.task.get("parameters", {}) or {}).get("session", {})
            )
            self.log(
                f"Task start: name={self.task.get('name', 'unknown')} steps={len(self.task.get('steps', []))}"
            )
            self.log(
                f"[DEBUG] session={self._to_json_preview(context['session'], 300)}"
            )

            # Đăng ký input data vào context trước khi chạy các step khác
            for step in self.task.get("steps", []):
                if step.get("type") == "INPUT":
                    context[step.get("name", "input")] = self.input_data
                    self.log(
                        f"[DEBUG] INPUT registered step={step.get('name', 'input')} data={self._to_json_preview(self.input_data, 400)}"
                    )

            for step in self.task.get("steps", []):
                step_name = step.get("name", "step")
                step_type = step.get("type", "")
                self.step_started.emit(step_name)
                self.log(f"▶ Step: {step_name} [{step_type}]")
                self.log(
                    f"[DEBUG] Step config {step_name}: {self._to_json_preview(step, 600)}"
                )

                if step_type == "INPUT":
                    data = context.get(step_name, {})
                    self.step_completed.emit(step_name, data, False)

                elif step_type == "HTTP_REQUEST":
                    result, cache_used = self.run_http_step(step, context)
                    result = self.apply_step_transforms(step, context, result)
                    context[step_name] = result
                    if step.get("accumulation", False):
                        self.append_accumulated(step_name, result)
                    display_result = self.resolve_accumulated_display(step, context, result)
                    self.step_completed.emit(step_name, display_result, cache_used)

                elif step_type == "DATA_MAPPING":
                    result = self.run_mapping_step(step, context)
                    result = self.apply_step_transforms(step, context, result)
                    context[step_name] = result
                    if step.get("accumulation", False):
                        self.append_accumulated(step_name, result)
                    display_result = self.resolve_accumulated_display(step, context, result)
                    self.step_completed.emit(step_name, display_result, False)

                elif step_type == "DATA_TRANSFORM":
                    result = self.run_transform_step(step, context)
                    context[step_name] = result
                    if step.get("accumulation", False):
                        self.append_accumulated(step_name, result)
                    display_result = self.resolve_accumulated_display(step, context, result)
                    self.step_completed.emit(step_name, display_result, False)

                else:
                    raise RuntimeError(f"Unsupported step type: {step_type!r}")

            self.all_done.emit(context)
            self.log(
                f"Task completed: context_keys={list(context.keys())}"
            )

        except Exception as ex:
            self.failed.emit(f"{ex}\n{traceback.format_exc()}")