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
import json
import os
import traceback
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from PyQt6.QtCore import QThread, pyqtSignal

from .constants import ACCUMULATED_FILE, CACHE_DIR
from .nexacro import nexacro_xml_to_json, payload_to_nexacro_xml
from .utils import (
    dict_of_lists_to_records,
    evaluate_calc_expression,
    flatten_single,
    jsonpath_values,
    load_json,
    merge_records_inner,
    normalize_extracts,
    save_json,
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

    def _extract_fallback_path(self, path: str) -> Optional[str]:
        if not path.startswith("$.response.") or not path.endswith("[*]"):
            return None
        field_name = path[len("$.response.") : -3]
        if not field_name:
            return None
        return f"$.response[*].{field_name}"

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
                if loop_idx is not None:
                    return None
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
        theo danh sách extracts trong config (chỉ hỗ trợ JSON_PATH ở bước này).
        Trả về dict columnar + key "items" là list row-dict.
        """
        result: Dict[str, Any] = {}

        for ext in extracts:
            name = ext.get("name")
            if not name or ext.get("type", "JSON_PATH") != "JSON_PATH":
                continue
            path = str(ext.get("value", ""))
            vals = jsonpath_values(source, path)
            corrected_path = self._extract_fallback_path(path)
            if not vals and corrected_path:
                vals = jsonpath_values(source, corrected_path)

            # Unwrap list-of-list khi JSONPath trả về [[...]]
            if len(vals) == 1 and isinstance(vals[0], list):
                vals = vals[0]
            result[name] = vals

        result["items"] = dict_of_lists_to_records(result)
        return result

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
                return response.text
            except Exception as ex:
                last_error = ex
                self.log(f"Retry {attempt}/3 — {ex}")

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
        current: Dict[str, Any] = load_json(path, {})
        current.setdefault(step_name, [])
        before_count = len(current[step_name])
        current[step_name].extend(data.get("items", []))
        after_count = len(current[step_name])
        save_json(path, current)
        self.log(
            f"[DEBUG] accumulation step={step_name} appended={after_count - before_count} total={after_count} file={path}"
        )

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
                        f"[DEBUG] HTTP step={step_name} cached_items={len(step_data.get('items', []))}"
                    )

            self.log(
                f"[DEBUG] HTTP step={step_name} xml_payload_bytes={len(xml_payload)}"
            )
            if step_data is None:
                resp_xml = self.request_with_retry(method, url, xml_payload)

                resp_json = nexacro_xml_to_json(resp_xml)
          
                
                self.log(
                    f"[DEBUG] HTTP step={step_name} parsed_response={self._to_json_preview(resp_json, 400)}"
                )
                
                step_data = self.apply_extracts(extracts, resp_json)
                self.log(
                    f"[DEBUG] HTTP step={step_name} iteration={idx + 1}/{len(loop_items)}"
                )
                if use_cache:
                    self.write_cache(cache_key, step_data)


            # Gộp kết quả vào aggregate
            for row in step_data.get("items", []):
                aggregate_rows.append(row)
            for k, v in step_data.items():
                if k == "items":
                    continue
                if isinstance(v, list):
                    aggregate_cols.setdefault(k, []).extend(v)

        result: Dict[str, Any] = dict(aggregate_cols)
    
        result["items"] = aggregate_rows
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

        self.log(
            f"[DEBUG] inputs {inputs} mapping {mapping} extracts={extracts}"
        )
        
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
                        value, pair["sourceA"], pair["sourceB"]
                    )
            out_rows.append(row)

        if out_rows:
            self.log(
                f"[DEBUG] MAP step={step.get('name', 'mapping')} first_row={self._to_json_preview(out_rows[0], 300)}"
            )

        result: Dict[str, Any] = {"items": out_rows}
        if out_rows:
            for col in out_rows[0].keys():
                result[col] = [r.get(col) for r in out_rows]
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
                    data = context.get(step_name, {"items": []})
                    self.step_completed.emit(step_name, data, False)

                elif step_type == "HTTP_REQUEST":
                    result, cache_used = self.run_http_step(step, context)
                    context[step_name] = result
                    self.step_completed.emit(step_name, result, cache_used)
                    if step.get("accumulation", False):
                        self.append_accumulated(step_name, result)

                elif step_type == "DATA_MAPPING":
                    result = self.run_mapping_step(step, context)
                    context[step_name] = result
                    self.step_completed.emit(step_name, result, False)
                    if step.get("accumulation", False):
                        self.append_accumulated(step_name, result)

                else:
                    raise RuntimeError(f"Unsupported step type: {step_type!r}")

            self.all_done.emit(context)
            self.log(
                f"Task completed: context_keys={list(context.keys())}"
            )

        except Exception as ex:
            self.failed.emit(f"{ex}\n{traceback.format_exc()}")