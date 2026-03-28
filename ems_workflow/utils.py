"""
utils.py – Các hàm tiện ích dùng chung:
  - Đọc/ghi JSON file an toàn
  - Chuẩn hoá cấu trúc extracts từ config
  - JSONPath query wrapper
  - Các helper chuyển đổi dữ liệu nội bộ
  - Đánh giá biểu thức CALCULATION
  - INNER_JOIN hai danh sách record
"""

import datetime
import json
import math
import os
import re
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Dict, List, Optional, Tuple

from jsonpath_ng.ext import parse


# ---------------------------------------------------------------------------
# JSON file helpers
# ---------------------------------------------------------------------------

def ensure_json_file(path: str, default: Any) -> None:
    """Tạo file JSON với giá trị mặc định nếu chưa tồn tại."""
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)


def load_json(path: str, default: Any) -> Any:
    """Đọc file JSON; trả về default nếu file không tồn tại hoặc lỗi parse."""
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


class _JsonEncoder(json.JSONEncoder):
    """Encoder mở rộng: tự chuyển datetime/date/time sang chuỗi ISO-8601."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        return super().default(obj)


def save_json(path: str, data: Any) -> None:
    """Ghi dữ liệu ra file JSON (UTF-8, indent=2)."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, cls=_JsonEncoder)


# ---------------------------------------------------------------------------
# Extracts normalisation
# ---------------------------------------------------------------------------

def normalize_extracts(extracts: Any) -> List[Dict[str, Any]]:
    """
    Chuẩn hoá trường extracts trong config:
      - Có thể là list phẳng hoặc list-of-list (trường hợp step có nhiều nhóm).
    Luôn trả về list phẳng gồm các dict extract.
    """
    if not extracts:
        return []
    # Trường hợp list-of-list (nhóm nhiều extract)
    if isinstance(extracts, list) and extracts and isinstance(extracts[0], list):
        result: List[Dict[str, Any]] = []
        for chunk in extracts:
            if isinstance(chunk, list):
                result.extend([x for x in chunk if isinstance(x, dict)])
        return result
    # Trường hợp list phẳng thông thường
    if isinstance(extracts, list):
        return [x for x in extracts if isinstance(x, dict)]
    return []


# ---------------------------------------------------------------------------
# JSONPath helpers
# ---------------------------------------------------------------------------

def jsonpath_values(doc: Any, expr: str) -> List[Any]:
    """Trả về danh sách giá trị khớp với JSONPath expression trong doc."""
    try:
        matches = parse(expr).find(doc)
        return [m.value for m in matches]
    except Exception:
        return []


def flatten_single(value: Any) -> Any:
    """Nếu list chỉ có 1 phần tử, trả về phần tử đó; ngược lại giữ nguyên."""
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    return value


# ---------------------------------------------------------------------------
# Record conversion helpers
# ---------------------------------------------------------------------------

def dict_of_lists_to_records(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Chuyển dict dạng columnar {col: [v1,v2,...]} sang list of row-dicts.
    """
    if not isinstance(data, dict):
        return []
    if not data:
        return []

    def _is_sequence_value(value: Any) -> bool:
        if isinstance(value, (str, bytes, bytearray, dict)):
            return False
        return isinstance(value, Sequence)

    seq_values: Dict[str, List[Any]] = {}
    for key, value in data.items():
        if _is_sequence_value(value):
            seq_values[key] = list(value)

    if not seq_values:
        return [data]

    max_len = max(len(values) for values in seq_values.values())
    if max_len == 0:
        return []

    records: List[Dict[str, Any]] = []
    for i in range(max_len):
        rec: Dict[str, Any] = {}
        for key, val in data.items():
            if key in seq_values:
                seq_val = seq_values[key]
                # Nếu mảng chỉ có 1 phần tử nhưng số dòng tối đa > 1 thì lặp lại phần tử đó (broadcast)
                if len(seq_val) == 1 and max_len > 1:
                    rec[key] = seq_val[0]
                else:
                    rec[key] = seq_val[i] if i < len(seq_val) else None
            else:
                rec[key] = val
        records.append(rec)
    return records


def records_to_dict_of_lists(rows: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    """Chuyển list row-dicts sang dict dạng columnar {col: [v1, v2, ...]}."""
    columns: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    return {
        key: [row.get(key) if isinstance(row, dict) else None for row in rows]
        for key in columns
    }


def rows_from_any(data: Any) -> List[Dict[str, Any]]:
    """Chuẩn hoá dữ liệu bất kỳ thành danh sách row-dicts."""
    if isinstance(data, list):
        rows: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append({"value": item})
        return rows

    if isinstance(data, dict):
        if isinstance(data.get("items"), list) and all(
            isinstance(item, dict) for item in data.get("items", [])
        ):
            return list(data["items"])
        return dict_of_lists_to_records(data)

    if data is None:
        return []

    return [{"value": data}]


def get_value_by_path(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Đọc giá trị theo field path dạng a.b.c; ưu tiên exact key nếu có."""
    if not path:
        return default
    if isinstance(data, dict) and path in data:
        return data[path]

    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return default
    return default if current is None else current


def merge_record_sets(
    left_records: List[Dict[str, Any]],
    right_records: List[Dict[str, Any]],
    join_keys: List[Dict[str, str]],
    join_type: str = "INNER_JOIN",
    left_prefix: str = "left.",
    right_prefix: str = "right.",
) -> List[Dict[str, Any]]:
    """Join hai tập row và flatten field của hai bên ra cùng một record."""
    normalized_join_type = str(join_type or "INNER_JOIN").upper()
    if not join_keys:
        return []

    def build_key(row: Dict[str, Any], side: str) -> Tuple[Any, ...]:
        key_parts: List[Any] = []
        for key_map in join_keys:
            field_name = key_map.get("leftKey") if side == "left" else key_map.get("rightKey")
            key_parts.append(get_value_by_path(row, str(field_name or "")))
        return tuple(key_parts)

    right_index: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for right in right_records:
        right_index.setdefault(build_key(right, "right"), []).append(right)

    matched_right_ids: set[int] = set()
    merged: List[Dict[str, Any]] = []

    def flatten_row(
        left_row: Optional[Dict[str, Any]],
        right_row: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        combined: Dict[str, Any] = {}
        if isinstance(left_row, dict):
            for key, value in left_row.items():
                target_key = f"{left_prefix}{key}" if left_prefix else key
                combined[target_key] = value
        if isinstance(right_row, dict):
            for key, value in right_row.items():
                target_key = f"{right_prefix}{key}" if right_prefix else key
                if target_key in combined:
                    target_key = f"right.{key}"
                combined[target_key] = value
        return combined

    for left in left_records:
        key = build_key(left, "left")
        matches = right_index.get(key, [])
        if matches:
            for right in matches:
                merged.append(flatten_row(left, right))
                matched_right_ids.add(id(right))
        elif normalized_join_type in {"LEFT_JOIN", "LEFT", "FULL_JOIN", "OUTER_JOIN", "OUTER"}:
            merged.append(flatten_row(left, None))

    if normalized_join_type in {"RIGHT_JOIN", "RIGHT", "FULL_JOIN", "OUTER_JOIN", "OUTER"}:
        for right in right_records:
            if id(right) not in matched_right_ids:
                merged.append(flatten_row(None, right))

    return merged


def duplicate_rows(
    rows: List[Dict[str, Any]],
    *,
    copies: Optional[List[Dict[str, Any]]] = None,
    times: int = 1,
    distinct_keys: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Hỗ trợ duplicate field, nhân bản row hoặc loại row trùng theo key."""
    if distinct_keys:
        deduped: List[Dict[str, Any]] = []
        seen: set[Tuple[Any, ...]] = set()
        for row in rows:
            signature = tuple(get_value_by_path(row, key) for key in distinct_keys)
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(dict(row))
        rows = deduped

    if copies:
        duplicated_fields: List[Dict[str, Any]] = []
        for row in rows:
            new_row = dict(row)
            for field_copy in copies:
                source = str(field_copy.get("field") or field_copy.get("source") or "")
                target = str(field_copy.get("target") or field_copy.get("name") or "")
                if not source or not target:
                    continue
                new_row[target] = get_value_by_path(row, source, field_copy.get("default"))
            duplicated_fields.append(new_row)
        rows = duplicated_fields

    if times <= 1:
        return rows

    duplicated_rows: List[Dict[str, Any]] = []
    for row in rows:
        for index in range(times):
            new_row = dict(row)
            new_row.setdefault("duplicateIndex", index + 1)
            duplicated_rows.append(new_row)
    return duplicated_rows


def aggregate_values(values: List[Any], operator: str, separator: str = ", ") -> Any:
    normalized_operator = str(operator or "first").lower()
    compact_values = [value for value in values if value is not None and value != ""]

    if normalized_operator == "count":
        return len(compact_values)
    if normalized_operator == "sum":
        total = 0.0
        for value in compact_values:
            try:
                total += float(value)
            except (TypeError, ValueError):
                continue
        return int(total) if total.is_integer() else total
    if normalized_operator == "min":
        return min(compact_values) if compact_values else None
    if normalized_operator == "max":
        return max(compact_values) if compact_values else None
    if normalized_operator == "concat":
        return separator.join(str(value) for value in compact_values)
    if normalized_operator == "unique_concat":
        seen: List[Any] = []
        for value in compact_values:
            if value not in seen:
                seen.append(value)
        return separator.join(str(value) for value in seen)
    if normalized_operator == "first":
        return compact_values[0] if compact_values else None
    if normalized_operator == "last":
        return compact_values[-1] if compact_values else None
    return compact_values[0] if compact_values else None


def group_records(
    rows: List[Dict[str, Any]],
    group_keys: List[str],
    aggregations: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Group row theo keys và áp dụng các phép tổng hợp cơ bản."""
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        signature = tuple(get_value_by_path(row, key) for key in group_keys)
        grouped[signature].append(row)

    results: List[Dict[str, Any]] = []
    for signature, group_rows in grouped.items():
        result_row = {key: signature[index] for index, key in enumerate(group_keys)}
        if not aggregations:
            result_row["count"] = len(group_rows)
            results.append(result_row)
            continue

        for aggregation in aggregations:
            field = str(aggregation.get("field") or "")
            operator = str(aggregation.get("op") or aggregation.get("type") or "first")
            target = str(aggregation.get("target") or field or operator)
            separator = str(aggregation.get("separator") or ", ")
            values = [get_value_by_path(row, field) for row in group_rows] if field else group_rows
            result_row[target] = aggregate_values(values, operator, separator)
        results.append(result_row)

    return results


def pivot_records(
    rows: List[Dict[str, Any]],
    index_fields: List[str],
    column_field: str,
    value_field: str,
    aggregator: str = "first",
    fill_value: Any = None,
    column_prefix: str = "",
) -> List[Dict[str, Any]]:
    """Pivot row theo index fields và column/value field."""
    grouped: Dict[Tuple[Any, ...], Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
    pivot_columns: List[str] = []

    for row in rows:
        index_key = tuple(get_value_by_path(row, field) for field in index_fields)
        pivot_key_raw = get_value_by_path(row, column_field)
        if pivot_key_raw is None:
            continue
        pivot_key = f"{column_prefix}{pivot_key_raw}"
        grouped[index_key][pivot_key].append(get_value_by_path(row, value_field))
        if pivot_key not in pivot_columns:
            pivot_columns.append(pivot_key)

    results: List[Dict[str, Any]] = []
    for index_key, value_map in grouped.items():
        result_row = {field: index_key[pos] for pos, field in enumerate(index_fields)}
        for pivot_key in pivot_columns:
            values = value_map.get(pivot_key, [])
            result_row[pivot_key] = (
                aggregate_values(values, aggregator) if values else fill_value
            )
        results.append(result_row)
    return results


def sort_records(rows: List[Dict[str, Any]], sort_keys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort đa cột ổn định, hỗ trợ numeric/text và asc/desc."""
    sorted_rows = list(rows)

    def coerce_sort_value(value: Any, numeric: bool) -> Any:
        if value is None:
            return float("-inf") if numeric else ""
        if numeric:
            try:
                return float(value)
            except (TypeError, ValueError):
                return float("-inf")
        return str(value)

    for sort_key in reversed(sort_keys):
        field = str(sort_key.get("field") or sort_key.get("name") or "")
        if not field:
            continue
        reverse = str(sort_key.get("direction") or "asc").lower() == "desc"
        numeric = bool(sort_key.get("numeric", False))
        sorted_rows.sort(
            key=lambda row, field=field, numeric=numeric: coerce_sort_value(
                get_value_by_path(row, field),
                numeric,
            ),
            reverse=reverse,
        )
    return sorted_rows

def normalize_records(rows_raw: Any) -> Tuple[List[Dict[str, Any]], int]:
    """
    Ép kiểu dữ liệu (coercion) đầu vào thành list các row-dicts.
    Hữu ích cho việc đổ dữ liệu vào DataGrid / Table.
    Trả về: (Danh sách dict đã xử lý, số lượng row phải ép kiểu cưỡng bức)
    """
    rows: List[Dict[str, Any]] = []
    coerced_count = 0
    
    if isinstance(rows_raw, list):
        for item in rows_raw:
            if isinstance(item, dict):
                rows.append(item)
            else:
                coerced_count += 1
                rows.append({"value": "" if item is None else str(item)})
    elif isinstance(rows_raw, dict):
        rows.append(rows_raw)
        coerced_count = 1
    elif rows_raw is not None:
        rows.append({"value": str(rows_raw)})
        coerced_count = 1
        
    return rows, coerced_count

# ---------------------------------------------------------------------------
# CALCULATION expression evaluator
# ---------------------------------------------------------------------------

def _resolve_calc_reference(context: Dict[str, Any], path: str) -> Any:
    current: Any = context
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _find_top_level_ternary(expression: str) -> int:
    depth = 0
    quote: Optional[str] = None
    escaped = False

    for index, char in enumerate(expression):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        if char in ("'", '"'):
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(0, depth - 1)
        elif char == "?" and depth == 0:
            return index

    return -1


def _find_matching_ternary_colon(expression: str, question_index: int) -> int:
    depth = 0
    nested_ternary = 0
    quote: Optional[str] = None
    escaped = False

    for index in range(question_index + 1, len(expression)):
        char = expression[index]

        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        if char in ("'", '"'):
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(0, depth - 1)
        elif depth == 0:
            if char == "?":
                nested_ternary += 1
            elif char == ":":
                if nested_ternary == 0:
                    return index
                nested_ternary -= 1

    return -1


def _convert_js_ternary(expression: str) -> str:
    question_index = _find_top_level_ternary(expression)
    if question_index == -1:
        return expression

    colon_index = _find_matching_ternary_colon(expression, question_index)
    if colon_index == -1:
        return expression

    condition = expression[:question_index].strip()
    true_expr = expression[question_index + 1:colon_index].strip()
    false_expr = expression[colon_index + 1:].strip()

    return (
        f"({_convert_js_ternary(true_expr)}) if ({_convert_js_ternary(condition)}) "
        f"else ({_convert_js_ternary(false_expr)})"
    )

def evaluate_calc_expression(
    expression: str,
    context: Dict[str, Any],
) -> Any:
    """
    Đánh giá biểu thức CALCULATION bằng eval(expression) sau khi chuẩn hoá
    tham chiếu $.scope.field và cú pháp JS phổ biến sang cú pháp Python.
    """
    expr = expression.strip()

    def replace_reference(match: re.Match) -> str:
        return repr(_resolve_calc_reference(context, match.group(1)))

    expr = re.sub(r"\$\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)", replace_reference, expr)
    expr = re.sub(r"\bNumber\s*\(", "float(", expr)
    expr = re.sub(r"\btrue\b", "True", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bfalse\b", "False", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bnull\b", "None", expr, flags=re.IGNORECASE)
    expr = expr.replace("&&", " and ").replace("||", " or ")
    expr = re.sub(r"(?<![<>=!])=(?!=)", "==", expr)
    expr = _convert_js_ternary(expr)

    _eval_globals = {
        "datetime": datetime,
        "math": math,
    }

    try:
        return eval(expr, _eval_globals)
    except Exception as ex:
        print(f"[CALCULATION ERROR] Lỗi khi tính toán biểu thức: {expr} | Chi tiết: {ex}")
        return None


# ---------------------------------------------------------------------------
# INNER JOIN helper
# ---------------------------------------------------------------------------

def merge_records_inner(
    left_records: List[Dict[str, Any]],
    right_records: List[Dict[str, Any]],
    join_keys: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Thực hiện INNER JOIN giữa hai danh sách record theo join_keys.
    Trả về list các dict {"sourceA": left_row, "sourceB": right_row}.
    """
    if not join_keys:
        return []

    # Build hash index phía phải để tăng tốc lookup
    right_index: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for r in right_records:
        key = tuple(r.get(k.get("rightKey")) for k in join_keys)
        right_index.setdefault(key, []).append(r)

    merged: List[Dict[str, Any]] = []
    for left in left_records:
        key = tuple(left.get(k.get("leftKey")) for k in join_keys)
        for right in right_index.get(key, []):
            merged.append({"sourceA": left, "sourceB": right})
    return merged