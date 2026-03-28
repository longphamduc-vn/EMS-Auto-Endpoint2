"""
utils.py – Các hàm tiện ích dùng chung:
  - Đọc/ghi JSON file an toàn
  - Chuẩn hoá cấu trúc extracts từ config
  - JSONPath query wrapper
  - Các helper chuyển đổi dữ liệu nội bộ
  - Đánh giá biểu thức CALCULATION
  - INNER_JOIN hai danh sách record
"""

import json
import os
import re
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
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    """Ghi dữ liệu ra file JSON (UTF-8, indent=2)."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
    Nếu data đã có key 'items' là list-of-dict thì dùng luôn.
    """
    if not isinstance(data, dict):
        return []
    items = data.get("items")
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items

    list_keys = [k for k, v in data.items() if isinstance(v, list)]
    if not list_keys:
        return [data]

    max_len = max(len(data[k]) for k in list_keys)
    records: List[Dict[str, Any]] = []
    for i in range(max_len):
        rec: Dict[str, Any] = {}
        for key, val in data.items():
            if isinstance(val, list):
                rec[key] = val[i] if i < len(val) else None
            else:
                rec[key] = val
        records.append(rec)
    return records

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

def evaluate_calc_expression(
    expression: str,
    source_a: Dict[str, Any],
    source_b: Dict[str, Any],
) -> Any:
    """
    Đánh giá biểu thức CALCULATION dạng:
      $.sourceA.field = 'value' && $.sourceB.field > 0 ? 'True branch' : 'False branch'
    Thay thế tham chiếu JSONPath bằng giá trị thực từ source_a/source_b,
    rồi eval an toàn (không expose builtins).
    """
    expr = expression

    def repl_a(m: re.Match) -> str:
        return repr(source_a.get(m.group(1)))

    def repl_b(m: re.Match) -> str:
        return repr(source_b.get(m.group(1)))

    expr = re.sub(r"\$\.sourceA\.([A-Za-z0-9_]+)", repl_a, expr)
    expr = re.sub(r"\$\.sourceB\.([A-Za-z0-9_]+)", repl_b, expr)

    if "?" in expr and ":" in expr:
        condition, rest = expr.split("?", 1)
        true_val, false_val = rest.split(":", 1)
    else:
        condition, true_val, false_val = expr, "True", "False"

    condition = condition.replace("&&", " and ").replace("||", " or ")
    condition = re.sub(r"(?<![<>=!])=(?!=)", "==", condition)

    try:
        cond_result = bool(eval(condition, {"__builtins__": {}}, {}))
        branch = true_val if cond_result else false_val
        return eval(branch.strip(), {"__builtins__": {}}, {})
    except Exception:
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