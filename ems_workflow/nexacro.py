"""
nexacro.py – Chuyển đổi hai chiều giữa định dạng JSON cấu hình
và định dạng XML đặc thù của nền tảng Nexacro.

Hai hàm chính:
  payload_to_nexacro_xml(payload)  →  bytes  (XML gửi lên server)
  nexacro_xml_to_json(xml_text)    →  dict   (JSON nội bộ để JSONPath truy vấn)
"""

from typing import Any, Dict, List
from xml.etree.ElementTree import Element, SubElement, fromstring, tostring

from .constants import NEXACRO_NS


# ---------------------------------------------------------------------------
# JSON payload  →  Nexacro XML request
# ---------------------------------------------------------------------------

def _escape(value: Any) -> str:
    """Chuyển giá trị bất kỳ sang chuỗi an toàn đặt vào XML text node."""
    return "" if value is None else str(value)


def payload_to_nexacro_xml(payload: Dict[str, Any]) -> bytes:
    """
    Build Nexacro XML từ dict payload JSON (đã được resolve JSONPath).

    Cấu trúc payload đầu vào:
      {
        "parameters": {"_ems_userId": "abc", ...},
        "datasets": [
          {
            "id": "ds_search",
            "fields": [{"name": "organizationId", "value": "28753"}, ...]
          }
        ]
      }

    Cấu trúc XML đầu ra:
      <Root xmlns="...">
        <Parameters>
          <Parameter id="_ems_userId">abc</Parameter>
        </Parameters>
        <Dataset id="ds_search">
          <ColumnInfo>
            <Column id="organizationId" type="STRING" size="256"/>
          </ColumnInfo>
          <Rows>
            <Row><Col id="organizationId">28753</Col></Row>
            ...
          </Rows>
        </Dataset>
      </Root>
    """
    root = Element("Root", xmlns=NEXACRO_NS)

    # --- Parameters block ---
    params: Dict[str, Any] = payload.get("parameters", {}) or {}
    if params:
        params_el = SubElement(root, "Parameters")
        for k, v in params.items():
            param_el = SubElement(params_el, "Parameter", id=str(k))
            param_el.text = _escape(v)

    # --- Dataset blocks ---
    for dataset in payload.get("datasets", []) or []:
        ds_id = str(dataset.get("id", ""))
        ds_el = SubElement(root, "Dataset", id=ds_id)
        ci_el = SubElement(ds_el, "ColumnInfo")
        rows_el = SubElement(ds_el, "Rows")

        fields: List[Dict[str, Any]] = dataset.get("fields", []) or []
        field_names = [str(f.get("name", "")) for f in fields]

        # ColumnInfo – mỗi field tương ứng một Column STRING
        for name in field_names:
            SubElement(ci_el, "Column", id=name, type="STRING", size="256")

        # Tính số Row cần sinh (trường hợp field value là list nhiều phần tử)
        columns_data: Dict[str, List[Any]] = {}
        max_rows = 1
        for f in fields:
            name = str(f.get("name", ""))
            value = f.get("value", "")
            if isinstance(value, list):
                columns_data[name] = value
                max_rows = max(max_rows, len(value))
            else:
                columns_data[name] = [value]

        # Rows – mỗi list index tương ứng một Row XML
        for i in range(max_rows):
            row_el = SubElement(rows_el, "Row")
            for name in field_names:
                col_values = columns_data.get(name, [""])
                if not col_values:
                    cell_val = ""
                elif i < len(col_values):
                    cell_val = col_values[i]
                else:
                    cell_val = col_values[-1]
                col_el = SubElement(row_el, "Col", id=name)
                col_el.text = _escape(cell_val)

    return tostring(root, encoding="utf-8", xml_declaration=True)


# ---------------------------------------------------------------------------
# Nexacro XML response  →  JSON nội bộ
# ---------------------------------------------------------------------------

def nexacro_xml_to_json(xml_text: str) -> Dict[str, Any]:
    """
    Parse XML response Nexacro thành dict JSON nội bộ để các bước extracts
    có thể dùng JSONPath như $.response.itemName[*] truy vấn.

    Cấu trúc XML đầu vào (ví dụ):
      <Root>
        <Dataset id="ds_output">
          <Rows>
            <Row><Col id="itemName">Panel A</Col></Row>
            <Row><Col id="itemName">Panel B</Col></Row>
          </Rows>
        </Dataset>
      </Root>

    Cấu trúc JSON đầu ra:
      {
        "response": {"itemName": ["Panel A", "Panel B"], ...},
        "datasets": {"ds_output": [{"itemName": "Panel A"}, ...]},
        "items":    [{"itemName": "Panel A"}, {"itemName": "Panel B"}]
      }

    Ghi chú: key "response" (typo) được giữ nguyên để khớp với
    JSONPath expressions đã định nghĩa trong workflow config.
    """
    output: Dict[str, Any] = {"response": {}, "datasets": {}, "items": []}
    try:
        root = fromstring(xml_text)
    except Exception:
        return output

    all_rows: List[Dict[str, Any]] = []

    for ds in root.findall(".//Dataset"):
        ds_id = ds.attrib.get("id", "dataset")
        ds_rows: List[Dict[str, Any]] = []

        for row in ds.findall(".//Rows/Row"):
            row_data: Dict[str, Any] = {}
            for col in row.findall("Col"):
                key = col.attrib.get("id", "")
                val = (col.text or "").strip()
                row_data[key] = val
                # Tích lũy vào response dưới dạng columnar (list per field)
                output["response"].setdefault(key, []).append(val)
            if row_data:
                ds_rows.append(row_data)
                all_rows.append(row_data)

        output["datasets"][ds_id] = ds_rows

    output["items"] = all_rows
    return output