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

import xml.etree.ElementTree as ET
from typing import Dict, Any

def nexacro_xml_to_json(xml_text: str) -> Dict[str, Any]:
    # Loại bỏ namespace để dễ truy vấn (hoặc định nghĩa namespace)
    # Nexacro thường dùng xmlns="http://www.nexacroplatform.com/platform/dataset"
    root = ET.fromstring(xml_text)
    ns = {'ns': 'http://www.nexacroplatform.com/platform/dataset'}

    result = {
        "parameters": {},
        "response": {}
    }

    # 1. Xử lý Parameters
    parameters_node = root.find('ns:Parameters', ns)
    if parameters_node is not None:
        for param in parameters_node.findall('ns:Parameter', ns):
            p_id = param.get('id')
            p_value = param.text if param.text else ""
            # Chuyển kiểu dữ liệu cơ bản nếu cần
            p_type = param.get('type')
            if p_type == 'int' and p_value:
                p_value = int(p_value)
            result["parameters"][p_id] = p_value

    # 2. Xử lý tất cả các Dataset
    for dataset in root.findall('ns:Dataset', ns):
        rows_data = []
        rows_node = dataset.find('ns:Rows', ns)
        
        if rows_node is not None:
            for row in rows_node.findall('ns:Row', ns):
                row_dict = {}
                for col in row.findall('ns:Col', ns):
                    col_id = col.get('id')
                    row_dict[col_id] = col.text if col.text is not None else ""
                rows_data.append(row_dict)
        
        # Gộp dữ liệu vào 'response'
        # Nếu bạn muốn gộp tất cả các cột của các dataset vào chung response:
        if rows_data:
            # Lấy record đầu tiên hoặc tất cả record tùy vào logic của bạn
            # Ở đây tôi đưa toàn bộ list record vào key của Dataset ID (ví dụ: ds_Item)
            # Hoặc gộp phẳng các field vào response theo yêu cầu của bạn:
            for record in rows_data:
                for key, value in record.items():
                    if key not in result["response"]:
                        result["response"][key] = []
                    result["response"][key].append(value)
        else:
            # Xử lý trường hợp Row trống nhưng có ColumnInfo
            col_info = dataset.find('ns:ColumnInfo', ns)
            if col_info is not None:
                for col in col_info.findall('ns:Column', ns):
                    c_id = col.get('id')
                    if c_id not in result["response"]:
                        result["response"][c_id] = []

    return result

# --- Test thử với dữ liệu của bạn ---
# json_data = nexacro_xml_to_json(xml_input)
# import json
# print(json.dumps(json_data, indent=4, ensure_ascii=False))