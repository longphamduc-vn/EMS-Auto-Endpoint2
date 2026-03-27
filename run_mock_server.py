"""
Mock Nexacro server để chạy thử local với workflow.json hiện tại.

Chạy:
    python run_mock_server.py
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional
from xml.etree.ElementTree import Element, SubElement, fromstring, tostring


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data_mock")
NEXACRO_NS = "http://www.nexacroplatform.com/platform/dataset"


def load_mock_rows(file_name: str) -> List[Dict[str, Any]]:
    with open(os.path.join(DATA_DIR, file_name), "r", encoding="utf-8") as file:
        payload = json.load(file)
    response = payload.get("response", [])
    return response if isinstance(response, list) else []


def parse_request_datasets(xml_bytes: bytes) -> Dict[str, List[Dict[str, str]]]:
    try:
        root = fromstring(xml_bytes)
    except Exception:
        return {}

    datasets: Dict[str, List[Dict[str, str]]] = {}
    for dataset in root.findall(".//Dataset"):
        dataset_id = dataset.attrib.get("id", "dataset")
        rows: List[Dict[str, str]] = []
        for row in dataset.findall(".//Rows/Row"):
            row_data: Dict[str, str] = {}
            for col in row.findall("Col"):
                row_data[col.attrib.get("id", "")] = (col.text or "").strip()
            if row_data:
                rows.append(row_data)
        datasets[dataset_id] = rows
    return datasets


def build_response_xml(dataset_id: str, rows: Iterable[Dict[str, Any]]) -> bytes:
    rows = list(rows)
    columns: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    root = Element("Root", xmlns=NEXACRO_NS)
    dataset_el = SubElement(root, "Dataset", id=dataset_id)
    column_info_el = SubElement(dataset_el, "ColumnInfo")
    rows_el = SubElement(dataset_el, "Rows")

    for column in columns:
        SubElement(column_info_el, "Column", id=column, type="STRING", size="256")

    for row in rows:
        row_el = SubElement(rows_el, "Row")
        for column in columns:
            col_el = SubElement(row_el, "Col", id=column)
            value = row.get(column)
            col_el.text = "" if value is None else str(value)

    return tostring(root, encoding="utf-8", xml_declaration=True)


class MockNexacroHandler(BaseHTTPRequestHandler):
    stock_status_rows = load_mock_rows("StockStatusList.json")
    stock_detail_rows = load_mock_rows("StockDetail.json")

    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        self.send_error(404, "Not found")

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length)
        datasets = parse_request_datasets(request_body)

        if self.path == "/api/nexacro/RetStockStatusList":
            items = {
                row.get("item", "")
                for row in datasets.get("ds_ItemInfo", [])
                if row.get("item")
            }
            if items:
                rows = [row for row in self.stock_status_rows if row.get("itemName") in items]
            else:
                rows = self.stock_status_rows
            self._write_xml(build_response_xml("ds_output", rows))
            return

        if self.path == "/api/nexacro/retrieveItemInfoDetail":
            request_rows = datasets.get("ds_Input", [])
            inventory_item_id = request_rows[0].get("inventoryItemId") if request_rows else None
            if inventory_item_id:
                status_row = next(
                    (row for row in self.stock_status_rows if row.get("inventoryItemId") == inventory_item_id),
                    None,
                )
                item_name = status_row.get("itemName") if status_row else None
                rows = [row for row in self.stock_detail_rows if row.get("item") == item_name]
            else:
                rows = self.stock_detail_rows
            self._write_xml(build_response_xml("ds_output", rows))
            return

        self.send_error(404, "Not found")

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_xml(self, payload: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), MockNexacroHandler)
    print(f"Mock Nexacro server listening at http://{host}:{port}")
    print("Health check: http://127.0.0.1:8000/health")
    server.serve_forever()


if __name__ == "__main__":
    main()