"""
output_tab.py – Widget cho các tab OUTPUT step (HTTP_REQUEST / DATA_MAPPING):
  - Bảng QTableWidget hiển thị kết quả extracts.
  - Nút Export Excel (dùng pandas + openpyxl).
  - Checkbox hiển thị trạng thái Cache.
  - Hàm populate_output_table() để cập nhật bảng từ kết quả step.
"""

import os
from typing import Any, Dict, List

import pandas as pd
from PyQt6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


def build_output_tab(
    step: Dict[str, Any],
    table_registry: Dict[str, QTableWidget],
    cache_check_registry: Dict[str, QCheckBox],
    log_fn,
) -> QWidget:
    """
    Tạo QWidget cho tab OUTPUT:
      - Toolbar: nút Export Excel + checkbox Cache.
      - QTableWidget trống (sẽ được điền sau khi step chạy xong).

    Tham số:
      table_registry       – dict để đăng ký bảng ra WorkflowApp.
      cache_check_registry – dict để đăng ký checkbox ra WorkflowApp.
      log_fn               – callable(str) để ghi log ra main window.
    """
    step_name = step.get("name", "output_step")

    tab = QWidget()
    vbox = QVBoxLayout(tab)

    # --- Toolbar ---
    toolbar = QHBoxLayout()

    export_btn = QPushButton("Export Excel")
    toolbar.addWidget(export_btn)

    cache_chk = QCheckBox("Cache")
    cache_chk.setEnabled(False)
    toolbar.addWidget(cache_chk)

    toolbar.addStretch(1)
    vbox.addLayout(toolbar)

    # --- Result table ---
    table = QTableWidget(0, 0)
    table.horizontalHeader().setSectionResizeMode(
        QHeaderView.ResizeMode.ResizeToContents
    )
    vbox.addWidget(table)

    table_registry[step_name] = table
    cache_check_registry[step_name] = cache_chk

    export_btn.clicked.connect(
        lambda: _export_excel(step_name, table, log_fn, tab)
    )

    return tab


def populate_output_table(
    table: QTableWidget,
    rows: List[Dict[str, Any]],
) -> None:
    """
    Điền dữ liệu danh sách row-dict vào QTableWidget.
    Reset toàn bộ bảng rồi ghi lại từ đầu.
    """
    if not rows:
        table.setRowCount(0)
        table.setColumnCount(0)
        return

    safe_rows: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            safe_rows.append(row)
        else:
            safe_rows.append({"value": "" if row is None else str(row)})

    if not safe_rows:
        table.setRowCount(0)
        table.setColumnCount(0)
        return

    columns: List[str] = []
    for row in safe_rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    table.setColumnCount(len(columns))
    table.setHorizontalHeaderLabels(columns)
    table.setRowCount(len(safe_rows))

    for r, row in enumerate(safe_rows):
        for c, key in enumerate(columns):
            val = row.get(key)
            table.setItem(r, c, QTableWidgetItem("" if val is None else str(val)))


def _export_excel(
    step_name: str,
    table: QTableWidget,
    log_fn,
    parent: QWidget,
) -> None:
    """Export toàn bộ dữ liệu trong bảng ra file .xlsx bằng pandas."""
    if table.columnCount() == 0:
        QMessageBox.information(parent, "Thông báo", "Không có dữ liệu để xuất.")
        return

    headers = [
        (table.horizontalHeaderItem(i) or QTableWidgetItem("")).text()
        for i in range(table.columnCount())
    ]
    records = []
    for r in range(table.rowCount()):
        record: Dict[str, Any] = {}
        for c, h in enumerate(headers):
            item = table.item(r, c)
            record[h] = item.text() if item else ""
        records.append(record)

    df = pd.DataFrame(records)
    out_path, _ = QFileDialog.getSaveFileName(
        parent, "Lưu Excel", f"{step_name}.xlsx", "Excel (*.xlsx)"
    )
    if not out_path:
        return

    try:
        df.to_excel(out_path, index=False)
        log_fn(f"Exported: {os.path.basename(out_path)}")
    except Exception as ex:
        QMessageBox.critical(parent, "Lỗi", f"Không thể xuất file:\n{ex}")
