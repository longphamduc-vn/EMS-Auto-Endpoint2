"""
output_tab.py – Widget cho các tab OUTPUT step (HTTP_REQUEST / DATA_MAPPING):
  - Bảng QTableWidget hiển thị kết quả extracts.
  - Nút Export Excel (dùng pandas + openpyxl).
  - Checkbox hiển thị trạng thái Cache.
  - Hàm populate_output_table() để cập nhật bảng từ kết quả step.
  - Hàm auto_export_xlsb() để tự động xuất file .xlsb sau khi workflow xong.
"""

import os
import sys
import tempfile
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QKeySequence
from PyQt6.QtWidgets import (
    QApplication,
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


def _matches_condition(actual_value: Any, rule: Dict[str, Any]) -> bool:
    operator = str(rule.get("operator") or rule.get("op") or "eq").lower()
    expected = rule.get("value")

    if operator == "eq":
        return actual_value == expected
    if operator == "ne":
        return actual_value != expected
    if operator == "in":
        return isinstance(expected, list) and actual_value in expected
    if operator == "not_in":
        return isinstance(expected, list) and actual_value not in expected
    if operator == "contains":
        return actual_value is not None and str(expected) in str(actual_value)
    if operator == "is_empty":
        return actual_value in (None, "")
    if operator == "is_not_empty":
        return actual_value not in (None, "")

    try:
        if operator == "gt":
            return float(actual_value) > float(expected)
        if operator == "gte":
            return float(actual_value) >= float(expected)
        if operator == "lt":
            return float(actual_value) < float(expected)
        if operator == "lte":
            return float(actual_value) <= float(expected)
    except (TypeError, ValueError):
        return False

    return False


def _resolve_row_style(row: Dict[str, Any], row_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    resolved: Dict[str, Any] = {}
    for rule in row_rules:
        condition = rule.get("when") if isinstance(rule.get("when"), dict) else rule
        field_name = str(condition.get("field") or "")
        if not field_name:
            continue
        if _matches_condition(row.get(field_name), condition):
            for key, value in rule.items():
                if key != "when":
                    resolved[key] = value
    return resolved


def _resolve_column_style(field_name: str, column_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    for rule in column_rules:
        if str(rule.get("field") or rule.get("name") or "") == field_name:
            return {k: v for k, v in rule.items() if k not in {"field", "name"}}
    return {}


def _apply_item_style(item: QTableWidgetItem, style: Dict[str, Any]) -> None:
    background = style.get("background")
    foreground = style.get("foreground")
    alignment = str(style.get("alignment") or "").lower()
    font_weight = str(style.get("fontWeight") or style.get("weight") or "").lower()

    if isinstance(background, str) and background:
        item.setBackground(QColor(background))
    if isinstance(foreground, str) and foreground:
        item.setForeground(QColor(foreground))

    if alignment == "center":
        item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
    elif alignment == "right":
        item.setTextAlignment(int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter))
    elif alignment == "left":
        item.setTextAlignment(int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter))

    if font_weight or style.get("italic"):
        font = item.font()
        if font_weight == "bold":
            font.setBold(True)
        if bool(style.get("italic")):
            font.setItalic(True)
        item.setFont(font)


def _selected_bounds(table: QTableWidget, selection_only: bool) -> tuple[int, int, int, int]:
    if not selection_only or not table.selectedRanges():
        return 0, table.rowCount() - 1, 0, table.columnCount() - 1

    selected = table.selectedRanges()[0]
    return selected.topRow(), selected.bottomRow(), selected.leftColumn(), selected.rightColumn()


def table_to_tsv(
    table: QTableWidget,
    include_headers: bool = True,
    selection_only: bool = True,
) -> str:
    if table.columnCount() == 0:
        return ""

    top_row, bottom_row, left_col, right_col = _selected_bounds(table, selection_only)
    if bottom_row < top_row or right_col < left_col:
        return ""

    lines: List[str] = []
    if include_headers:
        headers = [
            (table.horizontalHeaderItem(col) or QTableWidgetItem("")).text()
            for col in range(left_col, right_col + 1)
        ]
        lines.append("\t".join(headers))

    for row in range(top_row, bottom_row + 1):
        cells: List[str] = []
        for col in range(left_col, right_col + 1):
            item = table.item(row, col)
            cells.append(item.text() if item else "")
        lines.append("\t".join(cells))
    return "\n".join(lines)


def copy_table_to_clipboard(
    table: QTableWidget,
    include_headers: bool = True,
    selection_only: bool = True,
) -> bool:
    text = table_to_tsv(table, include_headers=include_headers, selection_only=selection_only)
    if not text:
        return False
    QApplication.clipboard().setText(text)
    return True


class OutputTableWidget(QTableWidget):
    def keyPressEvent(self, event) -> None:
        if event.matches(QKeySequence.StandardKey.Copy):
            copied = copy_table_to_clipboard(self, include_headers=True, selection_only=True)
            if not copied:
                copy_table_to_clipboard(self, include_headers=True, selection_only=False)
            return
        super().keyPressEvent(event)


def build_output_tab(
    step: Dict[str, Any],
    table_registry: Dict[str, QTableWidget],
    cache_check_registry: Dict[str, QCheckBox],
    log_fn,
    task_name: str = "task",
    base_export_dir: str = r"D:\ems",
) -> QWidget:
    """
    Tạo QWidget cho tab OUTPUT:
      - Toolbar: nút Export Excel + checkbox Cache.
      - QTableWidget trống (sẽ được điền sau khi step chạy xong).

    Tham số:
      table_registry       – dict để đăng ký bảng ra WorkflowApp.
      cache_check_registry – dict để đăng ký checkbox ra WorkflowApp.
      log_fn               – callable(str) để ghi log ra main window.
      task_name            – tên task dùng trong đường dẫn xuất file.
      base_export_dir      – thư mục gốc xuất file (mặc định D:\ems).
    """
    step_name = step.get("name", "output_step")

    tab = QWidget()
    vbox = QVBoxLayout(tab)

    # --- Toolbar ---
    toolbar = QHBoxLayout()

    copy_btn = QPushButton("Copy Table")
    toolbar.addWidget(copy_btn)

    export_btn = QPushButton("Export Excel")
    toolbar.addWidget(export_btn)

    cache_chk = QCheckBox("Cache")
    cache_chk.setEnabled(False)
    toolbar.addWidget(cache_chk)

    toolbar.addStretch(1)
    vbox.addLayout(toolbar)

    # --- Result table ---
    table = OutputTableWidget(0, 0)
    table.horizontalHeader().setSectionResizeMode(
        QHeaderView.ResizeMode.ResizeToContents
    )
    vbox.addWidget(table)

    table_registry[step_name] = table
    cache_check_registry[step_name] = cache_chk

    def _on_export() -> None:
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        path = auto_export_xlsb(
            step_name=step_name,
            table=table,
            task_name=task_name,
            timestamp=ts,
            base_export_dir=base_export_dir,
            log_fn=log_fn,
        )
        if path:
            open_exported_file(path, log_fn)

    def _on_copy() -> None:
        if copy_table_to_clipboard(table, include_headers=True, selection_only=False):
            log_fn(f"[Clipboard] {step_name}: đã copy bảng kèm header.")
        else:
            log_fn(f"[Clipboard] {step_name}: không có dữ liệu để copy.")

    export_btn.clicked.connect(_on_export)
    copy_btn.clicked.connect(_on_copy)

    return tab


def populate_output_table(
    table: QTableWidget,
    rows: List[Dict[str, Any]],
    column_labels: Dict[str, str] | None = None,
    style_rules: Dict[str, Any] | None = None,
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
    headers = [column_labels.get(col, col) for col in columns] if column_labels else columns
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(safe_rows))
    row_rules = list(style_rules.get("rows", [])) if isinstance(style_rules, dict) else []
    column_rules = list(style_rules.get("columns", [])) if isinstance(style_rules, dict) else []

    for r, row in enumerate(safe_rows):
        row_style = _resolve_row_style(row, row_rules)
        for c, key in enumerate(columns):
            val = row.get(key)
            item = QTableWidgetItem("" if val is None else str(val))
            merged_style = dict(row_style)
            merged_style.update(_resolve_column_style(key, column_rules))
            if merged_style:
                _apply_item_style(item, merged_style)
            table.setItem(r, c, item)


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


def _table_to_dataframe(table: QTableWidget) -> Optional[pd.DataFrame]:
    """Đọc toàn bộ dữ liệu từ QTableWidget, trả về DataFrame hoặc None nếu rỗng."""
    if table.columnCount() == 0 or table.rowCount() == 0:
        return None
    headers = [
        (table.horizontalHeaderItem(i) or QTableWidgetItem("")).text()
        for i in range(table.columnCount())
    ]
    records = []
    for r in range(table.rowCount()):
        rec: Dict[str, Any] = {}
        for c, h in enumerate(headers):
            item = table.item(r, c)
            rec[h] = item.text() if item else ""
        records.append(rec)
    return pd.DataFrame(records)


def auto_export_xlsb(
    step_name: str,
    table: QTableWidget,
    task_name: str,
    timestamp: str,
    base_export_dir: str,
    log_fn: Callable[[str], None],
) -> Optional[str]:
    """
    Tự động xuất bảng ra file .xlsb (hoặc .xlsx nếu xlwings không khả dụng).

    Đường dẫn: {base_export_dir}\\{task_name}\\{step_name}_{timestamp}.xlsb

    Trả về đường dẫn file đã xuất, hoặc None nếu thất bại / không có dữ liệu.
    """
    df = _table_to_dataframe(table)
    if df is None:
        log_fn(f"[Export] {step_name}: bảng rỗng, bỏ qua.")
        return None

    # Chuẩn hoá tên để dùng trong path
    safe_task = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in task_name)
    safe_step = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in step_name)

    export_dir = os.path.join(base_export_dir, safe_task)
    os.makedirs(export_dir, exist_ok=True)

    xlsb_path = os.path.join(export_dir, f"{safe_step}_{timestamp}.xlsb")

    # --- Thử xuất .xlsb qua xlwings (chỉ hoạt động trên Windows + Excel) ---
    if sys.platform == "win32":
        try:
            import xlwings as xw  # type: ignore

            tmp_xlsx = os.path.join(tempfile.gettempdir(), f"_ems_tmp_{safe_step}_{timestamp}.xlsx")
            df.to_excel(tmp_xlsx, index=False)

            app = xw.App(visible=False)
            try:
                wb = app.books.open(tmp_xlsx)
                # xlsb format code = 50 (xlExcel12)
                wb.api.SaveAs(xlsb_path, FileFormat=50)
                wb.close()
            finally:
                app.quit()

            try:
                os.unlink(tmp_xlsx)
            except OSError:
                pass

            log_fn(f"[Export] {step_name} → {xlsb_path}")
            return xlsb_path

        except ImportError:
            log_fn(f"[Export] xlwings không tìm thấy, fallback sang .xlsx")
        except Exception as ex:
            log_fn(f"[Export] Lỗi xlwings ({ex}), fallback sang .xlsx")

    # --- Fallback: xuất .xlsx ---
    xlsx_path = os.path.join(export_dir, f"{safe_step}_{timestamp}.xlsx")
    try:
        df.to_excel(xlsx_path, index=False)
        log_fn(f"[Export] {step_name} → {xlsx_path}")
        return xlsx_path
    except Exception as ex:
        log_fn(f"[Export] Lỗi xuất file {step_name}: {ex}")
        return None


def open_exported_file(path: str, log_fn: Callable[[str], None]) -> None:
    """Mở file vừa xuất bằng ứng dụng mặc định của hệ điều hành."""
    try:
        if sys.platform == "win32":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            import subprocess
            subprocess.Popen(["open", path])
        else:
            import subprocess
            subprocess.Popen(["xdg-open", path])
        log_fn(f"[Export] Đã mở: {os.path.basename(path)}")
    except Exception as ex:
        log_fn(f"[Export] Không thể mở file: {ex}")
