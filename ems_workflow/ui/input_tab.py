"""
input_tab.py – Widget cho tab INPUT step:
  - InputTableWidget: QTableWidget hỗ trợ paste nhiều dòng/cột từ Excel.
  - build_input_tab(): sinh widget tab INPUT hoàn chỉnh gồm bảng nhập liệu
    và thanh quản lý preset (Save / Load).
"""

import os
from typing import Any, Callable, Dict, List

from PyQt6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)
from PyQt6.QtGui import QKeySequence
from PyQt6.QtWidgets import QHeaderView

from ..constants import SAVED_FILTERS_FILE
from ..utils import load_json, save_json


class InputTableWidget(QTableWidget):
    """
    QTableWidget mở rộng, hỗ trợ Ctrl+V paste dữ liệu dạng TSV
    (tab-separated) từ clipboard – tương thích copy từ Excel.
    """

    def keyPressEvent(self, event) -> None:
        if event.matches(QKeySequence.StandardKey.Paste):
            self._paste_from_clipboard()
            return
        super().keyPressEvent(event)

    def _paste_from_clipboard(self) -> None:
        text = QApplication.clipboard().text()
        if not text.strip():
            return

        rows = [r for r in text.splitlines() if r.strip()]
        start_row = max(self.currentRow(), 0)
        start_col = max(self.currentColumn(), 0)

        for i, row_text in enumerate(rows):
            cells = row_text.split("\t")
            row_idx = start_row + i
            if row_idx >= self.rowCount():
                self.insertRow(self.rowCount())
            for j, cell in enumerate(cells):
                col_idx = start_col + j
                if col_idx < self.columnCount():
                    self.setItem(row_idx, col_idx, QTableWidgetItem(cell.strip()))


def collect_input_data(
    step: Dict[str, Any], table: InputTableWidget
) -> Dict[str, List[str]]:
    """
    Đọc toàn bộ dữ liệu từ bảng nhập liệu, bỏ qua hàng trống.
    Trả về dict {field_name: [val1, val2, ...]} + key "items" alias cột đầu tiên.
    """
    fields = step.get("fields", [])
    col_map = [f.get("name", f"col_{i}") for i, f in enumerate(fields)]
    out: Dict[str, List[str]] = {k: [] for k in col_map}

    for r in range(table.rowCount()):
        row_vals: Dict[str, str] = {}
        has_data = False
        for c, key in enumerate(col_map):
            item = table.item(r, c)
            val = item.text().strip() if item else ""
            row_vals[key] = val
            if val:
                has_data = True
        if has_data:
            for k, v in row_vals.items():
                out[k].append(v)

    # "items" = alias của cột đầu tiên, dùng cho loopOver và ds_ItemInfo
    out["items"] = out[col_map[0]] if col_map else []
    return out


def fill_input_table(
    step: Dict[str, Any],
    table: InputTableWidget,
    data: Dict[str, List[str]],
) -> None:
    """Điền dữ liệu preset vào bảng nhập liệu."""
    fields = step.get("fields", [])
    col_map = [f.get("name", f"col_{i}") for i, f in enumerate(fields)]
    max_rows = max((len(data.get(k, [])) for k in col_map), default=0)
    table.setRowCount(max(30, max_rows))
    table.clearContents()

    for c, key in enumerate(col_map):
        for r, val in enumerate(data.get(key, [])):
            table.setItem(r, c, QTableWidgetItem(str(val)))


def build_input_tab(
    step: Dict[str, Any],
    task_name: str,
    base_dir: str,
    table_registry: Dict[str, InputTableWidget],
) -> QWidget:
    """
    Tạo QWidget cho tab INPUT gồm:
      - Thanh Preset: ô nhập tên, combo chọn, nút Save / Load.
      - InputTableWidget để nhập/paste liệu.

    Tham số table_registry dùng để đăng ký widget bảng ra ngoài
    để WorkflowApp có thể đọc dữ liệu khi Run.
    """
    step_name = step.get("name", "input")
    fields = step.get("fields", [])

    tab = QWidget()
    vbox = QVBoxLayout(tab)

    # --- Preset bar ---
    top = QHBoxLayout()
    top.addWidget(QLabel("Preset:"))

    preset_name_edit = QLineEdit()
    preset_name_edit.setPlaceholderText("Tên preset...")
    top.addWidget(preset_name_edit)

    preset_combo = QComboBox()
    top.addWidget(preset_combo)

    save_btn = QPushButton("Save Preset")
    load_btn = QPushButton("Load Preset")
    top.addWidget(save_btn)
    top.addWidget(load_btn)
    top.addStretch(1)
    vbox.addLayout(top)

    # --- Input table ---
    table = InputTableWidget(30, max(1, len(fields)))
    table.setHorizontalHeaderLabels(
        [f.get("label", f.get("name", f"Col {i}")) for i, f in enumerate(fields)]
    )
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    vbox.addWidget(table)

    table_registry[step_name] = table

    filters_path = os.path.join(base_dir, SAVED_FILTERS_FILE)

    def refresh_presets() -> None:
        preset_combo.clear()
        saved = load_json(filters_path, {})
        presets = saved.get(task_name, {})
        preset_combo.addItems(sorted(presets.keys()))

    def save_preset() -> None:
        name = preset_name_edit.text().strip()
        if not name:
            QMessageBox.warning(tab, "Cảnh báo", "Vui lòng nhập tên preset.")
            return
        # Ghi preset input (điều kiện lọc) vào saved_filters.json
        saved = load_json(filters_path, {})
        saved.setdefault(task_name, {})
        preset_data = saved[task_name].setdefault(name, {})
        preset_data[step_name] = collect_input_data(step, table)
        save_json(filters_path, saved)
        refresh_presets()

    def load_preset() -> None:
        selected = preset_combo.currentText().strip()
        if not selected:
            return
        saved = load_json(filters_path, {})
        data = saved.get(task_name, {}).get(selected, {}).get(step_name, {})
        fill_input_table(step, table, data)

    save_btn.clicked.connect(save_preset)
    load_btn.clicked.connect(load_preset)
    refresh_presets()

    return tab
