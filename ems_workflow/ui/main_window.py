"""
main_window.py – WorkflowApp: QMainWindow chính của ứng dụng.

Chịu trách nhiệm:
  - Load và chọn task từ workflow.json.
  - Sinh động các tab (INPUT / OUTPUT) theo config step.
  - Điều phối WorkflowWorker (QThread) và nhận signal cập nhật UI.
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from PyQt6.QtGui import QAction
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QTableWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ..constants import (
    ACCUMULATED_FILE,
    FALLBACK_WORKFLOW_FILE,
    SAVED_FILTERS_FILE,
    WORKFLOW_FILE,
)
from ..engine import WorkflowWorker
from ..utils import ensure_json_file, load_json
from .input_tab import InputTableWidget, build_input_tab, collect_input_data
from .output_tab import build_output_tab, populate_output_table


class WorkflowApp(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Nexacro Workflow Engine")
        self.resize(1360, 860)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        # base_dir trỏ vào ems_workflow/ui/ – cần lùi lên 2 cấp để ra thư mục gốc project
        self.base_dir = os.path.normpath(os.path.join(self.base_dir, "..", ".."))

        self.tasks: List[Dict[str, Any]] = []
        self.current_task: Optional[Dict[str, Any]] = None

        # Đăng ký widget bảng và checkbox cache theo step_name
        self.step_tables: Dict[str, QTableWidget] = {}
        self.cache_checks: Dict[str, QCheckBox] = {}
        self.worker: Optional[WorkflowWorker] = None
        self.current_run_log_path: Optional[str] = None
        self.open_workflow_action: Optional[QAction] = None
        self.reload_btn: Optional[QPushButton] = None

        ensure_json_file(os.path.join(self.base_dir, SAVED_FILTERS_FILE), {})
        ensure_json_file(os.path.join(self.base_dir, ACCUMULATED_FILE), {})

        self._init_ui()
        self.load_workflow()

    # ------------------------------------------------------------------
    # UI setup
    # ------------------------------------------------------------------

    def _init_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        # Top bar: task selector + action buttons
        top = QHBoxLayout()
        top.addWidget(QLabel("Task:"))

        self.task_combo = QComboBox()
        self.task_combo.currentIndexChanged.connect(self._on_task_changed)
        top.addWidget(self.task_combo)

        self.run_btn = QPushButton("▶  Run Workflow")
        self.run_btn.clicked.connect(self.run_workflow)
        top.addWidget(self.run_btn)

        self.reload_btn = QPushButton("↺  Reload")
        self.reload_btn.clicked.connect(self.load_workflow)
        top.addWidget(self.reload_btn)

        top.addStretch(1)
        root_layout.addLayout(top)

        # Step tabs
        self.tabs = QTabWidget()
        root_layout.addWidget(self.tabs)

        # Log panel
        self.log_panel = QTextEdit()
        self.log_panel.setReadOnly(True)
        self.log_panel.setFixedHeight(180)
        root_layout.addWidget(self.log_panel)

        # Menu bar shortcut to open another workflow file
        self.open_workflow_action = QAction("Open workflow.json…", self)
        self.open_workflow_action.triggered.connect(self._open_workflow_file)
        self.menuBar().addAction(self.open_workflow_action)

    def _set_execution_state(self, running: bool) -> None:
        self.run_btn.setEnabled(not running)
        self.task_combo.setEnabled(not running)
        if self.reload_btn is not None:
            self.reload_btn.setEnabled(not running)
        if self.open_workflow_action is not None:
            self.open_workflow_action.setEnabled(not running)

    # ------------------------------------------------------------------
    # Workflow loading
    # ------------------------------------------------------------------

    def _workflow_path(self) -> str:
        preferred = os.path.join(self.base_dir, WORKFLOW_FILE)
        fallback = os.path.join(self.base_dir, FALLBACK_WORKFLOW_FILE)
        return preferred if os.path.exists(preferred) else fallback

    def load_workflow(self) -> None:
        if self.worker and self.worker.isRunning():
            self._log("Không thể reload workflow khi đang chạy.")
            return
        path = self._workflow_path()
        try:
            data = load_json(path, [])
            if isinstance(data, dict):
                data = [data]
            self.tasks = data if isinstance(data, list) else []
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi", f"Không thể đọc workflow:\n{ex}")
            self.tasks = []

        self.task_combo.blockSignals(True)
        self.task_combo.clear()
        for t in self.tasks:
            self.task_combo.addItem(t.get("label") or t.get("name", "Unnamed"))
        self.task_combo.blockSignals(False)

        if self.tasks:
            self.task_combo.setCurrentIndex(0)
            self._on_task_changed(0)

    def _open_workflow_file(self) -> None:
        if self.worker and self.worker.isRunning():
            self._log("Không thể đổi workflow khi đang chạy.")
            return
        path, _ = QFileDialog.getOpenFileName(
            self, "Chọn file workflow", self.base_dir, "JSON (*.json)"
        )
        if not path:
            return
        try:
            data = load_json(path, [])
            if isinstance(data, dict):
                data = [data]
            self.tasks = data
            self.task_combo.blockSignals(True)
            self.task_combo.clear()
            for t in self.tasks:
                self.task_combo.addItem(t.get("label") or t.get("name", "Unnamed"))
            self.task_combo.blockSignals(False)
            if self.tasks:
                self.task_combo.setCurrentIndex(0)
                self._on_task_changed(0)
            self._log(f"Loaded: {os.path.basename(path)}")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi", f"Không thể mở file:\n{ex}")

    # ------------------------------------------------------------------
    # Tab building
    # ------------------------------------------------------------------

    def _on_task_changed(self, index: int) -> None:
        if self.worker and self.worker.isRunning():
            self._log("Không thể đổi task khi workflow đang chạy.")
            self.task_combo.blockSignals(True)
            current_index = self.tasks.index(self.current_task) if self.current_task in self.tasks else 0
            self.task_combo.setCurrentIndex(current_index)
            self.task_combo.blockSignals(False)
            return
        if index < 0 or index >= len(self.tasks):
            self.current_task = None
            return
        self.current_task = self.tasks[index]
        self._build_tabs()

    def _build_tabs(self) -> None:
        self.tabs.clear()
        self.step_tables.clear()
        self.cache_checks.clear()

        if not self.current_task:
            return

        task_name = self.current_task.get("name", "task")

        for step in self.current_task.get("steps", []):
            step_type = step.get("type", "")
            tab_title = step.get("label") or step.get("name", "step")

            if step_type == "INPUT":
                tab_widget = build_input_tab(
                    step=step,
                    task_name=task_name,
                    base_dir=self.base_dir,
                    table_registry=self.step_tables,
                )
            else:
                tab_widget = build_output_tab(
                    step=step,
                    table_registry=self.step_tables,
                    cache_check_registry=self.cache_checks,
                    log_fn=self._log,
                )

            self.tabs.addTab(tab_widget, tab_title)

    # ------------------------------------------------------------------
    # Collect input data from INPUT tabs
    # ------------------------------------------------------------------

    def _gather_all_input(self) -> Dict[str, List[str]]:
        if not self.current_task:
            return {}
        merged: Dict[str, List[str]] = {}
        for step in self.current_task.get("steps", []):
            if step.get("type") == "INPUT":
                step_name = step.get("name", "input")
                table = self.step_tables.get(step_name)
                if isinstance(table, InputTableWidget):
                    data = collect_input_data(step, table)
                    merged.update(data)
        return merged

    # ------------------------------------------------------------------
    # Workflow execution
    # ------------------------------------------------------------------

    def run_workflow(self) -> None:
        if not self.current_task:
            return

        self._start_run_log()
        input_data = self._gather_all_input()
        self._log(
            f"Task={self.current_task.get('name', 'unknown')} steps={len(self.current_task.get('steps', []))}"
        )
        if input_data:
            input_keys = ", ".join(input_data.keys())
            self._log(f"Input keys: {input_keys}")
            for key, value in input_data.items():
                if isinstance(value, list):
                    sample = value[:5]
                    self._log(f"Input[{key}] count={len(value)} sample={sample}")

        self._log("Bắt đầu chạy workflow…")
        self._set_execution_state(True)

        self.worker = WorkflowWorker(self.current_task, input_data, self.base_dir)
        self.worker.log_signal.connect(self._log)
        self.worker.step_started.connect(self._on_step_started)
        self.worker.step_completed.connect(self._on_step_completed)
        self.worker.failed.connect(self._on_failed)
        self.worker.all_done.connect(self._on_all_done)
        self.worker.start()

    # ------------------------------------------------------------------
    # Worker signal handlers
    # ------------------------------------------------------------------

    def _on_step_started(self, step_name: str) -> None:
        self._log(f"Step bắt đầu: {step_name}")

    def _get_step_type(self, step_name: str) -> str:
        if not self.current_task:
            return ""
        for step in self.current_task.get("steps", []):
            if step.get("name") == step_name:
                return str(step.get("type", ""))
        return ""

    def _on_step_completed(
        self, step_name: str, data: Dict[str, Any], cache_used: bool
    ) -> None:
        step_type = self._get_step_type(step_name)
        self._log(f"Step hoàn thành: {step_name} [{step_type}]")
        table = self.step_tables.get(step_name)
        if table is None:
            return

        if not isinstance(data, dict):
            self._log(
                f"[DEBUG] step={step_name} result_type={type(data).__name__} (expected dict)"
            )
            rows_raw: Any = []
        else:
            keys_preview = ", ".join(list(data.keys())[:8])
            rows_raw = data.get("items", [])
            self._log(
                f"[DEBUG] step={step_name} keys=[{keys_preview}] items_type={type(rows_raw).__name__}"
            )

        # INPUT tab đã có bảng nhập riêng, không render lại như output table.
        if step_type == "INPUT":
            count = len(rows_raw) if isinstance(rows_raw, list) else 0
            self._log(f"[DEBUG] step={step_name} INPUT rows={count} (skip output render)")
            return

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

        if coerced_count:
            self._log(
                f"[DEBUG] step={step_name} coerced_rows={coerced_count} because non-dict row detected"
            )

        preview_type = type(rows[0]).__name__ if rows else "none"
        self._log(
            f"[DEBUG] step={step_name} rows_ready={len(rows)} first_row_type={preview_type}"
        )
        populate_output_table(table, rows)

        if step_name in self.cache_checks:
            self.cache_checks[step_name].setChecked(cache_used)

    def _on_failed(self, message: str) -> None:
        self._set_execution_state(False)
        self._log("Workflow thất bại.")
        self._log("=== RUN FAILED ===")
        QMessageBox.critical(self, "Lỗi Workflow", message)

    def _on_all_done(self, _: Dict[str, Any]) -> None:
        self._set_execution_state(False)
        self._log("Workflow hoàn thành.")
        self._log("=== RUN COMPLETED ===")
        if self.current_run_log_path:
            self._log(f"Log file: {self.current_run_log_path}")
        QMessageBox.information(self, "Xong", "Workflow đã chạy xong.")

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _start_run_log(self) -> None:
        logs_dir = os.path.join(self.base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        task_name = "task"
        if self.current_task:
            task_name = str(self.current_task.get("name", "task"))
        safe_task_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in task_name)
        self.current_run_log_path = os.path.join(logs_dir, f"run_{safe_task_name}_{ts}.log")

        with open(self.current_run_log_path, "w", encoding="utf-8") as f:
            f.write(f"Run started: {datetime.now().isoformat()}\n")

        self._log(f"=== RUN STARTED === {datetime.now().isoformat()}")

    def _log(self, text: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {text}"
        self.log_panel.append(line)
        print(line)
        if self.current_run_log_path:
            with open(self.current_run_log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
