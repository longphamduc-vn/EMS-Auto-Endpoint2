"""
main.py – Entry point của ứng dụng Nexacro Workflow Engine.

Chạy:
    python main.py
"""

import sys

from PyQt6.QtWidgets import QApplication

from ems_workflow.ui.main_window import WorkflowApp


def main() -> None:
    app = QApplication(sys.argv)
    window = WorkflowApp()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
