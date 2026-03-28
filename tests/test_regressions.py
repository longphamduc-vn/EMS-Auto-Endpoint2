import os
import tempfile
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication, QTableWidget

from ems_workflow.engine import WorkflowWorker
from ems_workflow.ui.input_tab import InputTableWidget, fill_input_table
from ems_workflow.ui.output_tab import populate_output_table


class WorkflowRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

    def make_worker(self) -> WorkflowWorker:
        return WorkflowWorker({"name": "test", "steps": []}, {}, tempfile.gettempdir())

    def test_resolve_value_returns_none_for_out_of_range_loop_item(self) -> None:
        worker = self.make_worker()
        context = {"values": {"items": ["A"]}}

        value = worker.resolve_value("$.values.items", context, 3)

        self.assertIsNone(value)

    def test_apply_extracts_supports_response_row_fallback(self) -> None:
        worker = self.make_worker()
        source = {"response": [{"inventoryItemId": "INV1001"}]}

        result = worker.apply_extracts(
            [
                {
                    "name": "inventoryItemId",
                    "type": "JSON_PATH",
                    "value": "$.response.inventoryItemId[*]",
                }
            ],
            source,
        )

        self.assertEqual(result["inventoryItemId"], ["INV1001"])
        self.assertEqual(result["items"], [{"inventoryItemId": "INV1001"}])

    def test_fill_input_table_clears_old_cells(self) -> None:
        step = {"fields": [{"name": "items", "label": "Item"}]}
        table = InputTableWidget(30, 1)
        table.setItem(0, 0, self._cell("OLD"))
        table.setItem(1, 0, self._cell("STALE"))

        fill_input_table(step, table, {"items": ["NEW"]})

        self.assertEqual(table.item(0, 0).text(), "NEW")
        self.assertIsNone(table.item(1, 0))

    def test_populate_output_table_uses_union_of_columns(self) -> None:
        table = QTableWidget()

        populate_output_table(table, [{"a": 1}, {"b": 2}])

        headers = [table.horizontalHeaderItem(i).text() for i in range(table.columnCount())]
        self.assertEqual(headers, ["a", "b"])
        self.assertEqual(table.item(0, 0).text(), "1")
        self.assertEqual(table.item(1, 1).text(), "2")

    def test_populate_output_table_prefers_label_headers(self) -> None:
        table = QTableWidget()

        populate_output_table(
            table,
            [{"inventoryItemId": "INV1001", "itemName": "Q001"}],
            {"inventoryItemId": "Item ID", "itemName": "Q Code"},
        )

        headers = [table.horizontalHeaderItem(i).text() for i in range(table.columnCount())]
        self.assertEqual(headers, ["Item ID", "Q Code"])

    @staticmethod
    def _cell(value: str):
        from PyQt6.QtWidgets import QTableWidgetItem

        return QTableWidgetItem(value)


if __name__ == "__main__":
    unittest.main()