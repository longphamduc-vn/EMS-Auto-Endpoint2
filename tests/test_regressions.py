import os
import tempfile
import unittest
from unittest.mock import patch

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

    def test_resolve_accumulated_display_filters_by_current_context_items(self) -> None:
        worker = WorkflowWorker(
            {"name": "ItemStatus", "steps": []},
            {},
            tempfile.gettempdir(),
        )
        step = {
            "name": "EstimateHistory",
            "accumulation": "$.accumulated_data.ItemStatus.EstimateHistory[?(@.item in $.StockDetail[*].item )]",
        }
        context = {
            "StockDetail": {
                "items": [
                    {"item": "QCAKA0171"},
                    {"item": "QDAA48980"},
                ]
            }
        }
        accumulated_rows = {
            "EstimateHistory": [
                {"item": "QCAKA0171", "value": 1},
                {"item": "QCAKA0104", "value": 2},
                {"item": "QDAA48980", "value": 3},
            ]
        }

        with patch("ems_workflow.engine.load_json", return_value=accumulated_rows):
            result = worker.resolve_accumulated_display(step, context, {"items": []})

        self.assertEqual(
            result["items"],
            [
                {"item": "QCAKA0171", "value": 1},
                {"item": "QDAA48980", "value": 3},
            ],
        )

    def test_parse_accumulation_expression_supports_filter_syntax(self) -> None:
        worker = self.make_worker()

        parsed = worker._parse_accumulation_expression(
            "$.accumulated_data.ItemStatus.EstimateHistory[?(@.item in $.StockDetail[*].item )]"
        )

        self.assertEqual(
            parsed,
            (
                "ItemStatus",
                "EstimateHistory",
                "item",
                "$.StockDetail[*].item",
            ),
        )

    def test_resolve_accumulation_context_values_supports_step_items_shorthand(self) -> None:
        worker = self.make_worker()
        context = {
            "StockDetail": {
                "items": [
                    {"item": "QCAKA0171"},
                    {"item": "QDAA48980"},
                ]
            }
        }

        values = worker._resolve_accumulation_context_values(
            context,
            "$.StockDetail[*].item",
        )

        self.assertEqual(values, ["QCAKA0171", "QDAA48980"])

    def test_resolve_accumulated_display_flattens_nested_filter_values(self) -> None:
        worker = WorkflowWorker(
            {"name": "ItemStatus", "steps": []},
            {},
            tempfile.gettempdir(),
        )
        step = {
            "name": "EstimateHistory",
            "accumulation": "$.accumulated_data.ItemStatus.EstimateHistory[?(@.item in $.StockDetail[*].item )]",
        }
        context = {
            "StockDetail": {
                "item": ["QCAKA0171", "QDAA48980"],
            }
        }
        accumulated_rows = {
            "EstimateHistory": [
                {"item": "QCAKA0171", "value": 1},
                {"item": "QCAKA0104", "value": 2},
                {"item": "QDAA48980", "value": 3},
            ]
        }

        with patch("ems_workflow.engine.load_json", return_value=accumulated_rows):
            result = worker.resolve_accumulated_display(step, context, {"items": []})

        self.assertEqual(
            result["items"],
            [
                {"item": "QCAKA0171", "value": 1},
                {"item": "QDAA48980", "value": 3},
            ],
        )

    def test_records_from_step_data_supports_columnar_dict_without_items(self) -> None:
        worker = self.make_worker()

        rows = worker._records_from_step_data(
            {
                "item": ["QCAKA0171", "QDAA48980"],
                "value": [1, 3],
            }
        )

        self.assertEqual(
            rows,
            [
                {"item": "QCAKA0171", "value": 1},
                {"item": "QDAA48980", "value": 3},
            ],
        )

    def test_resolve_accumulated_display_keeps_live_result_for_boolean_flag(self) -> None:
        worker = self.make_worker()
        live_result = {"items": [{"item": "LIVE"}]}

        result = worker.resolve_accumulated_display(
            {"name": "EstimateHistory", "accumulation": True},
            {},
            live_result,
        )

        self.assertIs(result, live_result)

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