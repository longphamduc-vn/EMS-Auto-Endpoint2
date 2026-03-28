import os
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication, QTableWidget

from ems_workflow.engine import WorkflowWorker
from ems_workflow.ui.input_tab import InputTableWidget, fill_input_table
from ems_workflow.ui.output_tab import copy_table_to_clipboard, populate_output_table
from ems_workflow.utils import load_json, save_json


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

    def test_input_paste_skips_header_row_from_output_copy(self) -> None:
        table = InputTableWidget(5, 1)
        table.setHorizontalHeaderLabels(["Q code"])
        QApplication.clipboard().setText("Q code\nQ001\nQ002")

        table._paste_from_clipboard()

        self.assertEqual(table.item(0, 0).text(), "Q001")
        self.assertEqual(table.item(1, 0).text(), "Q002")

    def test_fill_input_table_accepts_scalar_values(self) -> None:
        step = {"fields": [{"name": "items", "label": "Item"}]}
        table = InputTableWidget(5, 1)

        fill_input_table(step, table, {"items": "Q001"})

        self.assertEqual(table.item(0, 0).text(), "Q001")

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

    def test_copy_table_to_clipboard_includes_headers(self) -> None:
        table = QTableWidget()
        populate_output_table(table, [{"item": "Q001", "qty": 3}])

        copied = copy_table_to_clipboard(table, include_headers=True, selection_only=False)

        self.assertTrue(copied)
        self.assertEqual(QApplication.clipboard().text(), "item\tqty\nQ001\t3")

    def test_populate_output_table_applies_row_and_column_styles(self) -> None:
        table = QTableWidget()

        populate_output_table(
            table,
            [{"item": "Q001", "status": "RED", "qty": 3}],
            style_rules={
                "rows": [
                    {
                        "when": {"field": "status", "operator": "eq", "value": "RED"},
                        "background": "#fff1f2",
                    }
                ],
                "columns": [
                    {
                        "field": "qty",
                        "foreground": "#1d4ed8",
                        "fontWeight": "bold",
                        "alignment": "right",
                    }
                ],
            },
        )

        status_item = table.item(0, 1)
        qty_item = table.item(0, 2)

        self.assertEqual(status_item.background().color().name(), "#fff1f2")
        self.assertEqual(qty_item.foreground().color().name(), "#1d4ed8")
        self.assertTrue(qty_item.font().bold())

    def test_run_transform_step_supports_complex_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            save_json(
                os.path.join(temp_dir, "accumulated_data.json"),
                {
                    "EstimateHistory": [
                        {"item": "Q001", "factory": "M1", "requestComment": "A"},
                        {"item": "Q001", "factory": "M1", "requestComment": "B"},
                        {"item": "Q002", "factory": "M2", "requestComment": "C"},
                    ]
                },
            )
            worker = WorkflowWorker({"name": "ItemStatus", "steps": []}, {}, temp_dir)
            context = {
                "StockDetail": {"item": ["Q001", "Q002"]},
                "PoStatusCheck": [
                    {"item": "Q001", "inventoryItemStatusCode": "ACTIVE"},
                    {"item": "Q002", "inventoryItemStatusCode": "HOLD"},
                ],
            }
            step = {
                "name": "ComplexSummary",
                "type": "DATA_TRANSFORM",
                "transforms": [
                    {
                        "type": "ACCUMULATE",
                        "mode": "load",
                        "source": "$.accumulated_data.ItemStatus.EstimateHistory[?(@.item in $.StockDetail[*].item )]",
                    },
                    {
                        "type": "MERGE",
                        "rightSource": "$.PoStatusCheck",
                        "joinKeys": [{"leftKey": "item", "rightKey": "item"}],
                        "leftPrefix": "hist.",
                        "rightPrefix": "po.",
                    },
                    {
                        "type": "DUPLICATE",
                        "field": "hist.factory",
                        "targetField": "factoryCopy",
                    },
                    {
                        "type": "GROUP BY",
                        "keys": ["hist.item", "po.inventoryItemStatusCode", "factoryCopy"],
                        "aggregations": [
                            {"field": "hist.requestComment", "op": "count", "target": "requestCount"}
                        ],
                    },
                    {
                        "type": "ACCUMULATE",
                        "target": "ComplexSummaryStore",
                    },
                    {
                        "type": "PIVOT",
                        "index": ["hist.item", "factoryCopy"],
                        "columnField": "po.inventoryItemStatusCode",
                        "valueField": "requestCount",
                        "aggregator": "sum",
                        "fillValue": 0,
                    },
                    {
                        "type": "SORT",
                        "keys": [{"field": "hist.item", "direction": "asc"}],
                    },
                ],
            }

            result = worker.run_transform_step(step, context)
            rows = worker._records_from_step_data(result)
            stored = load_json(os.path.join(temp_dir, "accumulated_data.json"), {})

            self.assertEqual(
                rows,
                [
                    {"hist.item": "Q001", "factoryCopy": "M1", "ACTIVE": 2, "HOLD": 0},
                    {"hist.item": "Q002", "factoryCopy": "M2", "ACTIVE": 0, "HOLD": 1},
                ],
            )
            self.assertEqual(len(stored["ComplexSummaryStore"]), 2)

    def test_is_no_data_response_not_limited_to_minus_one(self) -> None:
        worker = self.make_worker()

        self.assertTrue(worker._is_no_data_response(-1, "데이터 없음 -"))
        self.assertTrue(worker._is_no_data_response(999, "데이터 없음 -"))
        self.assertTrue(worker._is_no_data_response(12, "No data found"))

    def test_response_error_info_reads_common_variants(self) -> None:
        worker = self.make_worker()

        code, msg = worker._response_error_info(
            {"parameters": {"errorCode": "-5", "errorMsg": "some error"}}
        )

        self.assertEqual(code, -5)
        self.assertEqual(msg, "some error")

    def test_response_with_data_is_not_treated_as_no_data(self) -> None:
        worker = self.make_worker()
        parsed_response = {
            "parameters": {"errorCode": 1, "errorMsg": ""},
            "response": {"itemName": ["Q001"], "inventoryItemId": ["123"]},
        }

        response_has_data = worker._response_has_data(parsed_response)
        is_no_data = worker._is_no_data_response(1, "")

        self.assertTrue(response_has_data)
        self.assertFalse(is_no_data and not response_has_data)

    @staticmethod
    def _cell(value: str):
        from PyQt6.QtWidgets import QTableWidgetItem

        return QTableWidgetItem(value)


if __name__ == "__main__":
    unittest.main()