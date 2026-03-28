"""
constants.py – Hằng số dùng chung toàn ứng dụng.
"""

import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

WORKFLOW_FILE = "workflow.json"
FALLBACK_WORKFLOW_FILE = "sample.json"
SAVED_FILTERS_FILE = "saved_filters.json"
ACCUMULATED_FILE = "accumulated_data.json"
CACHE_DIR = ".cache"
NEXACRO_NS = "http://www.nexacroplatform.com/platform/dataset"

# Thư mục gốc tự động xuất file Excel
AUTO_EXPORT_BASE_DIR = r"D:\ems"
