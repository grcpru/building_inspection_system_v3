"""
Reports Module
==============
Centralized report generation for all user roles.

Available Reports:
- InspectorReport: Excel and Word inspection reports (excel_generator.py, word_generator.py)
- BuilderDefectReport: Defect management reports for builders (builder_report.py)
"""

# Inspector reports (existing)
try:
    from reports.excel_generator import *
    from reports.word_generator import *
except ImportError:
    pass  # These modules may not have exports

# Builder reports (new)
from reports.builder_report import BuilderReportGenerator, add_builder_report_ui

__all__ = [
    'BuilderReportGenerator',
    'add_builder_report_ui'
]

__version__ = '1.0.0'