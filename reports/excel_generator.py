"""
Professional Excel Report Generator - Fixed Version
==================================================

Based on the working excel_report_generator.py from streamlit_app.py
This generates Excel reports with the exact format and logic that works.
"""

import pandas as pd
from datetime import datetime
import pytz
from io import BytesIO
import xlsxwriter
import logging

# Set up logging
logger = logging.getLogger(__name__)


def generate_professional_excel_report(final_df: pd.DataFrame, metrics: dict) -> BytesIO:
    """
    Generate professional Excel report using exact logic from working excel_report_generator.py
    
    Args:
        final_df: Processed inspection DataFrame
        metrics: Dictionary containing calculated metrics
        
    Returns:
        BytesIO: Excel file buffer
    """
    logger.info("Generating Excel report with working logic")
    
    # Add component summary to metrics (exact logic from working code)
    add_component_summary_to_metrics(final_df, metrics)
    
    # Create BytesIO buffer
    excel_buffer = BytesIO()

    # Create workbook with xlsxwriter for better formatting
    workbook = xlsxwriter.Workbook(excel_buffer, {
    'nan_inf_to_errors': True,
    'remove_timezone': True
    })

    # === Core table formats (exact from working code) ===
    table_header = workbook.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#1F4E78',
        'font_color': 'white',
        'border': 1
    })

    cell_format = workbook.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': False,
        'font_size': 10
    })

    alt_row_format = workbook.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': False,
        'font_size': 10,
        'bg_color': '#F7F9FC'   # zebra shade
    })

    # === Date formats (exact from working code) ===
    date_cell_format = workbook.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': False,
        'font_size': 10,
        'num_format': 'yyyy-mm-dd'
    })

    date_alt_row_format = workbook.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': False,
        'font_size': 10,
        'bg_color': '#F7F9FC',
        'num_format': 'yyyy-mm-dd'
    })

    # ======= Other visual formats (exact from working code) =======
    title_format = workbook.add_format({
        'bold': True,
        'font_size': 18,
        'bg_color': '#4CAF50',
        'font_color': 'white',
        'align': 'center',
        'valign': 'vcenter',
        'border': 2,
        'border_color': '#2E7D32'
    })

    building_header = workbook.add_format({
        'bold': True,
        'font_size': 14,
        'bg_color': '#2196F3',
        'font_color': 'white',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    section_header = workbook.add_format({
        'bold': True,
        'font_size': 12,
        'bg_color': '#FF9800',
        'font_color': 'white',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    label_format = workbook.add_format({
        'bold': True,
        'font_size': 11,
        'bg_color': '#F5F5F5',
        'border': 1,
        'align': 'left',
        'valign': 'vcenter'
    })

    data_format = workbook.add_format({
        'font_size': 11,
        'border': 1,
        'align': 'right',
        'valign': 'vcenter'
    })

    # Special format for Development Quality Score
    quality_score_format = workbook.add_format({
        'font_size': 11,
        'border': 1,
        'align': 'right',
        'valign': 'vcenter',
        'bg_color': '#C8E6C9',
        'font_color': '#2E7D32',
        'bold': True
    })

    # Settlement readiness formats with color coding (exact from working code)
    ready_format = workbook.add_format({
        'font_size': 11,
        'border': 1,
        'align': 'right',
        'valign': 'vcenter',
        'bg_color': '#C8E6C9',
        'font_color': '#2E7D32'
    })

    minor_format = workbook.add_format({
        'font_size': 11,
        'border': 1,
        'align': 'right',
        'valign': 'vcenter',
        'bg_color': '#FFF3C4',
        'font_color': '#F57F17'
    })

    major_format = workbook.add_format({
        'font_size': 11,
        'border': 1,
        'align': 'right',
        'valign': 'vcenter',
        'bg_color': '#FFCDD2',
        'font_color': '#C62828'
    })

    extensive_format = workbook.add_format({
        'font_size': 11,
        'border': 1,
        'align': 'right',
        'valign': 'vcenter',
        'bg_color': '#F8BBD9',
        'font_color': '#AD1457'
    })

    # Header style used for summary sheets (exact from working code)
    table_header_dark = workbook.add_format({
        'bold': True,
        'font_size': 10,
        'bg_color': '#37474F',
        'font_color': 'white',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': True
    })

    # ===== EXECUTIVE DASHBOARD SHEET (exact from working code) =====
    worksheet = workbook.add_worksheet("📊 Executive Dashboard")
    worksheet.set_column('A:A', 35)
    worksheet.set_column('B:B', 45)

    current_row = 0

    # Main Title
    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        f'🏢 {metrics["building_name"].upper()} - INSPECTION REPORT',
        title_format
    )
    worksheet.set_row(current_row, 30)
    current_row += 2

    # Building Information Section (exact from working code)
    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        '🏢 BUILDING INFORMATION',
        building_header
    )
    worksheet.set_row(current_row, 25)
    current_row += 2

    # Determine inspection date display (conditional: range for multi-day, single for one-day)
    if metrics.get('is_multi_day_inspection', False):
        inspection_date_display = metrics.get('inspection_date_range', metrics['inspection_date'])
    else:
        inspection_date_display = metrics['inspection_date']

    building_data = [
        ('Building Name', metrics['building_name']),
        ('Address', metrics['address']),
        ('Inspection Date', inspection_date_display),  # Now shows range if multi-day
        ('Total Units Inspected', f"{metrics['total_units']:,}"),
        ('Unit Types', metrics['unit_types_str'])
    ]

    for label, value in building_data:
        worksheet.write(current_row, 0, label, label_format)
        worksheet.write(current_row, 1, value, data_format)
        current_row += 1

    current_row += 1

    # Inspection Summary Section (WITH QUALITY SCORE - exact from working code)
    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        '📋 INSPECTION SUMMARY',
        section_header
    )
    worksheet.set_row(current_row, 25)
    current_row += 2

    # Calculate Development Quality Score (exact formula from working code)
    defect_rate = metrics.get('defect_rate', 0)
    quality_score = max(0, 100 - defect_rate)

    inspection_data = [
        ('Total Inspection Points', f"{metrics['total_inspections']:,}", data_format),
        ('Total Defects Found', f"{metrics['total_defects']:,}", data_format),
        ('Overall Defect Rate', f"{metrics['defect_rate']:.2f}%", data_format),
        ('Average Defects per Unit', f"{metrics['avg_defects_per_unit']:.1f}", data_format),
        ('Development Quality Score', f"{quality_score:.1f}/100", quality_score_format)
    ]

    for label, value, fmt in inspection_data:
        worksheet.write(current_row, 0, label, label_format)
        worksheet.write(current_row, 1, value, fmt)
        current_row += 1

    current_row += 1

    # Settlement Readiness Section (exact from working code)
    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        '🏠 SETTLEMENT READINESS ANALYSIS',
        section_header
    )
    worksheet.set_row(current_row, 25)
    current_row += 2

    readiness_data = [
        ('✅ Minor Work Required (0-2 defects)',
         f"{metrics['ready_units']} units ({metrics['ready_pct']:.1f}%)", ready_format),
        ('⚠️ Intermediate Remediation Required (3-7 defects)',
         f"{metrics['minor_work_units']} units ({metrics['minor_pct']:.1f}%)", minor_format),
        ('🔧 Major Work Required (8-15 defects)',
         f"{metrics['major_work_units']} units ({metrics['major_pct']:.1f}%)", major_format),
        ('🚧 Extensive Work Required (15+ defects)',
         f"{metrics['extensive_work_units']} units ({metrics['extensive_pct']:.1f}%)", extensive_format)
    ]

    for label, value, fmt in readiness_data:
        worksheet.write(current_row, 0, label, label_format)
        worksheet.write(current_row, 1, value, fmt)
        current_row += 1

    current_row += 1

    # Quality Score Analysis Section (exact from working code)
    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        '🎯 QUALITY SCORE ANALYSIS',
        section_header
    )
    worksheet.set_row(current_row, 25)
    current_row += 2

    # Quality score interpretation (exact from working code)
    quality_interpretation = get_quality_score_interpretation(quality_score)
    quality_analysis_data = [
        ('Component Pass Rate', f"{quality_score:.1f}%", quality_score_format),
        ('Quality Grade', quality_interpretation['grade'], data_format),
        ('Industry Benchmark', quality_interpretation['benchmark'], data_format),
        ('Recommended Action', quality_interpretation['action'], data_format)
    ]

    for label, value, fmt in quality_analysis_data:
        worksheet.write(current_row, 0, label, label_format)
        worksheet.write(current_row, 1, value, fmt)
        current_row += 1

    current_row += 1

    # Top Problem Trades Section (exact from working code)
    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        '⚠️ TOP PROBLEM TRADES',
        section_header
    )
    worksheet.set_row(current_row, 25)
    current_row += 2

    if isinstance(metrics.get('summary_trade'), pd.DataFrame) and len(metrics['summary_trade']) > 0:
        top_trades = metrics['summary_trade'].head(10)
        for idx, (_, row) in enumerate(top_trades.iterrows(), 1):
            trade_label = f"{idx}. {row['Trade']}"
            defect_count = f"{row['DefectCount']} defects"
            worksheet.write(current_row, 0, trade_label, label_format)
            worksheet.write(current_row, 1, defect_count, data_format)
            current_row += 1
    else:
        worksheet.write(current_row, 0, "No defects found", label_format)
        worksheet.write(current_row, 1, "All trades passed inspection", data_format)
        current_row += 1

    current_row += 2

    # Footer (exact from working code)
    melbourne_tz = pytz.timezone('Australia/Melbourne')
    melbourne_time = datetime.now(melbourne_tz)
    report_time = melbourne_time.strftime('%d/%m/%Y at %I:%M %p AEDT')

    worksheet.merge_range(
        f'A{current_row + 1}:B{current_row + 1}',
        f'Report generated on {report_time} | Professional Inspection Report Processor v2.0',
        workbook.add_format({'font_size': 9, 'italic': True, 'align': 'center', 'font_color': '#666666'})
    )

    # ===== RAW INSPECTION DATA SHEET (exact from working code) =====
    create_data_sheet(
        workbook, final_df, "📋 All Inspections",
        table_header, cell_format, alt_row_format,
        date_cell_format, date_alt_row_format
    )

    # ===== DEFECTS ONLY SHEET (exact from working code) =====
    if 'StatusClass' in final_df.columns:
        defects_only = final_df[final_df['StatusClass'] == 'Not OK']
        if len(defects_only) > 0:
            create_data_sheet(
                workbook, defects_only, "🚨 Defects Only",
                table_header, cell_format, alt_row_format,
                date_cell_format, date_alt_row_format
            )

    # ===== SETTLEMENT READINESS SHEET (exact from working code) =====
    create_settlement_sheet(
        workbook, metrics, table_header_dark, cell_format,
        ready_format, minor_format, major_format, extensive_format
    )

    # ===== TRADE SUMMARY SHEET (exact from working code) =====
    if isinstance(metrics.get('summary_trade'), pd.DataFrame) and len(metrics['summary_trade']) > 0:
        create_data_sheet(
            workbook, metrics['summary_trade'], "🔧 Trade Summary",
            table_header_dark, cell_format, alt_row_format,
            date_cell_format, date_alt_row_format
        )

    # ===== ROOM SUMMARY SHEET (exact from working code) =====
    if isinstance(metrics.get('summary_room'), pd.DataFrame) and len(metrics['summary_room']) > 0:
        create_data_sheet(
            workbook, metrics['summary_room'], "🚪 Room Summary",
            table_header_dark, cell_format, alt_row_format,
            date_cell_format, date_alt_row_format
        )

    # ===== COMPONENT SUMMARY SHEET (exact from working code) =====
    if isinstance(metrics.get('summary_component'), pd.DataFrame) and len(metrics['summary_component']) > 0:
        create_data_sheet(
            workbook, metrics['summary_component'], "🔧 Component Summary",
            table_header_dark, cell_format, alt_row_format,
            date_cell_format, date_alt_row_format
        )

    # ===== UNIT SUMMARY SHEET (exact from working code) =====
    if isinstance(metrics.get('summary_unit'), pd.DataFrame) and len(metrics['summary_unit']) > 0:
        create_data_sheet(
            workbook, metrics['summary_unit'], "🏠 Unit Summary",
            table_header_dark, cell_format, alt_row_format,
            date_cell_format, date_alt_row_format
        )

    # ===== COMPONENT DETAILS SHEET (exact from working code) =====
    if isinstance(metrics.get('component_details_summary'), pd.DataFrame) and len(metrics['component_details_summary']) > 0:
        create_data_sheet(
            workbook, metrics['component_details_summary'], "📝 Component Details",
            table_header_dark, cell_format, alt_row_format,
            date_cell_format, date_alt_row_format
        )
        
    # ===== INSPECTION TIMELINE SHEET (NEW) =====
    create_inspection_timeline_sheet(
        workbook, final_df, metrics,
        table_header_dark, cell_format, alt_row_format,
        date_cell_format, date_alt_row_format
    )
    
    # ===== METADATA SHEET (exact from working code) =====
    create_metadata_sheet(workbook, metrics, table_header_dark, cell_format)

    # Close workbook and return buffer
    workbook.close()
    excel_buffer.seek(0)
    
    logger.info("Excel report generated successfully")
    return excel_buffer


def add_component_summary_to_metrics(final_df: pd.DataFrame, metrics: dict):
    """Add component summary to metrics dictionary (exact from working code)"""
    try:
        component_summary = generate_component_summary(final_df)
        if len(component_summary) > 0:
            metrics['summary_component'] = component_summary
            logger.info("Added component summary to metrics")
        else:
            metrics['summary_component'] = pd.DataFrame()
    except Exception as e:
        logger.error(f"Error adding component summary to metrics: {e}")
        metrics['summary_component'] = pd.DataFrame()


def generate_component_summary(final_df: pd.DataFrame) -> pd.DataFrame:
    """Generate simple component summary (exact from working code)"""
    try:
        required_columns = ['StatusClass', 'Component']
        missing_columns = [col for col in required_columns if col not in final_df.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns for component summary: {missing_columns}")
            return pd.DataFrame()
        
        # Filter for defects only
        defects_only = final_df[final_df['StatusClass'] == 'Not OK']
        
        if len(defects_only) == 0:
            logger.info("No defects found for component summary")
            return pd.DataFrame()
        
        # Simple groupby - count defects per component
        component_summary = defects_only.groupby('Component').size().reset_index(name='DefectCount')
        
        # Sort by defect count (descending)
        component_summary = component_summary.sort_values('DefectCount', ascending=False)
        
        logger.info(f"Generated component summary with {len(component_summary)} components")
        return component_summary
        
    except Exception as e:
        logger.error(f"Error generating component summary data: {e}")
        return pd.DataFrame()


def get_quality_score_interpretation(quality_score: float) -> dict:
    """Interpret quality score and provide context (exact from working code)"""
    if quality_score >= 98:
        return {'grade': 'Excellent (A+)', 'benchmark': 'Above Industry Standard', 'action': 'Maintain current standards'}
    elif quality_score >= 95:
        return {'grade': 'Very Good (A)', 'benchmark': 'Industry Leading', 'action': 'Minor quality improvements'}
    elif quality_score >= 90:
        return {'grade': 'Good (B+)', 'benchmark': 'Above Average', 'action': 'Targeted improvements'}
    elif quality_score >= 85:
        return {'grade': 'Satisfactory (B)', 'benchmark': 'Industry Average', 'action': 'Quality enhancement needed'}
    elif quality_score >= 75:
        return {'grade': 'Below Average (C)', 'benchmark': 'Below Industry Standard', 'action': 'Significant improvements required'}
    else:
        return {'grade': 'Poor (D)', 'benchmark': 'Well Below Standard', 'action': 'Comprehensive quality overhaul'}


def create_data_sheet(workbook, data_df, sheet_name: str, header_format, cell_format, alt_row_format,
                     date_cell_format, date_alt_row_format):
    """Create a data sheet with proper date cells (exact from working code)"""
    import pandas as pd
    from datetime import datetime as _dt
    
    # ADD DATETIME FORMATS HERE
    datetime_cell_format = workbook.add_format({
        'num_format': 'yyyy-mm-dd hh:mm',
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': False,
        'font_size': 10
    })
    
    datetime_alt_row_format = workbook.add_format({
        'num_format': 'yyyy-mm-dd hh:mm',
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'text_wrap': False,
        'font_size': 10,
        'bg_color': '#F7F9FC'
    })

    # Helper functions (exact from working code)
    def _is_date_col(col_name: str, series: pd.Series) -> bool:
        col_lower = col_name.lower().replace("_", "")
        name_hit = (
            ("date" in col_lower) or 
            ("plannedcompletion" in col_lower) or
            ("timestamp" in col_lower) or          # ADD THIS
            ("signoff" in col_lower)                # ADD THIS
        )
        dtype_hit = pd.api.types.is_datetime64_any_dtype(series)
        return name_hit or dtype_hit

    def _normalize(series: pd.Series) -> pd.Series:
        if series is None or len(series) == 0:
            return pd.Series(dtype="datetime64[ns]")
        
        s = series.copy()
        
        # Convert to datetime
        dt = pd.to_datetime(s, errors="coerce")
        
        # Handle Excel serial numbers
        num = pd.to_numeric(s, errors="coerce")
        maybe_excel = dt.isna() & num.notna() & num.between(20000, 60000)
        
        if maybe_excel.any():
            dt.loc[maybe_excel] = pd.to_datetime(num.loc[maybe_excel], unit="D", origin="1899-12-30", errors="coerce")
        
        return dt

    def _coerce_cell(value):
        # Handle None and NaT first
        if value is None or pd.isna(value):
            return None
        
        # Handle pandas Timestamp
        if hasattr(value, "to_pydatetime"):
            try:
                return value.to_pydatetime()
            except Exception:
                return None
        
        # Handle Python datetime
        if isinstance(value, _dt):
            return value
        
        # Try parsing as datetime string
        try:
            dt_try = pd.to_datetime(value, errors="coerce")
            if pd.notna(dt_try):
                try:
                    return dt_try.to_pydatetime()
                except Exception:
                    return None
        except Exception:
            pass
        
        # Try Excel serial number
        try:
            num = float(value)
            if 20000 <= num <= 60000:
                dt_try = pd.to_datetime(num, unit="D", origin="1899-12-30")
                if pd.notna(dt_try):
                    return dt_try.to_pydatetime()
        except Exception:
            pass
        
        return None

    # Work on a copy
    df = data_df.copy()
    for c in df.columns:
        if _is_date_col(str(c), df[c]):
            df[c] = _normalize(df[c])

    # Make sheet
    ws = workbook.add_worksheet(sheet_name)

    # Column widths
    for col_idx, col in enumerate(df.columns):
        width = min(max(len(str(col)), (df[col].astype(str).map(len).max() if len(df) else 0)) + 2, 50)
        ws.set_column(col_idx, col_idx, width)

    # Header row
    for col_idx, value in enumerate(df.columns):
        ws.write(0, col_idx, value, header_format)

    # Body with alternating shading
    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        is_alt = (row_idx % 2 == 0)
        base_fmt = alt_row_format if is_alt else cell_format
        base_date_fmt = date_alt_row_format if is_alt else date_cell_format
        base_datetime_fmt = datetime_alt_row_format if is_alt else datetime_cell_format  # ADD THIS

        for col_idx, col_name in enumerate(df.columns):
            val = row[col_name]
            if _is_date_col(str(col_name), df[col_name]):
                dt = _coerce_cell(val)
                if dt is not None:
                    # Check if timestamp column
                    col_lower = str(col_name).lower()
                    is_timestamp = 'timestamp' in col_lower or 'signoff' in col_lower
                    
                    # Use pre-defined formats (no dynamic format creation)
                    if is_timestamp:
                        ws.write_datetime(row_idx, col_idx, dt, base_datetime_fmt)
                    else:
                        ws.write_datetime(row_idx, col_idx, dt, base_date_fmt)
                else:
                    ws.write_blank(row_idx, col_idx, None, base_fmt)
            else:
                ws.write(row_idx, col_idx, "" if pd.isna(val) else val, base_fmt)

def create_inspection_timeline_sheet(workbook, processed_data: pd.DataFrame, metrics: dict,
                                    header_format, cell_format, alt_row_format,
                                    date_cell_format, date_alt_row_format):
    """
    Create Inspection Timeline sheet showing dates, defects, and sign-offs
    """
    import pandas as pd
    from datetime import datetime as _dt
    
    ws = workbook.add_worksheet("📅 Inspection Timeline")
    
    # Prepare data - defects only
    defects = processed_data[processed_data['StatusClass'] == 'Not OK'].copy()
    
    if len(defects) == 0:
        # Empty sheet with headers
        headers = ['Inspection Date', 'Unit', 'Unit Type', 'Room/Area', 'Component',
                  'Trade', 'Status', 'Urgency', 'Owner Sign-Off', 'Days Since', 'Signed?']
        for col_idx, header in enumerate(headers):
            ws.write(0, col_idx, header, header_format)
        return
    
    # Calculate days since inspection
    defects['InspectionDate'] = pd.to_datetime(defects['InspectionDate'], errors='coerce')
    
    # CRITICAL FIX: Remove timezone if present
    if hasattr(defects['InspectionDate'].dtype, 'tz') and defects['InspectionDate'].dt.tz is not None:
        defects['InspectionDate'] = defects['InspectionDate'].dt.tz_localize(None)
    
    today = _dt.now()
    defects['DaysSince'] = (today - defects['InspectionDate']).dt.days
    
    # Sign-off status
    defects['SignedStatus'] = defects['OwnerSignoffTimestamp'].apply(
        lambda x: 'Yes' if pd.notna(x) else 'Pending'
    )
    
    # Parse sign-off timestamps and remove timezone
    defects['OwnerSignoffTimestamp'] = pd.to_datetime(
        defects['OwnerSignoffTimestamp'], errors='coerce'
    )
    
    # CRITICAL FIX: Remove timezone from sign-off timestamps
    if hasattr(defects['OwnerSignoffTimestamp'].dtype, 'tz') and defects['OwnerSignoffTimestamp'].dt.tz is not None:
        defects['OwnerSignoffTimestamp'] = defects['OwnerSignoffTimestamp'].dt.tz_localize(None)
    
    # Sort by inspection date, then unit
    defects = defects.sort_values(['InspectionDate', 'Unit'])
    
    # Column widths
    ws.set_column('A:A', 15)
    ws.set_column('B:B', 12)
    ws.set_column('C:C', 12)
    ws.set_column('D:D', 20)
    ws.set_column('E:E', 25)
    ws.set_column('F:F', 25)
    ws.set_column('G:G', 10)
    ws.set_column('H:H', 15)
    ws.set_column('I:I', 18)
    ws.set_column('J:J', 12)
    ws.set_column('K:K', 12)
    
    # Headers
    headers = ['Inspection Date', 'Unit', 'Unit Type', 'Room/Area', 'Component',
              'Trade', 'Status', 'Urgency', 'Owner Sign-Off', 'Days Since', 'Signed?']
    for col_idx, header in enumerate(headers):
        ws.write(0, col_idx, header, header_format)
    
    # Conditional formats
    urgent_format = workbook.add_format({
        'bg_color': '#E74C3C', 'font_color': 'white',
        'align': 'center', 'border': 1
    })
    high_priority_format = workbook.add_format({
        'bg_color': '#F39C12', 'font_color': 'white',
        'align': 'center', 'border': 1
    })
    signed_format = workbook.add_format({
        'bg_color': '#27AE60', 'font_color': 'white',
        'align': 'center', 'border': 1
    })
    pending_format = workbook.add_format({
        'bg_color': '#E67E22', 'font_color': 'white',
        'align': 'center', 'border': 1
    })
    datetime_format = workbook.add_format({
        'num_format': 'yyyy-mm-dd hh:mm',
        'align': 'center', 'border': 1
    })
    datetime_alt_format = workbook.add_format({
        'num_format': 'yyyy-mm-dd hh:mm',
        'align': 'center', 'border': 1,
        'bg_color': '#F7F9FC'
    })
    
    # Write data rows
    for row_idx, (_, row) in enumerate(defects.iterrows(), start=1):
        is_alt = (row_idx % 2 == 0)
        base_fmt = alt_row_format if is_alt else cell_format
        base_date_fmt = date_alt_row_format if is_alt else date_cell_format
        base_datetime_fmt = datetime_alt_format if is_alt else datetime_format
        
        # Inspection Date
        insp_date = row['InspectionDate']
        if pd.notna(insp_date):
            # Convert to Python datetime (no timezone)
            dt = insp_date.to_pydatetime()
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            ws.write_datetime(row_idx, 0, dt, base_date_fmt)
        else:
            ws.write_blank(row_idx, 0, None, base_fmt)
        
        # Unit, Unit Type, Room, Component, Trade, Status
        ws.write(row_idx, 1, row.get('Unit', ''), base_fmt)
        ws.write(row_idx, 2, row.get('UnitType', ''), base_fmt)
        ws.write(row_idx, 3, row.get('Room', ''), base_fmt)
        ws.write(row_idx, 4, row.get('Component', ''), base_fmt)
        ws.write(row_idx, 5, row.get('Trade', ''), base_fmt)
        ws.write(row_idx, 6, row.get('StatusClass', ''), base_fmt)
        
        # Urgency with color coding
        urgency = row.get('Urgency', 'Normal')
        if urgency == 'Urgent':
            ws.write(row_idx, 7, urgency, urgent_format)
        elif urgency == 'High Priority':
            ws.write(row_idx, 7, urgency, high_priority_format)
        else:
            ws.write(row_idx, 7, urgency, base_fmt)
        
        # Owner Sign-Off datetime
        signoff = row['OwnerSignoffTimestamp']
        if pd.notna(signoff):
            # Convert to Python datetime (no timezone)
            dt = signoff.to_pydatetime()
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            ws.write_datetime(row_idx, 8, dt, base_datetime_fmt)
        else:
            ws.write_blank(row_idx, 8, None, base_fmt)
        
        # Days Since
        days_since = row.get('DaysSince', '')
        ws.write(row_idx, 9, days_since if pd.notna(days_since) else '', base_fmt)
        
        # Signed Status with color coding
        signed_status = row.get('SignedStatus', 'Pending')
        if signed_status == 'Yes':
            ws.write(row_idx, 10, signed_status, signed_format)
        else:
            ws.write(row_idx, 10, signed_status, pending_format)
    
    # Freeze header row
    ws.freeze_panes(1, 0)
    
    # Add auto-filter
    if len(defects) > 0:
        ws.autofilter(0, 0, len(defects), 10)

def create_settlement_sheet(workbook, metrics, header_format, cell_format,
                           ready_format, minor_format, major_format, extensive_format):
    """Create settlement readiness analysis sheet (exact from working code)"""
    ws = workbook.add_worksheet("🏠 Settlement Readiness")
    ws.set_column('A:A', 25)
    ws.set_column('B:B', 15)
    ws.set_column('C:C', 15)
    ws.set_column('D:D', 20)

    # Headers
    headers = ['Category', 'Units', 'Percentage', 'Criteria']
    for col_num, header in enumerate(headers):
        ws.write(0, col_num, header, header_format)

    # Data with color coding (exact from working code)
    settlement_data = [
        ('✅ Minor Work Required', metrics['ready_units'], f"{metrics['ready_pct']:.1f}%", '0-2 defects', ready_format),
        ('⚠️ Intermediate Remediation Required', metrics['minor_work_units'], f"{metrics['minor_pct']:.1f}%", '3-7 defects', minor_format),
        ('🔧 Major Work Required', metrics['major_work_units'], f"{metrics['major_pct']:.1f}%", '8-15 defects', major_format),
        ('🚧 Extensive Work Required', metrics['extensive_work_units'], f"{metrics['extensive_pct']:.1f}%", '15+ defects', extensive_format)
    ]

    for row_num, (category, units, percentage, criteria, fmt) in enumerate(settlement_data, 1):
        ws.write(row_num, 0, category, fmt)
        ws.write(row_num, 1, units, fmt)
        ws.write(row_num, 2, percentage, fmt)
        ws.write(row_num, 3, criteria, fmt)


def create_metadata_sheet(workbook, metrics, header_format, cell_format):
    """Create report metadata sheet (exact from working code)"""
    ws = workbook.add_worksheet("📄 Report Metadata")
    ws.set_column('A:A', 25)
    ws.set_column('B:B', 40)

    melbourne_tz = pytz.timezone('Australia/Melbourne')
    melbourne_time = datetime.now(melbourne_tz)

    # Calculate quality score for metadata (exact from working code)
    defect_rate = metrics.get('defect_rate', 0)
    quality_score = max(0, 100 - defect_rate)
    quality_interpretation = get_quality_score_interpretation(quality_score)

    metadata = [
        ('Report Generated', melbourne_time.strftime('%Y-%m-%d %H:%M:%S AEDT')),
        ('Report Version', '2.0 Professional'),
        ('Building Name', metrics['building_name']),
        ('Total Units', str(metrics['total_units'])),
        ('Total Defects', str(metrics['total_defects'])),
        ('Development Quality Score', f"{quality_score:.1f}/100"),
        ('Quality Grade', quality_interpretation['grade']),
        ('Industry Benchmark', quality_interpretation['benchmark']),
        ('Data Source', 'iAuditor CSV Export'),
        ('Processing Engine', 'Professional Inspection Report Processor'),
        ('Charts Included', 'Yes'),
        ('Raw Data Included', 'Yes'),
        ('Component Summary Included', 'Yes' if len(metrics.get('summary_component', pd.DataFrame())) > 0 else 'No')
    ]

    # Headers
    ws.write(0, 0, 'Property', header_format)
    ws.write(0, 1, 'Value', header_format)

    # Data
    for row_num, (prop, value) in enumerate(metadata, 1):
        ws.write(row_num, 0, prop, cell_format)
        ws.write(row_num, 1, value, cell_format)


def generate_filename(building_name: str, report_type: str = "Excel") -> str:
    """Generate professional filename with building name first (exact from working code)"""
    clean_building_name = "".join(c for c in building_name if c.isalnum() or c in (' ', '-', '_')).strip()
    clean_building_name = clean_building_name.replace(' ', '_')

    melbourne_tz = pytz.timezone('Australia/Melbourne')
    timestamp = datetime.now(melbourne_tz).strftime("%Y%m%d_%H%M%S")

    filename = f"{clean_building_name}_Inspection_Report_{report_type}_{timestamp}"
    return filename


def test_excel_generator():
    """Test function to verify Excel generator is working (exact from working code)"""
    try:
        # Create sample data for testing
        sample_data = pd.DataFrame({
            'Unit': ['Unit_1', 'Unit_2', 'Unit_1'],
            'UnitType': ['Apartment', 'Apartment', 'Apartment'],
            'Room': ['Bathroom', 'Kitchen Area', 'Kitchen Area'],
            'Component': ['Toilet', 'Kitchen Sink', 'Stovetop and Oven'],
            'StatusClass': ['OK', 'Not OK', 'Not OK'],
            'Trade': ['Plumbing', 'Plumbing', 'Appliances'],
            'PlannedCompletion': [45903, '2025-10-09', 46000]
        })

        sample_metrics = {
            'building_name': 'Test Building',
            'address': 'Test Address',
            'inspection_date': '2025-01-01',
            'unit_types_str': 'Apartment',
            'total_units': 2,
            'total_inspections': 3,
            'total_defects': 2,
            'defect_rate': 66.67,
            'avg_defects_per_unit': 1.0,
            'ready_units': 1,
            'minor_work_units': 1,
            'major_work_units': 0,
            'extensive_work_units': 0,
            'ready_pct': 50.0,
            'minor_pct': 50.0,
            'major_pct': 0.0,
            'extensive_pct': 0.0,
            'summary_trade': pd.DataFrame({'Trade': ['Plumbing', 'Appliances'], 'DefectCount': [1, 1]}),
            'summary_unit': pd.DataFrame({'Unit': ['Unit_2', 'Unit_1'], 'DefectCount': [1, 1]}),
            'summary_room': pd.DataFrame({'Room': ['Kitchen Area', 'Bathroom'], 'DefectCount': [2, 0]}),
            'component_details_summary': pd.DataFrame({
                'Trade': ['Plumbing', 'Appliances'],
                'Room': ['Kitchen Area', 'Kitchen Area'],
                'Component': ['Kitchen Sink', 'Stovetop and Oven'],
                'Units with Defects': ['Unit_2', 'Unit_1']
            })
        }

        # Generate Excel
        excel_buffer = generate_professional_excel_report(sample_data, sample_metrics)

        # Test quality score calculation
        quality_score = max(0, 100 - sample_metrics['defect_rate'])

        # Test filename generation
        filename = generate_filename("Test Building", "Excel")

        # Test component summary generation
        component_summary = generate_component_summary(sample_data)
        
        return True, f"Excel generator test successful. Quality Score: {quality_score:.1f}/100, Components: {len(component_summary)}, Filename: {filename}.xlsx"

    except Exception as e:
        return False, f"Excel generator test failed: {str(e)}"


if __name__ == "__main__":
    print("Professional Excel Report Generator - Fixed Version")
    print("Based on working excel_report_generator.py logic")
    
    success, message = test_excel_generator()
    print(f"Test Result: {message}")

    print("\n✅ FIXED EXCEL REPORT FEATURES:")
    print("• Development Quality Score: Component Pass Rate calculation")
    print("• Quality Score Analysis section with grade interpretation")
    print("• Special formatting for quality metrics (green highlighting)")
    print("• Component Summary sheet: Simple 2-column format (Component | DefectCount)")
    print("• Updated metadata sheet with component summary information")
    print("• Industry benchmark comparisons and recommended actions")
    print("• Proper Excel date writing with preserved zebra row shading")
    print("• Consistent sheet ordering: Trade → Room → Component → Unit summaries")
    
    print("\n📋 SHEET STRUCTURE:")
    print("1. 📊 Executive Dashboard")
    print("2. 📋 All Inspections")
    print("3. 🚨 Defects Only")
    print("4. 🏠 Settlement Readiness")
    print("5. 🔧 Trade Summary")
    print("6. 🚪 Room Summary") 
    print("7. 🔧 Component Summary")
    print("8. 🏠 Unit Summary")
    print("9. 📝 Component Details (if available)")
    print("10. 📄 Report Metadata")
    
    print("\nREADY FOR INTEGRATION WITH BUILDING INSPECTION SYSTEM V2!")