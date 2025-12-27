"""
Professional Word Report Generator - ECM Template Style
Building Inspection System V3

Features:
- Professional ECM template fonts and colors
- Modern, easy-to-read defect layouts
- Single inspection: Detailed report with unit information
- Multiple inspections: Executive summary matching template
- Logo and cover image support
- Proper text handling (no HTML entities)
"""

import os
import io
import tempfile
from datetime import datetime
from typing import List, Dict, Optional
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT, WD_LINE_SPACING
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import requests
from PIL import Image
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')


# ═══════════════════════════════════════════════════════════════════
# TEXT SANITIZATION - Proper handling without HTML entities
# ═══════════════════════════════════════════════════════════════════

def sanitize_text(text):
    """
    Sanitize text for Word documents - removes problematic characters
    without converting to HTML entities
    """
    if not text or text is None:
        return ""
    
    # Convert to string
    text = str(text).strip()
    
    # Remove null bytes and control characters (but keep & < > etc)
    text = text.replace('\x00', '')  # Null
    text = text.replace('\x0b', '')  # Vertical tab
    text = text.replace('\x0c', '')  # Form feed
    text = text.replace('\x1f', '')  # Unit separator
    
    # Remove other problematic Unicode but keep printable characters
    text = ''.join(char for char in text if char.isprintable() or char in '\n\r\t')
    
    return text


def safe_add_run(paragraph, text, **kwargs):
    """Safely add a run with sanitized text and formatting"""
    clean_text = sanitize_text(text)
    run = paragraph.add_run(clean_text)
    
    # Apply formatting
    if 'bold' in kwargs:
        run.bold = kwargs['bold']
    if 'italic' in kwargs:
        run.italic = kwargs['italic']
    if 'font_size' in kwargs:
        run.font.size = Pt(kwargs['font_size'])
    if 'color' in kwargs:
        run.font.color.rgb = kwargs['color']
    if 'font_name' in kwargs:
        run.font.name = kwargs['font_name']
    
    return run


# ═══════════════════════════════════════════════════════════════════
# PROFESSIONAL TEMPLATE COLORS & FONTS (ECM Style)
# ═══════════════════════════════════════════════════════════════════

class TemplateStyle:
    """Professional ECM template styling"""
    
    # Fonts
    FONT_MAIN = 'Calibri'
    FONT_HEADING = 'Calibri'
    
    # Colors (from professional template)
    COLOR_PRIMARY = RGBColor(0, 51, 102)      # Dark blue headings
    COLOR_SECONDARY = RGBColor(68, 114, 196)  # Medium blue
    COLOR_ACCENT = RGBColor(47, 84, 150)      # Lighter blue
    COLOR_SEPARATOR = RGBColor(200, 200, 200) # Gray separator
    COLOR_TEXT = RGBColor(0, 0, 0)            # Black text
    
    # Severity colors
    COLOR_URGENT = RGBColor(192, 0, 0)        # Dark red
    COLOR_HIGH = RGBColor(255, 102, 0)        # Orange
    COLOR_MEDIUM = RGBColor(255, 192, 0)      # Yellow
    COLOR_LOW = RGBColor(146, 208, 80)        # Light green
    COLOR_INFO = RGBColor(68, 114, 196)       # Blue


# ═══════════════════════════════════════════════════════════════════
# HEADER & FOOTER FORMATTING
# ═══════════════════════════════════════════════════════════════════

def add_logo_to_header(doc, logo_path):
    """Add logo to document header - ECM style"""
    if not logo_path or not os.path.exists(logo_path):
        return
    
    try:
        section = doc.sections[0]
        header = section.header
        
        for paragraph in header.paragraphs:
            paragraph.clear()
        
        paragraph = header.paragraphs[0] if header.paragraphs else header.add_paragraph()
        run = paragraph.add_run()
        run.add_picture(logo_path, width=Inches(2.0))
        paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
        
    except Exception as e:
        print(f"Warning: Could not add logo: {e}")


def add_cover_page(doc, building_name, cover_image_path=None):
    """Professional ECM cover page"""
    # Title
    title = doc.add_paragraph()
    title.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    title_run = title.add_run("PRE-SETTLEMENT\nINSPECTION REPORT")
    title_run.bold = True
    title_run.font.size = Pt(28)
    title_run.font.color.rgb = TemplateStyle.COLOR_PRIMARY
    title_run.font.name = TemplateStyle.FONT_HEADING
    
    # Separator
    sep = doc.add_paragraph()
    sep.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    sep_run = sep.add_run("─" * 40)
    sep_run.font.size = Pt(14)
    sep_run.font.color.rgb = TemplateStyle.COLOR_SEPARATOR
    
    # Cover image
    if cover_image_path and os.path.exists(cover_image_path):
        try:
            img_para = doc.add_paragraph()
            img_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            img_para.add_run().add_picture(cover_image_path, width=Inches(4.7))
            doc.add_paragraph()
        except Exception as e:
            print(f"Warning: Could not add cover image: {e}")
    
    # Building name
    building = doc.add_paragraph()
    building.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    safe_add_run(building, building_name, font_size=24, bold=True, 
                 color=TemplateStyle.COLOR_PRIMARY, font_name=TemplateStyle.FONT_HEADING)
    
    doc.add_page_break()


def add_section_heading(doc, text, level=1):
    """Professional section heading - ECM style"""
    heading = doc.add_heading(level=level)
    heading.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
    
    # Clear existing runs
    for run in heading.runs:
        run.text = ''
    
    # Add styled run
    run = heading.add_run(sanitize_text(text))
    run.bold = True
    run.font.name = TemplateStyle.FONT_HEADING
    
    if level == 1:
        run.font.size = Pt(18)
        run.font.color.rgb = TemplateStyle.COLOR_PRIMARY
    else:
        run.font.size = Pt(14)
        run.font.color.rgb = TemplateStyle.COLOR_ACCENT
    
    # Separator line
    separator = doc.add_paragraph()
    sep_run = separator.add_run("─" * 60)
    sep_run.font.color.rgb = TemplateStyle.COLOR_SEPARATOR
    
    return heading


def add_professional_footer(doc, building_name):
    """ECM style footer"""
    section = doc.sections[-1]
    footer = section.footer
    
    for paragraph in footer.paragraphs:
        paragraph.clear()
    
    p = footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    
    safe_add_run(p, f"{building_name} | Pre-Settlement Inspection Report | ", 
                 font_size=8, font_name=TemplateStyle.FONT_MAIN)
    safe_add_run(p, f"Generated {datetime.now().strftime('%d/%m/%Y')}", 
                 font_size=8, italic=True, font_name=TemplateStyle.FONT_MAIN)


def get_severity_color(severity):
    """Get color for severity level"""
    severity_map = {
        'Urgent': TemplateStyle.COLOR_URGENT,
        'High Priority': TemplateStyle.COLOR_HIGH,
        'Medium Priority': TemplateStyle.COLOR_MEDIUM,
        'Low Priority': TemplateStyle.COLOR_LOW,
        'For Information': TemplateStyle.COLOR_INFO
    }
    return severity_map.get(severity, TemplateStyle.COLOR_TEXT)


# ═══════════════════════════════════════════════════════════════════
# CHART GENERATION
# ═══════════════════════════════════════════════════════════════════

def create_trade_chart(df, output_path):
    """Trade distribution chart"""
    trade_counts = df['trade'].value_counts().head(10)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    trade_counts.plot(kind='barh', ax=ax, color='#4472C4')
    ax.set_xlabel('Number of Defects', fontsize=11)
    ax.set_ylabel('Trade Category', fontsize=11)
    ax.set_title('Top 10 Trades by Defect Count', fontsize=13, weight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def create_severity_chart(df, output_path):
    """Severity distribution pie chart"""
    severity_counts = df['severity'].value_counts()
    colors = ['#C00000', '#FF6600', '#FFC000', '#92D050', '#4472C4']
    
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(severity_counts.values, labels=severity_counts.index, autopct='%1.1f%%',
           colors=colors[:len(severity_counts)], startangle=90, textprops={'fontsize': 11})
    ax.set_title('Defect Distribution by Severity', fontsize=13, weight='bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def create_units_chart(df, output_path):
    """Top units by defect count"""
    unit_counts = df['unit'].value_counts().head(20)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    unit_counts.plot(kind='bar', ax=ax, color='#FF6600')
    ax.set_xlabel('Unit', fontsize=11)
    ax.set_ylabel('Number of Defects', fontsize=11)
    ax.set_title('Top 20 Units Requiring Immediate Intervention', fontsize=13, weight='bold')
    ax.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


# ═══════════════════════════════════════════════════════════════════
# PHOTO DOWNLOAD
# ═══════════════════════════════════════════════════════════════════

def download_photo(photo_url, api_key):
    """Download photo from SafetyCulture API"""
    try:
        headers = {'Authorization': f'Bearer {api_key}'}
        response = requests.get(photo_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return io.BytesIO(response.content)
        return None
    except Exception as e:
        print(f"Error downloading photo: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# SINGLE INSPECTION REPORT - MODERN REDESIGN
# ═══════════════════════════════════════════════════════════════════

def generate_single_inspection_report(doc, df, building_name, api_key, images=None):
    """Modern single inspection report with unit details"""
    
    # Add header and cover
    if images:
        add_logo_to_header(doc, images.get('logo'))
    add_cover_page(doc, building_name, images.get('cover') if images else None)
    
    # ─────────────────────────────────────────────────────────────────
    # UNIT INFORMATION
    # ─────────────────────────────────────────────────────────────────
    
    add_section_heading(doc, "UNIT INFORMATION", level=1)
    
    # Get unit details
    unit = df['unit'].iloc[0] if 'unit' in df.columns and len(df) > 0 else 'Unknown'
    unit_type = df['unit_type'].iloc[0] if 'unit_type' in df.columns and len(df) > 0 else 'Apartment'
    inspection_date = df['inspection_date'].iloc[0] if 'inspection_date' in df.columns and len(df) > 0 else 'N/A'
    
    # Format date
    if isinstance(inspection_date, pd.Timestamp):
        inspection_date = inspection_date.strftime('%d %B %Y')
    elif inspection_date != 'N/A':
        inspection_date = str(inspection_date)
    
    # Unit info table
    table = doc.add_table(rows=4, cols=2)
    table.style = 'Light Grid Accent 1'
    
    unit_details = [
        ('Building', building_name),
        ('Unit Number', unit),
        ('Unit Type', unit_type),
        ('Inspection Date', inspection_date)
    ]
    
    for i, (label, value) in enumerate(unit_details):
        cell_label = table.rows[i].cells[0]
        cell_value = table.rows[i].cells[1]
        
        # Label formatting
        p_label = cell_label.paragraphs[0]
        safe_add_run(p_label, label, bold=True, font_size=11, font_name=TemplateStyle.FONT_MAIN)
        
        # Value formatting
        p_value = cell_value.paragraphs[0]
        safe_add_run(p_value, str(value), font_size=11, font_name=TemplateStyle.FONT_MAIN)
    
    doc.add_paragraph()
    
    # ─────────────────────────────────────────────────────────────────
    # EXECUTIVE SUMMARY
    # ─────────────────────────────────────────────────────────────────
    
    add_section_heading(doc, "EXECUTIVE SUMMARY", level=1)
    
    total_defects = len(df)
    severity_counts = df['severity'].value_counts()
    trade_counts = df['trade'].value_counts()
    
    p = doc.add_paragraph()
    safe_add_run(p, f"Total Defects Identified: ", bold=True, font_size=12, font_name=TemplateStyle.FONT_MAIN)
    safe_add_run(p, f"{total_defects}\n", font_size=12, font_name=TemplateStyle.FONT_MAIN, 
                 color=TemplateStyle.COLOR_URGENT if total_defects > 15 else TemplateStyle.COLOR_PRIMARY)
    
    if len(severity_counts) > 0:
        safe_add_run(p, f"Most Common Severity: ", bold=True, font_size=11, font_name=TemplateStyle.FONT_MAIN)
        safe_add_run(p, f"{severity_counts.index[0]} ({severity_counts.iloc[0]} defects)\n", 
                     font_size=11, font_name=TemplateStyle.FONT_MAIN)
    
    if len(trade_counts) > 0:
        safe_add_run(p, f"Primary Trade Category: ", bold=True, font_size=11, font_name=TemplateStyle.FONT_MAIN)
        safe_add_run(p, f"{trade_counts.index[0]} ({trade_counts.iloc[0]} defects)", 
                     font_size=11, font_name=TemplateStyle.FONT_MAIN)
    
    doc.add_paragraph()
    
    # Severity breakdown
    doc.add_paragraph("Severity Breakdown:", style='Heading 2')
    
    for severity, count in severity_counts.items():
        p = doc.add_paragraph()
        run = p.add_run(f"● {severity}: {count} defect{'s' if count != 1 else ''}")
        run.font.color.rgb = get_severity_color(severity)
        run.font.bold = True
        run.font.size = Pt(11)
        run.font.name = TemplateStyle.FONT_MAIN
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # ─────────────────────────────────────────────────────────────────
    # DETAILED DEFECTS - MODERN CARD LAYOUT
    # ─────────────────────────────────────────────────────────────────
    
    add_section_heading(doc, "DETAILED DEFECTS", level=1)
    
    for idx, row in df.iterrows():
        # Defect card header
        header_para = doc.add_paragraph()
        header_para.paragraph_format.space_before = Pt(6)
        header_para.paragraph_format.space_after = Pt(6)
        
        # Defect number and severity badge
        safe_add_run(header_para, f"Defect {idx + 1} of {total_defects}", 
                     bold=True, font_size=13, font_name=TemplateStyle.FONT_HEADING,
                     color=TemplateStyle.COLOR_PRIMARY)
        safe_add_run(header_para, "  │  ", font_size=13, color=TemplateStyle.COLOR_SEPARATOR)
        safe_add_run(header_para, row.get('severity', 'Unknown'), 
                     bold=True, font_size=13, font_name=TemplateStyle.FONT_MAIN,
                     color=get_severity_color(row.get('severity', '')))
        
        # Defect info - Modern 2-column layout
        table = doc.add_table(rows=4, cols=2)
        table.style = 'Light Grid Accent 1'
        
        # Left column
        left_details = [
            ('📍 Room/Location', row.get('room', 'Unknown')),
            ('🔧 Trade Category', row.get('trade', 'Unknown'))
        ]
        
        # Right column
        right_details = [
            ('🔩 Component', row.get('component', 'Unknown')),
            ('🏠 Unit', row.get('unit', 'Unknown'))
        ]
        
        # Fill table
        for i in range(2):
            # Left side
            cell_left_label = table.rows[i].cells[0]
            p_left = cell_left_label.paragraphs[0]
            safe_add_run(p_left, left_details[i][0], bold=True, font_size=10, font_name=TemplateStyle.FONT_MAIN)
            
            cell_left_value = table.rows[i + 2].cells[0]
            p_left_val = cell_left_value.paragraphs[0]
            safe_add_run(p_left_val, str(left_details[i][1]), font_size=10, font_name=TemplateStyle.FONT_MAIN)
            
            # Right side
            cell_right_label = table.rows[i].cells[1]
            p_right = cell_right_label.paragraphs[0]
            safe_add_run(p_right, right_details[i][0], bold=True, font_size=10, font_name=TemplateStyle.FONT_MAIN)
            
            cell_right_value = table.rows[i + 2].cells[1]
            p_right_val = cell_right_value.paragraphs[0]
            safe_add_run(p_right_val, str(right_details[i][1]), font_size=10, font_name=TemplateStyle.FONT_MAIN)
        
        doc.add_paragraph()
        
        # Issue description box
        desc_para = doc.add_paragraph()
        desc_para.paragraph_format.left_indent = Inches(0.25)
        desc_para.paragraph_format.right_indent = Inches(0.25)
        safe_add_run(desc_para, "📝 Issue Description:", bold=True, font_size=11, 
                     font_name=TemplateStyle.FONT_MAIN, color=TemplateStyle.COLOR_ACCENT)
        
        desc_text = doc.add_paragraph()
        desc_text.paragraph_format.left_indent = Inches(0.25)
        desc_text.paragraph_format.right_indent = Inches(0.25)
        safe_add_run(desc_text, row.get('description', 'No description'), 
                     font_size=10, font_name=TemplateStyle.FONT_MAIN)
        
        # Inspector notes (if any)
        notes = row.get('notes', '')
        if notes and str(notes).strip() and str(notes).lower() != 'nan':
            notes_para = doc.add_paragraph()
            notes_para.paragraph_format.left_indent = Inches(0.25)
            safe_add_run(notes_para, "💬 Inspector Notes:", bold=True, font_size=11,
                         font_name=TemplateStyle.FONT_MAIN, color=TemplateStyle.COLOR_ACCENT)
            
            notes_text = doc.add_paragraph()
            notes_text.paragraph_format.left_indent = Inches(0.25)
            notes_text.paragraph_format.right_indent = Inches(0.25)
            safe_add_run(notes_text, str(notes), font_size=10, font_name=TemplateStyle.FONT_MAIN, italic=True)
        
        doc.add_paragraph()
        
        # Photo
        photo_url = row.get('photo_url')
        if photo_url and api_key:
            photo_data = download_photo(photo_url, api_key)
            if photo_data:
                try:
                    img_para = doc.add_paragraph()
                    img_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
                    img_para.add_run().add_picture(photo_data, width=Inches(5.0))
                    
                    caption = doc.add_paragraph()
                    caption.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
                    safe_add_run(caption, f"📷 {row.get('room', 'Unknown')} - {row.get('component', 'Unknown')}", 
                                italic=True, font_size=9, font_name=TemplateStyle.FONT_MAIN,
                                color=TemplateStyle.COLOR_SEPARATOR)
                except Exception as e:
                    print(f"Error embedding photo: {e}")
        
        # Separator
        sep_para = doc.add_paragraph()
        sep_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        sep_run = sep_para.add_run("─" * 80)
        sep_run.font.color.rgb = TemplateStyle.COLOR_SEPARATOR
        sep_run.font.size = Pt(8)
        
        doc.add_paragraph()
    
    # Recommendations
    doc.add_page_break()
    add_section_heading(doc, "RECOMMENDATIONS", level=1)
    
    p = doc.add_paragraph()
    safe_add_run(p, "Based on the inspection findings for this unit:\n\n", 
                 bold=True, font_size=11, font_name=TemplateStyle.FONT_MAIN)
    
    urgent_count = severity_counts.get('Urgent', 0)
    top_trade = trade_counts.index[0] if len(trade_counts) > 0 else 'General'
    
    recommendations = [
        f"1. Address all {urgent_count} Urgent defects immediately before settlement" if urgent_count > 0 else "1. Address high priority defects before settlement",
        f"2. Schedule {top_trade} remediation work as a priority",
        "3. Conduct follow-up inspection after remediation",
        "4. Document all completed repairs with photographic evidence"
    ]
    
    for rec in recommendations:
        p = doc.add_paragraph()
        p.style = 'List Bullet'
        safe_add_run(p, rec, font_size=10, font_name=TemplateStyle.FONT_MAIN)
    
    add_professional_footer(doc, building_name)


# ═══════════════════════════════════════════════════════════════════
# MULTI-INSPECTION REPORT - ECM TEMPLATE STYLE
# ═══════════════════════════════════════════════════════════════════

def generate_multi_inspection_summary(doc, df, building_name, inspection_date_range, images=None):
    """Executive summary matching ECM professional template"""
    
    if images:
        add_logo_to_header(doc, images.get('logo'))
    add_cover_page(doc, building_name, images.get('cover') if images else None)
    
    # ─────────────────────────────────────────────────────────────────
    # INSPECTION OVERVIEW
    # ─────────────────────────────────────────────────────────────────
    
    add_section_heading(doc, "INSPECTION OVERVIEW", level=1)
    
    units_inspected = df['unit'].nunique()
    total_defects = len(df)
    
    p = doc.add_paragraph()
    safe_add_run(p, f"Generated on {datetime.now().strftime('%d %B %Y')}\n\n", 
                 font_size=10, font_name=TemplateStyle.FONT_MAIN)
    safe_add_run(p, f"Inspection Date: {inspection_date_range}\n", 
                 font_size=10, font_name=TemplateStyle.FONT_MAIN)
    safe_add_run(p, f"Units Inspected: {units_inspected}\n", 
                 font_size=10, font_name=TemplateStyle.FONT_MAIN)
    safe_add_run(p, f"Total Defects: {total_defects}", 
                 font_size=10, font_name=TemplateStyle.FONT_MAIN)
    
    doc.add_paragraph()
    
    # ─────────────────────────────────────────────────────────────────
    # EXECUTIVE OVERVIEW
    # ─────────────────────────────────────────────────────────────────
    
    add_section_heading(doc, "EXECUTIVE OVERVIEW", level=1)
    
    p = doc.add_paragraph()
    safe_add_run(p, 
        f"This comprehensive quality assessment encompasses the systematic evaluation of {units_inspected} residential units "
        f"with {total_defects} defects identified across multiple trade categories.\n\n",
        font_size=11, font_name=TemplateStyle.FONT_MAIN
    )
    
    # ─────────────────────────────────────────────────────────────────
    # CHARTS
    # ─────────────────────────────────────────────────────────────────
    
    doc.add_page_break()
    add_section_heading(doc, "DATA VISUALIZATION", level=1)
    
    # Chart 1: Trade Distribution
    doc.add_paragraph("Defects Distribution by Trade Category", style='Heading 2')
    
    try:
        chart_path = os.path.join(tempfile.gettempdir(), 'trade_chart.png')
        create_trade_chart(df, chart_path)
        if os.path.exists(chart_path):
            doc.add_picture(chart_path, width=Inches(6.0))
            os.remove(chart_path)
    except Exception as e:
        print(f"Error creating trade chart: {e}")
    
    doc.add_paragraph()
    
    # Chart 2: Severity
    doc.add_paragraph("Defect Distribution by Severity", style='Heading 2')
    
    try:
        chart_path = os.path.join(tempfile.gettempdir(), 'severity_chart.png')
        create_severity_chart(df, chart_path)
        if os.path.exists(chart_path):
            doc.add_picture(chart_path, width=Inches(5.0))
            os.remove(chart_path)
    except Exception as e:
        print(f"Error creating severity chart: {e}")
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # Chart 3: Top Units
    doc.add_paragraph("Top Units Requiring Attention", style='Heading 2')
    
    try:
        chart_path = os.path.join(tempfile.gettempdir(), 'units_chart.png')
        create_units_chart(df, chart_path)
        if os.path.exists(chart_path):
            doc.add_picture(chart_path, width=Inches(6.5))
            os.remove(chart_path)
    except Exception as e:
        print(f"Error creating units chart: {e}")
    
    doc.add_paragraph()
    
    add_professional_footer(doc, building_name)


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def create_word_report_from_database(
    inspection_ids: List[str],
    db_connection,
    api_key: str,
    output_path: str,
    report_type: str = "single",
    images: Dict = None
) -> bool:
    """Generate professional Word report"""
    
    try:
        print(f"Generating Word report (type: {report_type})...")
        
        cursor = db_connection.cursor()
        
        # Get building name and dates
        cursor.execute("""
            SELECT 
                b.name as building_name,
                MIN(i.inspection_date) as start_date,
                MAX(i.inspection_date) as end_date
            FROM inspector_inspections i
            JOIN inspector_buildings b ON i.building_id = b.id
            WHERE i.id = ANY(%s)
            GROUP BY b.name
        """, (inspection_ids,))
        
        row = cursor.fetchone()
        building_name = row[0] if row else "Unknown Building"
        start_date = row[1] if row else None
        end_date = row[2] if row else None
        
        if start_date and end_date:
            if start_date == end_date:
                date_range = start_date.strftime('%d %B %Y')
            else:
                date_range = f"{start_date.strftime('%d %B %Y')} to {end_date.strftime('%d %B %Y')}"
        else:
            date_range = "Date not specified"
        
        print(f"Building: {building_name}")
        
        # Get defects
        cursor.execute("""
            SELECT 
                ii.room,
                ii.component,
                ii.notes,
                ii.trade,
                ii.urgency,
                ii.status_class,
                ii.photo_url,
                ii.photo_media_id,
                ii.inspector_notes,
                ii.inspection_date,
                ii.created_at,
                ii.planned_completion,
                ii.owner_signoff_timestamp,
                ii.unit,
                b.name as building_name,
                ii.unit_type
            FROM inspector_inspection_items ii
            JOIN inspector_inspections i ON ii.inspection_id = i.id
            JOIN inspector_buildings b ON i.building_id = b.id
            WHERE ii.inspection_id = ANY(%s)
            AND LOWER(ii.status_class) = 'not ok'
            ORDER BY ii.unit, ii.room, ii.component
        """, (inspection_ids,))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if len(rows) == 0:
            print("No defects found")
            return False
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=[
            'room', 'component', 'description', 'trade', 'severity', 'status',
            'photo_url', 'photo_media_id', 'notes', 'inspection_date', 
            'created_at', 'planned_completion', 'owner_signoff_timestamp',
            'unit', 'building_name', 'unit_type'
        ])
        
        print(f"Found {len(df)} defects")
        
        # Create document
        doc = Document()
        
        # Set margins
        for section in doc.sections:
            section.top_margin = Inches(1.0)
            section.bottom_margin = Inches(1.0)
            section.left_margin = Inches(1.0)
            section.right_margin = Inches(1.0)
        
        # Generate report
        if report_type == "single" or len(inspection_ids) == 1:
            print("Generating SINGLE unit report...")
            generate_single_inspection_report(doc, df, building_name, api_key, images)
        else:
            print("Generating BUILDING summary report...")
            generate_multi_inspection_summary(doc, df, building_name, date_range, images)
        
        doc.save(output_path)
        print(f"✅ Saved: {output_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Professional Word Generator - ECM Template Style")