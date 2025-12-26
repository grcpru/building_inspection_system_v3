"""
Professional Word Report Generator with Dual-Report Support
Building Inspection System V3

Features:
- Single inspection: Detailed report with individual defects and photos
- Multiple inspections: Executive summary with charts and analytics
- Logo and cover image support
- XML text sanitization (fixes parsing errors)
- Professional formatting matching CSV template
"""

import os
import io
import html
import tempfile
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import requests
from PIL import Image
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend


# ═══════════════════════════════════════════════════════════════════
# TEXT SANITIZATION - Prevents XML Parsing Errors
# ═══════════════════════════════════════════════════════════════════

def sanitize_text(text):
    """
    Sanitize text to prevent XML parsing errors
    
    Fixes: "error parsing attribute name, line 1, column 8"
    """
    if not text or text is None:
        return ""
    
    # Convert to string
    text = str(text).strip()
    
    # Escape XML special characters
    text = html.escape(text, quote=False)
    
    # Remove null bytes and control characters
    text = text.replace('\x00', '')  # Null
    text = text.replace('\x0b', '')  # Vertical tab
    text = text.replace('\x0c', '')  # Form feed
    text = text.replace('\x1f', '')  # Unit separator
    
    # Remove any other problematic Unicode characters
    text = ''.join(char for char in text if char.isprintable() or char in '\n\r\t')
    
    return text


def safe_add_run(paragraph, text, **kwargs):
    """Safely add a run with sanitized text"""
    run = paragraph.add_run(sanitize_text(text))
    
    # Apply formatting
    if 'bold' in kwargs:
        run.bold = kwargs['bold']
    if 'italic' in kwargs:
        run.italic = kwargs['italic']
    if 'font_size' in kwargs:
        run.font.size = Pt(kwargs['font_size'])
    if 'color' in kwargs:
        run.font.color.rgb = kwargs['color']
    
    return run


# ═══════════════════════════════════════════════════════════════════
# COMMON FORMATTING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def add_logo_to_header(doc, logo_path):
    """Add logo to document header"""
    if not logo_path or not os.path.exists(logo_path):
        return
    
    try:
        section = doc.sections[0]
        header = section.header
        
        # Clear existing header content
        for paragraph in header.paragraphs:
            paragraph.clear()
        
        # Add logo
        paragraph = header.paragraphs[0] if header.paragraphs else header.add_paragraph()
        run = paragraph.add_run()
        run.add_picture(logo_path, width=Inches(2.0))
        paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
        
    except Exception as e:
        print(f"Warning: Could not add logo to header: {e}")


def add_cover_page(doc, building_name, cover_image_path=None):
    """Add professional cover page with optional cover image"""
    # Title
    title = doc.add_paragraph()
    title.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    run = title.add_run("PRE-SETTLEMENT\nINSPECTION REPORT")
    run.bold = True
    run.font.size = Pt(28)
    run.font.color.rgb = RGBColor(0, 51, 102)
    
    # Separator
    sep = doc.add_paragraph()
    sep.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    sep.add_run("─" * 40).font.size = Pt(14)
    
    # Cover image (if provided)
    if cover_image_path and os.path.exists(cover_image_path):
        try:
            img_para = doc.add_paragraph()
            img_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            run = img_para.add_run()
            run.add_picture(cover_image_path, width=Inches(4.7))
            doc.add_paragraph()  # Spacing
        except Exception as e:
            print(f"Warning: Could not add cover image: {e}")
    
    # Building name
    building = doc.add_paragraph()
    building.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    safe_add_run(building, building_name, font_size=24, bold=True)
    
    # Page break
    doc.add_page_break()


def add_section_heading(doc, text, level=1):
    """Add a formatted section heading"""
    heading = doc.add_heading(level=level)
    heading.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
    
    run = heading.runs[0] if heading.runs else heading.add_run()
    run.text = sanitize_text(text)
    run.bold = True
    
    if level == 1:
        run.font.size = Pt(18)
        run.font.color.rgb = RGBColor(0, 51, 102)
    else:
        run.font.size = Pt(14)
        run.font.color.rgb = RGBColor(47, 84, 150)
    
    # Underline
    separator = doc.add_paragraph()
    separator.add_run("─" * 60).font.color.rgb = RGBColor(200, 200, 200)
    
    return heading


def get_severity_color(severity):
    """Get color for severity level"""
    severity_colors = {
        'Urgent': RGBColor(192, 0, 0),      # Dark red
        'High Priority': RGBColor(255, 102, 0),  # Orange
        'Medium Priority': RGBColor(255, 192, 0),  # Yellow
        'Low Priority': RGBColor(146, 208, 80),   # Light green
        'For Information': RGBColor(68, 114, 196)  # Blue
    }
    return severity_colors.get(severity, RGBColor(0, 0, 0))


# ═══════════════════════════════════════════════════════════════════
# CHART GENERATION
# ═══════════════════════════════════════════════════════════════════

def create_trade_chart(df, output_path):
    """Create horizontal bar chart for trade distribution"""
    trade_counts = df['trade'].value_counts().head(10)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    trade_counts.plot(kind='barh', ax=ax, color='#4472C4')
    ax.set_xlabel('Number of Defects')
    ax.set_ylabel('Trade Category')
    ax.set_title('Top 10 Trades by Defect Count')
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def create_severity_chart(df, output_path):
    """Create pie chart for severity distribution"""
    severity_counts = df['severity'].value_counts()
    
    colors = ['#C00000', '#FF6600', '#FFC000', '#92D050', '#4472C4']
    
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(severity_counts.values, labels=severity_counts.index, autopct='%1.1f%%',
           colors=colors[:len(severity_counts)], startangle=90)
    ax.set_title('Defect Distribution by Severity')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def create_units_chart(df, output_path):
    """Create bar chart for top units by defect count"""
    unit_counts = df['unit'].value_counts().head(20)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    unit_counts.plot(kind='bar', ax=ax, color='#FF6600')
    ax.set_xlabel('Unit')
    ax.set_ylabel('Number of Defects')
    ax.set_title('Top 20 Units Requiring Immediate Intervention')
    ax.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def create_performance_chart(df, output_path):
    """Create trade performance analysis chart"""
    trade_severity = df.groupby(['trade', 'severity']).size().unstack(fill_value=0)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    trade_severity.head(10).plot(kind='bar', stacked=True, ax=ax,
                                  color=['#C00000', '#FF6600', '#FFC000', '#92D050', '#4472C4'])
    ax.set_xlabel('Trade Category')
    ax.set_ylabel('Number of Defects')
    ax.set_title('Trade Performance Analysis by Severity')
    ax.legend(title='Severity', bbox_to_anchor=(1.05, 1), loc='upper left')
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
        else:
            print(f"Failed to download photo: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading photo: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# SINGLE INSPECTION REPORT (Detailed with Photos)
# ═══════════════════════════════════════════════════════════════════

def generate_single_inspection_report(doc, df, building_name, api_key, images=None):
    """Generate detailed report for single inspection with photos"""
    
    # Add cover page with logo and cover image
    if images:
        add_logo_to_header(doc, images.get('logo'))
    add_cover_page(doc, building_name, images.get('cover') if images else None)
    
    # Executive Overview
    add_section_heading(doc, "EXECUTIVE OVERVIEW", level=1)
    
    total_defects = len(df)
    severity_counts = df['severity'].value_counts()
    trade_counts = df['trade'].value_counts()
    
    p = doc.add_paragraph()
    safe_add_run(p, f"Total Defects Identified: {total_defects}\n", bold=True, font_size=12)
    safe_add_run(p, f"Most Common Severity: {severity_counts.index[0]} ({severity_counts.iloc[0]} defects)\n", font_size=11)
    safe_add_run(p, f"Primary Trade Category: {trade_counts.index[0]} ({trade_counts.iloc[0]} defects)\n", font_size=11)
    
    doc.add_paragraph()
    
    # Severity Analysis
    add_section_heading(doc, "SEVERITY ANALYSIS", level=1)
    
    for severity, count in severity_counts.items():
        p = doc.add_paragraph()
        run = p.add_run(f"● {severity}: {count} defects")
        run.font.color.rgb = get_severity_color(severity)
        run.bold = True
    
    doc.add_paragraph()
    
    # Trade Distribution Chart
    add_section_heading(doc, "TRADE DISTRIBUTION", level=1)
    
    try:
        chart_path = os.path.join(tempfile.gettempdir(), 'trade_chart.png')
        create_trade_chart(df, chart_path)
        
        if os.path.exists(chart_path):
            doc.add_picture(chart_path, width=Inches(6.0))
            os.remove(chart_path)
    except Exception as e:
        print(f"Error creating trade chart: {e}")
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # Detailed Defects
    add_section_heading(doc, "DETAILED DEFECTS", level=1)
    
    for idx, row in df.iterrows():
        # Defect header
        p = doc.add_paragraph()
        safe_add_run(p, f"Defect {idx + 1} of {total_defects}", bold=True, font_size=14)
        
        # Defect details table
        table = doc.add_table(rows=7, cols=2)
        table.style = 'Light Grid Accent 1'
        
        details = [
            ('Room/Location', row.get('room', 'Unknown')),
            ('Component', row.get('component', 'Unknown')),
            ('Issue Description', row.get('issue', 'No description')),
            ('Severity', row.get('severity', 'Unknown')),
            ('Trade Category', row.get('trade', 'Unknown')),
            ('Unit', row.get('unit', 'Unknown')),
            ('Inspector Notes', row.get('notes', 'No notes'))
        ]
        
        for i, (label, value) in enumerate(details):
            table.rows[i].cells[0].text = label
            table.rows[i].cells[0].paragraphs[0].runs[0].bold = True
            safe_add_run(table.rows[i].cells[1].paragraphs[0], str(value))
        
        doc.add_paragraph()
        
        # Photo
        photo_url = row.get('photo_url')
        if photo_url and api_key:
            photo_data = download_photo(photo_url, api_key)
            if photo_data:
                try:
                    doc.add_picture(photo_data, width=Inches(5.0))
                    p = doc.add_paragraph()
                    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
                    safe_add_run(p, f"Photo: {row.get('room', 'Unknown')} - {row.get('component', 'Unknown')}", 
                               italic=True, font_size=9)
                except Exception as e:
                    print(f"Error embedding photo for defect {idx + 1}: {e}")
        
        doc.add_paragraph()
        doc.add_paragraph("─" * 80)
        doc.add_paragraph()
    
    # Recommendations
    doc.add_page_break()
    add_section_heading(doc, "RECOMMENDATIONS", level=1)
    
    p = doc.add_paragraph()
    safe_add_run(p, "Based on the inspection findings, we recommend:\n\n", bold=True)
    
    recommendations = [
        f"1. Address all {severity_counts.get('Urgent', 0)} Urgent defects immediately before settlement",
        f"2. Schedule {trade_counts.index[0]} remediation work as a priority",
        "3. Conduct follow-up inspection after remediation",
        "4. Document all completed repairs with photographic evidence"
    ]
    
    for rec in recommendations:
        p = doc.add_paragraph(sanitize_text(rec))
        p.style = 'List Bullet'
    
    # Footer
    add_professional_footer(doc, building_name)


# ═══════════════════════════════════════════════════════════════════
# MULTI-INSPECTION REPORT (Executive Summary like CSV)
# ═══════════════════════════════════════════════════════════════════

def generate_multi_inspection_summary(doc, df, building_name, inspection_date_range, images=None):
    """Generate executive summary report for multiple inspections (matches CSV template)"""
    
    # Add cover page with logo and cover image
    if images:
        add_logo_to_header(doc, images.get('logo'))
    add_cover_page(doc, building_name, images.get('cover') if images else None)
    
    # Inspection Overview
    add_section_heading(doc, "INSPECTION OVERVIEW", level=1)
    
    units_inspected = df['unit'].nunique()
    total_defects = len(df)
    date_str = inspection_date_range if inspection_date_range else "Date range not specified"
    
    p = doc.add_paragraph()
    safe_add_run(p, f"Generated on {datetime.now().strftime('%d %B %Y')}\n\n", font_size=10)
    safe_add_run(p, f"Inspection Date: {date_str}\n", font_size=10)
    safe_add_run(p, f"Units Inspected: {units_inspected}\n", font_size=10)
    safe_add_run(p, f"Total Defects: {total_defects}\n", font_size=10)
    
    doc.add_paragraph()
    
    # Executive Overview
    add_section_heading(doc, "EXECUTIVE OVERVIEW", level=1)
    
    severity_counts = df['severity'].value_counts()
    trade_counts = df['trade'].value_counts()
    
    p = doc.add_paragraph()
    safe_add_run(p, 
        f"This comprehensive quality assessment encompasses the systematic evaluation of {units_inspected} residential units "
        f"with {total_defects} defects identified across multiple trade categories.\n\n",
        font_size=11
    )
    
    # Inspection Process & Methodology
    add_section_heading(doc, "INSPECTION PROCESS & METHODOLOGY", level=1)
    
    doc.add_paragraph("INSPECTION SCOPE & STANDARDS", style='Heading 2')
    p = doc.add_paragraph()
    safe_add_run(p,
        f"The comprehensive pre-settlement quality assessment was systematically executed across {units_inspected} units "
        f"utilizing industry-standard inspection protocols and quality benchmarks.\n",
        font_size=10
    )
    
    doc.add_paragraph()
    
    # Units Requiring Priority Attention
    add_section_heading(doc, "UNITS REQUIRING PRIORITY ATTENTION", level=1)
    
    doc.add_paragraph("Top 20 Units Requiring Immediate Intervention", style='Heading 2')
    
    unit_defect_counts = df['unit'].value_counts().head(20)
    if len(unit_defect_counts) > 0:
        top_unit = unit_defect_counts.index[0]
        top_count = unit_defect_counts.iloc[0]
        
        p = doc.add_paragraph()
        safe_add_run(p,
            f"Priority Analysis Results: Unit {top_unit} requires immediate priority attention with {top_count} identified defects, "
            f"representing the highest concentration of remediation needs within the development.\n",
            font_size=10
        )
    
    doc.add_paragraph()
    
    # Defect Patterns & Analysis
    add_section_heading(doc, "DEFECT PATTERNS & ANALYSIS", level=1)
    
    p = doc.add_paragraph()
    safe_add_run(p,
        f"Primary Defect Category Analysis: The comprehensive evaluation of {total_defects} individual defects reveals "
        f"systematic patterns requiring strategic remediation approaches.\n",
        font_size=10
    )
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # COMPREHENSIVE DATA VISUALIZATION
    add_section_heading(doc, "COMPREHENSIVE DATA VISUALISATION", level=1)
    
    p = doc.add_paragraph()
    safe_add_run(p,
        "This section presents visual analytics of the inspection data, highlighting key patterns and priorities.\n\n",
        font_size=10
    )
    
    # Chart 1: Trade Distribution
    doc.add_paragraph("Defects Distribution by Trade Category", style='Heading 2')
    
    p = doc.add_paragraph()
    safe_add_run(p,
        f"The analysis reveals {trade_counts.index[0]} as the primary defect category, representing {trade_counts.iloc[0]} "
        f"of the total {total_defects} defects identified.\n",
        font_size=10
    )
    
    try:
        chart_path = os.path.join(tempfile.gettempdir(), 'trade_chart.png')
        create_trade_chart(df, chart_path)
        if os.path.exists(chart_path):
            doc.add_picture(chart_path, width=Inches(6.0))
            os.remove(chart_path)
    except Exception as e:
        print(f"Error creating trade chart: {e}")
    
    doc.add_paragraph()
    
    # Chart 2: Severity Classification
    doc.add_paragraph("Unit Classification by Defect Severity", style='Heading 2')
    
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
    doc.add_paragraph("Trade Category Performance Analysis", style='Heading 2')
    
    try:
        chart_path = os.path.join(tempfile.gettempdir(), 'units_chart.png')
        create_units_chart(df, chart_path)
        if os.path.exists(chart_path):
            doc.add_picture(chart_path, width=Inches(6.5))
            os.remove(chart_path)
    except Exception as e:
        print(f"Error creating units chart: {e}")
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # Trade-Specific Defect Analysis
    add_section_heading(doc, "TRADE-SPECIFIC DEFECT ANALYSIS", level=1)
    
    p = doc.add_paragraph()
    safe_add_run(p,
        "This section provides a comprehensive breakdown of identified defects organized by trade category.\n\n",
        font_size=10
    )
    
    for trade in trade_counts.head(10).index:
        doc.add_paragraph(sanitize_text(trade), style='Heading 2')
        
        trade_df = df[df['trade'] == trade]
        component_counts = trade_df.groupby(['component', 'room']).size().reset_index(name='count')
        component_counts = component_counts.sort_values('count', ascending=False).head(10)
        
        if len(component_counts) > 0:
            table = doc.add_table(rows=1 + len(component_counts), cols=3)
            table.style = 'Light Grid Accent 1'
            
            # Headers
            headers = ['Component & Location', 'Affected Units', 'Count']
            for i, header in enumerate(headers):
                table.rows[0].cells[i].text = header
                table.rows[0].cells[i].paragraphs[0].runs[0].bold = True
            
            # Data
            for idx, row in component_counts.iterrows():
                row_idx = component_counts.index.get_loc(idx) + 1
                affected_units = trade_df[
                    (trade_df['component'] == row['component']) & 
                    (trade_df['room'] == row['room'])
                ]['unit'].unique()
                
                safe_add_run(table.rows[row_idx].cells[0].paragraphs[0], 
                           f"{row['component']} ({row['room']})")
                safe_add_run(table.rows[row_idx].cells[1].paragraphs[0], 
                           ", ".join(map(str, affected_units[:4])))
                safe_add_run(table.rows[row_idx].cells[2].paragraphs[0], 
                           str(row['count']))
        
        doc.add_paragraph()
    
    doc.add_page_break()
    
    # Component-Level Analysis
    add_section_heading(doc, "COMPONENT-LEVEL ANALYSIS", level=1)
    
    doc.add_paragraph("Most Frequently Affected Components", style='Heading 2')
    
    component_counts = df.groupby(['component', 'room']).size().reset_index(name='count')
    component_counts = component_counts.sort_values('count', ascending=False).head(15)
    
    if len(component_counts) > 0:
        table = doc.add_table(rows=1 + len(component_counts), cols=3)
        table.style = 'Light Grid Accent 1'
        
        # Headers
        table.rows[0].cells[0].text = 'Component'
        table.rows[0].cells[1].text = 'Trade'
        table.rows[0].cells[2].text = 'Count'
        
        for cell in table.rows[0].cells:
            cell.paragraphs[0].runs[0].bold = True
        
        # Data
        for idx, row in component_counts.iterrows():
            row_idx = component_counts.index.get_loc(idx) + 1
            component_df = df[(df['component'] == row['component']) & (df['room'] == row['room'])]
            trade = component_df['trade'].mode()[0] if len(component_df) > 0 else 'Unknown'
            
            safe_add_run(table.rows[row_idx].cells[0].paragraphs[0], row['component'])
            safe_add_run(table.rows[row_idx].cells[1].paragraphs[0], trade)
            safe_add_run(table.rows[row_idx].cells[2].paragraphs[0], str(row['count']))
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # Strategic Recommendations & Action Plan
    add_section_heading(doc, "STRATEGIC RECOMMENDATIONS & ACTION PLAN", level=1)
    
    doc.add_paragraph("IMMEDIATE PRIORITIES", style='Heading 2')
    
    recommendations = [
        ("1. Quality-First Approach", 
         "Implement comprehensive remediation program before handover to ensure adherence to quality standards."),
        ("2. Painting Focus Initiative", 
         f"This trade represents {trade_counts.get('Painting', 0) / total_defects * 100:.1f}% of all defects "
         f"({trade_counts.get('Painting', 0)} instances), requiring specialized attention."),
        ("3. Specialized Remediation Teams", 
         f"{len([c for c in unit_defect_counts if c >= 15])} units require extensive work (15+ defects each), "
         f"necessitating dedicated remediation resources."),
        ("4. Enhanced Quality Protocols", 
         "Implement multi-tier inspection checkpoints with photographic documentation requirements.")
    ]
    
    for title, desc in recommendations:
        p = doc.add_paragraph()
        safe_add_run(p, f"{title}: ", bold=True, font_size=11)
        safe_add_run(p, desc, font_size=10)
    
    doc.add_paragraph()
    doc.add_page_break()
    
    # Report Documentation & Appendices
    add_section_heading(doc, "REPORT DOCUMENTATION & APPENDICES", level=1)
    
    doc.add_paragraph("COMPREHENSIVE INSPECTION METRICS", style='Heading 2')
    
    p = doc.add_paragraph()
    safe_add_run(p, "INSPECTION SCOPE & RESULTS:\n", bold=True)
    safe_add_run(p, f"• Total Residential Units Evaluated: {units_inspected}\n")
    safe_add_run(p, f"• Total Building Components Inspected: {df['component'].nunique()}\n")
    safe_add_run(p, f"• Defects Identified: {total_defects}\n")
    safe_add_run(p, f"• Defect Rate: {total_defects / units_inspected:.1f} defects per unit\n")
    
    doc.add_paragraph()
    
    doc.add_paragraph("REPORT GENERATION & COMPANION RESOURCES", style='Heading 2')
    
    p = doc.add_paragraph()
    safe_add_run(p, "REPORT METADATA:\n", bold=True)
    safe_add_run(p, f"• Report Generated: {datetime.now().strftime('%d %B %Y at %I:%M %p')}\n")
    safe_add_run(p, f"• Inspection Coverage: {inspection_date_range}\n")
    safe_add_run(p, "• Report Type: Executive Summary - Multi-Inspection Analysis\n")
    safe_add_run(p, "• Data Source: SafetyCulture API Integration\n")
    
    doc.add_paragraph()
    
    # Footer
    add_professional_footer(doc, building_name)


def add_professional_footer(doc, building_name):
    """Add professional footer to document"""
    section = doc.sections[-1]
    footer = section.footer
    
    # Clear existing
    for paragraph in footer.paragraphs:
        paragraph.clear()
    
    p = footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    
    safe_add_run(p, f"{building_name} | Pre-Settlement Inspection Report | ", font_size=8)
    safe_add_run(p, f"Generated {datetime.now().strftime('%d/%m/%Y')}", font_size=8, italic=True)


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT - Automatic Report Type Selection
# ═══════════════════════════════════════════════════════════════════

def create_word_report_from_database(
    inspection_ids: List[str],
    db_connection,
    api_key: str,
    output_path: str,
    report_type: str = "single",
    images: Dict = None
) -> bool:
    """
    Generate Word report with automatic style selection
    
    Args:
        inspection_ids: List of inspection IDs
        db_connection: Database connection (psycopg2)
        api_key: SafetyCulture API key
        output_path: Where to save the report
        report_type: "single" or "multi"
        images: Optional dict with 'logo' and 'cover' image paths
    
    Returns:
        True if successful, False otherwise
    """
    
    try:
        print(f"Generating Word report (type: {report_type})...")
        
        # Fetch data from database
        cursor = db_connection.cursor()
        
        # Get building name and date range
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
        
        print(f"Generating professional Word report for {building_name}...")
        
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
            print("No defects found for selected inspections")
            return False
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=[
            'item_id', 'unit', 'room', 'component', 'issue', 
            'severity', 'trade', 'photo_url', 'notes'
        ])
        
        print(f"Found {len(df)} defects to process")
        
        # Create document
        doc = Document()
        
        # Set margins
        for section in doc.sections:
            section.top_margin = Inches(1.0)
            section.bottom_margin = Inches(1.0)
            section.left_margin = Inches(1.0)
            section.right_margin = Inches(1.0)
        
        # Generate appropriate report type
        if report_type == "single" or len(inspection_ids) == 1:
            print("Generating DETAILED report with individual defects and photos...")
            generate_single_inspection_report(doc, df, building_name, api_key, images)
        else:
            print("Generating EXECUTIVE SUMMARY report with charts and analytics...")
            generate_multi_inspection_summary(doc, df, building_name, date_range, images)
        
        # Save document
        doc.save(output_path)
        print(f"✅ Word report saved to: {output_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating Word report: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Word Generator API - Dual Report System")
    print("This module provides:")
    print("- Single inspection: Detailed reports with photos")
    print("- Multiple inspections: Executive summaries with charts")
    print("- Logo and cover image support")
    print("- XML text sanitization")