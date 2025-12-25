"""
Professional Word Report Generator for API Inspections
Building Inspection System V3 - Essential Community Management (ECM)

Redesigned to match CSV template professional design while maintaining API functionality:
- Fetches data from PostgreSQL database
- Downloads photos from SafetyCulture API
- Professional cover page with building photo
- Company logo in header
- Executive summary with metrics
- Color-coded severity levels
- Professional table styling
- Charts and graphs
- Trade-specific analysis
- Component breakdown
- Strategic recommendations
- Photo integration at 5" width
"""

import os
import io
import json
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from io import BytesIO
from PIL import Image
from docx import Document
from docx.shared import Inches, Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import parse_xml
from docx.enum.section import WD_SECTION
import pandas as pd

# Safe imports for visualization
try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')  # Non-GUI backend
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None

try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    SEABORN_AVAILABLE = False
    sns = None

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None


class WordGeneratorAPI:
    """Generate professional Word reports for API inspections"""
    
    def __init__(self, api_key: str):
        """
        Initialize the Word generator
        
        Args:
            api_key: SafetyCulture API key for downloading photos
        """
        self.api_key = api_key
        self.photo_cache = {}
    
    # ==================== PHOTO HANDLING ====================
    
    def download_photo(self, photo_url: str) -> Optional[BytesIO]:
        """Download photo from SafetyCulture API with caching"""
        if photo_url in self.photo_cache:
            cached_bytes = self.photo_cache[photo_url]
            cached_bytes.seek(0)
            return BytesIO(cached_bytes.read())
        
        try:
            headers = {'Authorization': f'Bearer {self.api_key}'}
            response = requests.get(photo_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                img_bytes = BytesIO(response.content)
                self.photo_cache[photo_url] = BytesIO(response.content)
                img_bytes.seek(0)
                return img_bytes
            else:
                print(f"Failed to download photo: {photo_url} (Status: {response.status_code})")
                return None
        except Exception as e:
            print(f"Error downloading photo: {str(e)}")
            return None
    
    def calculate_image_dimensions(self, img_bytes: BytesIO, target_width: float = 5.0) -> tuple:
        """Calculate image dimensions maintaining aspect ratio"""
        try:
            img_bytes.seek(0)
            img = Image.open(img_bytes)
            original_width, original_height = img.size
            aspect_ratio = original_height / original_width
            target_height = target_width * aspect_ratio
            return (target_width, target_height)
        except Exception as e:
            print(f"Error calculating dimensions: {str(e)}")
            return (target_width, target_width)
    
    # ==================== DOCUMENT SETUP ====================
    
    def setup_document_formatting(self, doc: Document):
        """Setup document formatting with Arial font and professional styling"""
        sections = doc.sections
        for section in sections:
            section.top_margin = Cm(3.0)
            section.bottom_margin = Cm(2.5)
            section.left_margin = Cm(2.5)
            section.right_margin = Cm(2.5)
        
        styles = doc.styles
        
        # Title style
        if 'CleanTitle' not in [s.name for s in styles]:
            title_style = styles.add_style('CleanTitle', 1)
            title_font = title_style.font
            title_font.name = 'Arial'
            title_font.size = Pt(28)
            title_font.bold = True
            title_font.color.rgb = RGBColor(0, 0, 0)
            title_style.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
            title_style.paragraph_format.space_after = Pt(12)
            title_style.paragraph_format.space_before = Pt(10)
        
        # Section header
        if 'CleanSectionHeader' not in [s.name for s in styles]:
            section_style = styles.add_style('CleanSectionHeader', 1)
            section_font = section_style.font
            section_font.name = 'Arial'
            section_font.size = Pt(18)
            section_font.bold = True
            section_font.color.rgb = RGBColor(0, 0, 0)
            section_style.paragraph_format.space_before = Pt(20)
            section_style.paragraph_format.space_after = Pt(10)
        
        # Subsection header
        if 'CleanSubsectionHeader' not in [s.name for s in styles]:
            subsection_style = styles.add_style('CleanSubsectionHeader', 1)
            subsection_font = subsection_style.font
            subsection_font.name = 'Arial'
            subsection_font.size = Pt(14)
            subsection_font.bold = True
            subsection_font.color.rgb = RGBColor(0, 0, 0)
            subsection_style.paragraph_format.space_before = Pt(16)
            subsection_style.paragraph_format.space_after = Pt(8)
        
        # Body text
        if 'CleanBody' not in [s.name for s in styles]:
            body_style = styles.add_style('CleanBody', 1)
            body_font = body_style.font
            body_font.name = 'Arial'
            body_font.size = Pt(11)
            body_font.color.rgb = RGBColor(0, 0, 0)
            body_style.paragraph_format.line_spacing = 1.2
            body_style.paragraph_format.space_after = Pt(6)
    
    def add_logo_to_header(self, doc: Document, images: Dict = None):
        """Add company logo to document header"""
        try:
            if images and images.get('logo') and os.path.exists(images['logo']):
                section = doc.sections[0]
                header = section.header
                header.paragraphs[0].clear()
                
                header_para = header.paragraphs[0]
                header_para.alignment = WD_ALIGN_PARAGRAPH.LEFT
                header_run = header_para.add_run()
                header_run.add_picture(images['logo'], width=Inches(2.0))
        except Exception as e:
            print(f"Error adding logo: {e}")
    
    def add_formatted_text_with_bold(self, paragraph, text: str):
        """Convert **text** to bold formatting"""
        parts = text.split('**')
        for i, part in enumerate(parts):
            run = paragraph.add_run(part)
            run.font.name = 'Arial'
            run.font.size = Pt(11)
            run.font.color.rgb = RGBColor(0, 0, 0)
            if i % 2 == 1:
                run.font.bold = True
    
    # ==================== COVER PAGE ====================
    
    def add_cover_page(self, doc: Document, metrics: Dict, images: Dict = None):
        """Add professional cover page"""
        try:
            # Title - split into 2 lines
            title_para = doc.add_paragraph()
            title_para.style = 'CleanTitle'
            title_run = title_para.add_run("PRE-SETTLEMENT\nINSPECTION REPORT")
            title_run.font.name = 'Arial'
            title_run.font.size = Pt(28)
            title_run.font.bold = True
            title_run.font.color.rgb = RGBColor(0, 0, 0)
            
            doc.add_paragraph()
            
            # Building name
            building_para = doc.add_paragraph()
            building_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            building_run = building_para.add_run(metrics.get('building_name', 'Building Name'))
            building_run.font.name = 'Arial'
            building_run.font.size = Pt(16)
            building_run.font.color.rgb = RGBColor(0, 0, 0)
            
            doc.add_paragraph()
            
            # Cover image if available
            if images and images.get('cover') and os.path.exists(images['cover']):
                cover_para = doc.add_paragraph()
                cover_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
                cover_run = cover_para.add_run()
                cover_run.add_picture(images['cover'], width=Inches(4.7))
                doc.add_paragraph()
            
            # Inspection date
            date_para = doc.add_paragraph()
            date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            try:
                if isinstance(metrics.get('inspection_date'), str):
                    date_obj = datetime.strptime(metrics['inspection_date'], '%Y-%m-%d')
                    date_display = date_obj.strftime('%d %B %Y')
                else:
                    date_display = metrics.get('inspection_date', 'Date')
            except:
                date_display = metrics.get('inspection_date', 'Date')
            
            date_run = date_para.add_run(f"Inspection Date: {date_display}")
            date_run.font.name = 'Arial'
            date_run.font.size = Pt(12)
            date_run.font.color.rgb = RGBColor(0, 0, 0)
            
            # Page break
            doc.add_page_break()
            
        except Exception as e:
            print(f"Error in cover page: {e}")
    
    # ==================== EXECUTIVE OVERVIEW ====================
    
    def add_executive_overview(self, doc: Document, metrics: Dict):
        """Add executive overview section with metrics"""
        try:
            section_header = doc.add_paragraph("EXECUTIVE OVERVIEW")
            section_header.style = 'CleanSectionHeader'
            
            # Overview paragraph
            overview_text = f"""This comprehensive pre-settlement inspection report documents the condition assessment of **{metrics.get('building_name', 'the building')}**. The inspection identified **{metrics.get('total_defects', 0)} defects** requiring attention across **{metrics.get('total_units', 0)} units**."""
            
            overview_para = doc.add_paragraph()
            self.add_formatted_text_with_bold(overview_para, overview_text)
            
            doc.add_paragraph()
            
            # Key metrics table
            metrics_header = doc.add_paragraph("Key Inspection Metrics")
            metrics_header.style = 'CleanSubsectionHeader'
            
            table = doc.add_table(rows=6, cols=2)
            table.style = 'Light Grid'
            
            # Add shading to header row
            for cell in table.rows[0].cells:
                cell_xml = parse_xml(r'<w:shd {} w:fill="D9D9D9"/>'.format(qn('w:shd')))
                cell._element.get_or_add_tcPr().append(cell_xml)
            
            metrics_data = [
                ('Total Units Inspected', str(metrics.get('total_units', 0))),
                ('Total Defects Identified', str(metrics.get('total_defects', 0))),
                ('Average Defects per Unit', f"{metrics.get('avg_defects_per_unit', 0):.1f}"),
                ('Units Requiring Urgent Attention', str(metrics.get('urgent_units', 0))),
                ('Trade Categories Involved', str(metrics.get('total_trades', 0))),
                ('Inspection Date', metrics.get('inspection_date', 'N/A'))
            ]
            
            for idx, (label, value) in enumerate(metrics_data):
                row = table.rows[idx]
                row.cells[0].text = label
                row.cells[1].text = value
                
                # Bold labels
                row.cells[0].paragraphs[0].runs[0].font.bold = True
                row.cells[0].paragraphs[0].runs[0].font.name = 'Arial'
                row.cells[1].paragraphs[0].runs[0].font.name = 'Arial'
            
            doc.add_paragraph()
            
        except Exception as e:
            print(f"Error in executive overview: {e}")
    
    # ==================== SEVERITY ANALYSIS ====================
    
    def add_severity_analysis(self, doc: Document, metrics: Dict):
        """Add severity breakdown section"""
        try:
            section_header = doc.add_paragraph("DEFECT SEVERITY ANALYSIS")
            section_header.style = 'CleanSectionHeader'
            
            # Severity distribution table
            severity_data = metrics.get('severity_breakdown', {})
            if severity_data:
                table = doc.add_table(rows=4, cols=3)
                table.style = 'Light Grid'
                
                # Headers
                headers = ['Severity Level', 'Count', 'Percentage']
                for idx, header in enumerate(headers):
                    cell = table.rows[0].cells[idx]
                    cell.text = header
                    cell.paragraphs[0].runs[0].font.bold = True
                    cell_xml = parse_xml(r'<w:shd {} w:fill="D9D9D9"/>'.format(qn('w:shd')))
                    cell._element.get_or_add_tcPr().append(cell_xml)
                
                # Data rows
                total = metrics.get('total_defects', 0)
                severity_levels = [
                    ('Urgent', severity_data.get('Urgent', 0), RGBColor(192, 0, 0)),
                    ('High Priority', severity_data.get('High Priority', 0), RGBColor(255, 153, 0)),
                    ('Normal', severity_data.get('Normal', 0), RGBColor(0, 128, 0))
                ]
                
                for idx, (level, count, color) in enumerate(severity_levels, 1):
                    row = table.rows[idx]
                    row.cells[0].text = level
                    row.cells[1].text = str(count)
                    percentage = (count / total * 100) if total > 0 else 0
                    row.cells[2].text = f"{percentage:.1f}%"
                    
                    # Color code severity level
                    row.cells[0].paragraphs[0].runs[0].font.color.rgb = color
                    row.cells[0].paragraphs[0].runs[0].font.bold = True
                
                doc.add_paragraph()
            
        except Exception as e:
            print(f"Error in severity analysis: {e}")
    
    # ==================== TRADE ANALYSIS ====================
    
    def add_trade_analysis(self, doc: Document, metrics: Dict):
        """Add trade-specific analysis section"""
        try:
            section_header = doc.add_paragraph("TRADE-SPECIFIC ANALYSIS")
            section_header.style = 'CleanSectionHeader'
            
            trade_summary = metrics.get('trade_summary', [])
            if trade_summary:
                # Create DataFrame
                df = pd.DataFrame(trade_summary)
                
                # Trade distribution table
                table = doc.add_table(rows=len(df) + 1, cols=3)
                table.style = 'Light Grid'
                
                # Headers
                headers = ['Trade Category', 'Defect Count', 'Percentage']
                for idx, header in enumerate(headers):
                    cell = table.rows[0].cells[idx]
                    cell.text = header
                    cell.paragraphs[0].runs[0].font.bold = True
                    cell_xml = parse_xml(r'<w:shd {} w:fill="D9D9D9"/>'.format(qn('w:shd')))
                    cell._element.get_or_add_tcPr().append(cell_xml)
                
                # Data rows
                total_defects = metrics.get('total_defects', 0)
                for idx, row_data in enumerate(df.itertuples(), 1):
                    row = table.rows[idx]
                    trade = row_data.trade
                    count = row_data.count
                    percentage = (count / total_defects * 100) if total_defects > 0 else 0
                    
                    row.cells[0].text = trade
                    row.cells[1].text = str(count)
                    row.cells[2].text = f"{percentage:.1f}%"
                    
                    # Alternating row shading
                    if idx % 2 == 0:
                        for cell in row.cells:
                            cell_xml = parse_xml(r'<w:shd {} w:fill="F2F2F2"/>'.format(qn('w:shd')))
                            cell._element.get_or_add_tcPr().append(cell_xml)
                
                doc.add_paragraph()
                
                # Add chart if matplotlib available
                if MATPLOTLIB_AVAILABLE and len(df) > 0:
                    self.add_trade_chart(doc, df, total_defects)
            
        except Exception as e:
            print(f"Error in trade analysis: {e}")
    
    def add_trade_chart(self, doc: Document, df: pd.DataFrame, total_defects: int):
        """Add trade distribution chart"""
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Create bar chart
            trades = df['trade'].head(10)
            counts = df['count'].head(10)
            
            bars = ax.barh(trades, counts, color='#4472C4')
            ax.set_xlabel('Number of Defects', fontsize=12)
            ax.set_title('Top 10 Trade Categories by Defect Count', fontsize=14, fontweight='bold')
            ax.invert_yaxis()
            
            # Add value labels
            for bar in bars:
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2, 
                       f'{int(width)}', ha='left', va='center', fontsize=10)
            
            # Save and add to document
            chart_buffer = BytesIO()
            fig.savefig(chart_buffer, format='png', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            chart_buffer.seek(0)
            plt.close(fig)
            
            chart_para = doc.add_paragraph()
            chart_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            chart_run = chart_para.add_run()
            chart_run.add_picture(chart_buffer, width=Inches(6.5))
            
            doc.add_paragraph()
            
        except Exception as e:
            print(f"Error adding trade chart: {e}")
    
    # ==================== DETAILED DEFECTS ====================
    
    def add_detailed_defects(self, doc: Document, defects: List[Dict], metrics: Dict):
        """Add detailed defects section with photos"""
        try:
            section_header = doc.add_paragraph("DETAILED DEFECT DOCUMENTATION")
            section_header.style = 'CleanSectionHeader'
            
            intro_text = f"""The following section provides comprehensive documentation of all **{len(defects)} defects** identified during the inspection. Each defect includes location details, trade classification, severity assessment, and photographic evidence where available."""
            
            intro_para = doc.add_paragraph()
            self.add_formatted_text_with_bold(intro_para, intro_text)
            
            doc.add_paragraph()
            
            # Add each defect
            for idx, defect in enumerate(defects, 1):
                self.add_defect_detail(doc, defect, idx, len(defects))
            
        except Exception as e:
            print(f"Error in detailed defects: {e}")
    
    def add_defect_detail(self, doc: Document, defect: Dict, defect_num: int, total: int):
        """Add individual defect detail with professional formatting"""
        try:
            # Defect heading
            heading = doc.add_paragraph(f"Defect {defect_num} of {total}")
            heading.style = 'CleanSubsectionHeader'
            
            # Defect details table
            table = doc.add_table(rows=7, cols=2)
            table.style = 'Light List'
            
            table.columns[0].width = Inches(1.8)
            table.columns[1].width = Inches(4.7)
            
            # Get severity/priority
            priority = defect.get('priority') or defect.get('urgency') or 'Normal'
            
            details = [
                ('Location (Room)', str(defect.get('room') or 'N/A')),
                ('Component', str(defect.get('component') or 'N/A')),
                ('Description', str(defect.get('description') or defect.get('notes') or 'N/A')),
                ('Trade Category', str(defect.get('trade') or 'N/A')),
                ('Severity Level', priority),
                ('Current Status', str(defect.get('status') or defect.get('status_class') or 'Open')),
                ('Inspector Notes', str(defect.get('inspector_notes') or 'None'))
            ]
            
            for idx, (label, value) in enumerate(details):
                row = table.rows[idx]
                row.cells[0].text = label
                row.cells[0].paragraphs[0].runs[0].font.bold = True
                row.cells[0].paragraphs[0].runs[0].font.name = 'Arial'
                row.cells[1].text = value
                row.cells[1].paragraphs[0].runs[0].font.name = 'Arial'
                
                # Color code severity
                if label == 'Severity Level':
                    cell = row.cells[1]
                    if 'Urgent' in value or 'High' in value:
                        cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(192, 0, 0)
                        cell.paragraphs[0].runs[0].font.bold = True
                    elif 'Medium' in value or 'Priority' in value:
                        cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(255, 153, 0)
                        cell.paragraphs[0].runs[0].font.bold = True
                    elif 'Normal' in value or 'Low' in value:
                        cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(0, 128, 0)
                
                # Alternating row shading
                if idx % 2 == 1:
                    for cell in row.cells:
                        cell_xml = parse_xml(r'<w:shd {} w:fill="F2F2F2"/>'.format(qn('w:shd')))
                        cell._element.get_or_add_tcPr().append(cell_xml)
            
            # Add photo if available
            photo_url = defect.get('photo_url')
            if photo_url:
                doc.add_paragraph()
                img_bytes = self.download_photo(photo_url)
                
                if img_bytes:
                    try:
                        width, height = self.calculate_image_dimensions(img_bytes, target_width=5.0)
                        img_bytes.seek(0)
                        
                        photo_para = doc.add_paragraph()
                        photo_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
                        photo_run = photo_para.add_run()
                        photo_run.add_picture(img_bytes, width=Inches(width), height=Inches(height))
                        
                        # Caption
                        caption_para = doc.add_paragraph()
                        caption_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
                        caption_text = f"Photo {defect_num}: {defect.get('room', 'Location')} - {defect.get('component', 'Component')}"
                        caption_run = caption_para.add_run(caption_text)
                        caption_run.font.name = 'Arial'
                        caption_run.font.size = Pt(10)
                        caption_run.font.italic = True
                        caption_run.font.color.rgb = RGBColor(64, 64, 64)
                        
                    except Exception as e:
                        print(f"Error embedding photo {defect_num}: {e}")
            
            doc.add_paragraph()
            
        except Exception as e:
            print(f"Error in defect detail {defect_num}: {e}")
    
    # ==================== RECOMMENDATIONS ====================
    
    def add_recommendations(self, doc: Document, metrics: Dict):
        """Add strategic recommendations section"""
        try:
            section_header = doc.add_paragraph("STRATEGIC RECOMMENDATIONS")
            section_header.style = 'CleanSectionHeader'
            
            urgent_count = metrics.get('severity_breakdown', {}).get('Urgent', 0)
            high_count = metrics.get('severity_breakdown', {}).get('High Priority', 0)
            total_defects = metrics.get('total_defects', 0)
            
            recommendations = []
            
            if urgent_count > 0:
                recommendations.append(
                    f"**IMMEDIATE ACTION REQUIRED**: {urgent_count} urgent defects identified requiring immediate remediation before settlement."
                )
            
            if high_count > 0:
                recommendations.append(
                    f"**HIGH PRIORITY**: {high_count} high-priority defects should be addressed within 7-14 days."
                )
            
            if total_defects > 50:
                recommendations.append(
                    "**RESOURCE ALLOCATION**: Consider engaging multiple trade teams simultaneously to accelerate remediation timeline."
                )
            
            recommendations.append(
                "**QUALITY ASSURANCE**: Schedule re-inspection upon completion of remediation works to verify all defects have been properly addressed."
            )
            
            recommendations.append(
                "**DOCUMENTATION**: Maintain comprehensive photographic records of completed remediation works for compliance and warranty purposes."
            )
            
            for rec in recommendations:
                rec_para = doc.add_paragraph()
                self.add_formatted_text_with_bold(rec_para, f"• {rec}")
                rec_para.paragraph_format.left_indent = Inches(0.3)
            
            doc.add_paragraph()
            
        except Exception as e:
            print(f"Error in recommendations: {e}")
    
    # ==================== FOOTER ====================
    
    def add_footer(self, doc: Document, metrics: Dict):
        """Add professional footer"""
        try:
            doc.add_page_break()
            
            footer_header = doc.add_paragraph("REPORT INFORMATION")
            footer_header.style = 'CleanSectionHeader'
            
            # Report metadata
            try:
                if isinstance(metrics.get('inspection_date'), str):
                    date_obj = datetime.strptime(metrics['inspection_date'], '%Y-%m-%d')
                    inspection_display = date_obj.strftime('%d %B %Y')
                else:
                    inspection_display = metrics.get('inspection_date', 'N/A')
            except:
                inspection_display = metrics.get('inspection_date', 'N/A')
            
            details_text = f"""**REPORT METADATA**:
• Report Generated: {datetime.now().strftime('%d %B %Y at %I:%M %p')}
• Inspection Completion: {inspection_display}
• Building Development: {metrics.get('building_name', 'N/A')}
• Property Location: {metrics.get('address', 'N/A')}
• Inspector: {metrics.get('inspector_name', 'N/A')}

**COMPANION DOCUMENTATION**:
Complete defect inventories, unit-by-unit detailed breakdowns, interactive filterable data tables, and comprehensive photographic documentation are available in the accompanying Excel analytics workbook.

**TECHNICAL SUPPORT**:
For technical inquiries, data interpretation assistance, or additional analysis requirements, please contact the inspection team."""
            
            details_para = doc.add_paragraph()
            self.add_formatted_text_with_bold(details_para, details_text)
            
            doc.add_paragraph()
            
            # Closing
            closing_para = doc.add_paragraph()
            closing_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            closing_run = closing_para.add_run("END OF REPORT")
            closing_run.font.name = 'Arial'
            closing_run.font.size = Pt(14)
            closing_run.font.bold = True
            closing_run.font.color.rgb = RGBColor(0, 0, 0)
            
        except Exception as e:
            print(f"Error in footer: {e}")
    
    # ==================== MAIN GENERATION METHODS ====================
    
    def generate_single_inspection_report(
        self,
        inspection_data: Dict[str, Any],
        defects: List[Dict[str, Any]],
        output_path: str,
        images: Dict = None
    ) -> bool:
        """
        Generate professional Word report for single inspection
        
        Args:
            inspection_data: Dictionary with inspection metadata
            defects: List of defect dictionaries
            output_path: Where to save the report
            images: Optional dict with 'logo' and 'cover' image paths
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"Generating professional Word report for {inspection_data.get('building_name', 'Unknown')}...")
            
            # Create document
            doc = Document()
            self.setup_document_formatting(doc)
            
            # Calculate metrics
            metrics = self._calculate_metrics(inspection_data, defects)
            
            # Add logo to header
            if images:
                self.add_logo_to_header(doc, images)
            
            # Build report sections
            self.add_cover_page(doc, metrics, images)
            self.add_executive_overview(doc, metrics)
            self.add_severity_analysis(doc, metrics)
            self.add_trade_analysis(doc, metrics)
            
            doc.add_page_break()
            self.add_detailed_defects(doc, defects, metrics)
            
            self.add_recommendations(doc, metrics)
            self.add_footer(doc, metrics)
            
            # Save
            doc.save(output_path)
            print(f"✅ Professional Word report saved: {output_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error generating Word report: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def generate_multi_inspection_report(
        self,
        inspections: List[Dict[str, Any]],
        output_path: str,
        images: Dict = None
    ) -> bool:
        """
        Generate professional Word report for multiple inspections
        
        Args:
            inspections: List of dicts with 'inspection_data' and 'defects'
            output_path: Where to save the report
            images: Optional dict with 'logo' and 'cover' image paths
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"Generating multi-inspection Word report for {len(inspections)} inspections...")
            
            doc = Document()
            self.setup_document_formatting(doc)
            
            # Add logo
            if images:
                self.add_logo_to_header(doc, images)
            
            # Summary cover page
            all_defects = []
            for insp in inspections:
                all_defects.extend(insp['defects'])
            
            combined_metrics = {
                'building_name': f"Multi-Building Report ({len(inspections)} Buildings)",
                'total_defects': len(all_defects),
                'total_units': sum(insp['inspection_data'].get('total_defects', 0) for insp in inspections),
                'inspection_date': inspections[0]['inspection_data'].get('inspection_date', 'N/A')
            }
            
            self.add_cover_page(doc, combined_metrics, images)
            
            # Add each inspection
            for idx, inspection in enumerate(inspections, 1):
                data = inspection['inspection_data']
                defects = inspection['defects']
                
                metrics = self._calculate_metrics(data, defects)
                
                # Inspection section
                section_title = doc.add_paragraph(f"INSPECTION {idx}: {data.get('building_name', 'Unknown')}")
                section_title.style = 'CleanSectionHeader'
                
                self.add_executive_overview(doc, metrics)
                self.add_severity_analysis(doc, metrics)
                self.add_trade_analysis(doc, metrics)
                
                doc.add_page_break()
                self.add_detailed_defects(doc, defects, metrics)
                
                if idx < len(inspections):
                    doc.add_page_break()
            
            # Final recommendations and footer
            self.add_recommendations(doc, combined_metrics)
            self.add_footer(doc, combined_metrics)
            
            doc.save(output_path)
            print(f"✅ Multi-inspection Word report saved: {output_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error generating multi-inspection report: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _calculate_metrics(self, inspection_data: Dict, defects: List[Dict]) -> Dict:
        """Calculate report metrics from inspection data and defects"""
        try:
            # Severity breakdown
            severity_breakdown = {}
            for defect in defects:
                priority = defect.get('priority') or defect.get('urgency') or 'Normal'
                severity_breakdown[priority] = severity_breakdown.get(priority, 0) + 1
            
            # Trade summary
            trade_counts = {}
            for defect in defects:
                trade = defect.get('trade', 'Unknown')
                trade_counts[trade] = trade_counts.get(trade, 0) + 1
            
            trade_summary = [
                {'trade': trade, 'count': count}
                for trade, count in sorted(trade_counts.items(), key=lambda x: x[1], reverse=True)
            ]
            
            # Compile metrics
            metrics = {
                'building_name': inspection_data.get('building_name', 'N/A'),
                'inspection_date': inspection_data.get('inspection_date', 'N/A'),
                'inspector_name': inspection_data.get('inspector_name', 'N/A'),
                'address': inspection_data.get('address', 'N/A'),
                'total_defects': len(defects),
                'total_units': inspection_data.get('total_units', 1),
                'avg_defects_per_unit': len(defects) / max(inspection_data.get('total_units', 1), 1),
                'urgent_units': severity_breakdown.get('Urgent', 0),
                'total_trades': len(trade_counts),
                'severity_breakdown': severity_breakdown,
                'trade_summary': trade_summary
            }
            
            return metrics
            
        except Exception as e:
            print(f"Error calculating metrics: {e}")
            return {}


# ==================== DATABASE INTEGRATION ====================

def create_word_report_from_database(
    inspection_ids: List[int],
    db_connection,
    api_key: str,
    output_path: str,
    report_type: str = "single",
    images: Dict = None
) -> bool:
    """
    Main function to create Word report from database data
    
    Args:
        inspection_ids: List of inspection IDs to include
        db_connection: Database connection object
        api_key: SafetyCulture API key
        output_path: Where to save the Word file
        report_type: "single" or "multi" inspection report
        images: Optional dict with 'logo' and 'cover' image paths
        
    Returns:
        True if successful, False otherwise
    """
    try:
        generator = WordGeneratorAPI(api_key)
        
        if report_type == "single" and len(inspection_ids) == 1:
            inspection_data, defects = _query_inspection_data(db_connection, inspection_ids[0])
            return generator.generate_single_inspection_report(
                inspection_data, defects, output_path, images
            )
        
        elif report_type == "multi":
            inspections = []
            for inspection_id in inspection_ids:
                inspection_data, defects = _query_inspection_data(db_connection, inspection_id)
                inspections.append({
                    'inspection_data': inspection_data,
                    'defects': defects
                })
            return generator.generate_multi_inspection_report(inspections, output_path, images)
        
        else:
            print(f"Invalid report type or inspection count: {report_type}, {len(inspection_ids)} inspections")
            return False
            
    except Exception as e:
        print(f"Error creating Word report: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def _query_inspection_data(db_connection, inspection_id: int) -> tuple:
    """
    Query inspection data and defects from database
    
    Args:
        db_connection: Database connection
        inspection_id: ID of the inspection
        
    Returns:
        Tuple of (inspection_data dict, defects list)
    """
    cursor = db_connection.cursor()
    
    # Query inspection metadata
    cursor.execute("""
        SELECT i.id, i.inspection_date, i.inspector_name, i.total_defects,
               b.name as building_name, b.address
        FROM inspector_inspections i
        JOIN inspector_buildings b ON i.building_id = b.id
        WHERE i.id = %s
    """, (inspection_id,))
    
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Inspection {inspection_id} not found")
    
    inspection_data = {
        'id': row[0],
        'inspection_date': row[1].strftime('%Y-%m-%d') if row[1] else 'N/A',
        'inspector_name': row[2] or 'N/A',
        'total_defects': row[3],
        'building_name': row[4],
        'address': row[5] or 'N/A',
        'total_units': 1  # Can be queried if available
    }
    
    # Query defects
    cursor.execute("""
        SELECT room, component, notes, trade, urgency, status_class,
            photo_url, photo_media_id, inspector_notes
        FROM inspector_inspection_items
        WHERE inspection_id = %s
        ORDER BY room, component
    """, (inspection_id,))
    
    defects = []
    for row in cursor.fetchall():
        defects.append({
            'room': row[0],
            'component': row[1],
            'description': row[2],
            'notes': row[2],  # Duplicate for compatibility
            'trade': row[3],
            'priority': row[4],
            'urgency': row[4],  # Duplicate for compatibility
            'status': row[5],
            'status_class': row[5],  # Duplicate for compatibility
            'photo_url': row[6],
            'photo_media_id': row[7],
            'inspector_notes': row[8]
        })
    
    cursor.close()
    return inspection_data, defects


if __name__ == "__main__":
    print("Professional Word Report Generator for API Inspections - REDESIGNED")
    print("=" * 70)
    print("\n✅ FEATURES:")
    print("  • Professional cover page with building photo")
    print("  • Company logo in header")
    print("  • Executive overview with metrics tables")
    print("  • Color-coded severity analysis")
    print("  • Trade-specific breakdown with charts")
    print("  • Detailed defect documentation with photos")
    print("  • Strategic recommendations")
    print("  • Professional formatting (Arial, clean layout)")
    print("  • Photos embedded at 5\" width with captions")
    print("\n✅ API FUNCTIONALITY:")
    print("  • Fetches data from PostgreSQL database")
    print("  • Downloads photos from SafetyCulture API")
    print("  • Supports single and multi-inspection reports")
    print("  • Photo caching for performance")
    print("\n📦 DEPENDENCIES:")
    print(f"  • matplotlib: {'✅ Available' if MATPLOTLIB_AVAILABLE else '❌ Not Available (charts will be skipped)'}")
    print(f"  • seaborn: {'✅ Available' if SEABORN_AVAILABLE else '❌ Not Available'}")
    print(f"  • numpy: {'✅ Available' if NUMPY_AVAILABLE else '❌ Not Available'}")
    print("\n🚀 READY FOR PRODUCTION!")