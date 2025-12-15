"""
Word Report Generator for API Inspections with Photo Support
Building Inspection System V3 - Essential Community Management (ECM)

Features:
- Inspector notes inline in defect sections
- Full-size photos (5 inches wide, maintain aspect ratio)
- Downloads photos from SafetyCulture API
- Inserts using doc.add_picture()
- Captions: "Photo X: Room - Component"
"""

import os
import io
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from io import BytesIO
from PIL import Image
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE


class WordGeneratorAPI:
    """Generate Word reports for API inspections with photo support"""
    
    def __init__(self, api_key: str):
        """
        Initialize the Word generator
        
        Args:
            api_key: SafetyCulture API key for downloading photos
        """
        self.api_key = api_key
        self.photo_cache = {}  # Cache downloaded photos
    
    def download_photo(self, photo_url: str) -> Optional[BytesIO]:
        """
        Download a photo from SafetyCulture API
        
        Args:
            photo_url: URL to the photo
            
        Returns:
            BytesIO object containing image data or None if download fails
        """
        # Check cache first
        if photo_url in self.photo_cache:
            # Return a fresh BytesIO copy from cache
            cached_bytes = self.photo_cache[photo_url]
            cached_bytes.seek(0)
            return BytesIO(cached_bytes.read())
        
        try:
            headers = {'Authorization': f'Bearer {self.api_key}'}
            response = requests.get(photo_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                img_bytes = BytesIO(response.content)
                # Cache the bytes
                self.photo_cache[photo_url] = BytesIO(response.content)
                img_bytes.seek(0)
                return img_bytes
            else:
                print(f"Failed to download photo: {photo_url} (Status: {response.status_code})")
                return None
                
        except Exception as e:
            print(f"Error downloading photo from {photo_url}: {str(e)}")
            return None
    
    def calculate_image_dimensions(self, img_bytes: BytesIO, target_width: float = 5.0) -> tuple:
        """
        Calculate image dimensions maintaining aspect ratio
        
        Args:
            img_bytes: BytesIO containing image data
            target_width: Target width in inches
            
        Returns:
            Tuple of (width_inches, height_inches)
        """
        try:
            img_bytes.seek(0)
            img = Image.open(img_bytes)
            original_width, original_height = img.size
            
            # Calculate height maintaining aspect ratio
            aspect_ratio = original_height / original_width
            target_height = target_width * aspect_ratio
            
            return (target_width, target_height)
        except Exception as e:
            print(f"Error calculating image dimensions: {str(e)}")
            return (target_width, target_width)  # Fallback to square
    
    def add_header(self, doc: Document, inspection_data: Dict[str, Any]):
        """Add header section to document"""
        # Title
        title = doc.add_heading('BUILDING INSPECTION REPORT', level=0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Inspection details table
        doc.add_paragraph()
        table = doc.add_table(rows=6, cols=2)
        table.style = 'Light Grid Accent 1'
        
        # Set column widths
        table.columns[0].width = Inches(2.0)
        table.columns[1].width = Inches(4.5)
        
        # Fill in details
        details = [
            ('Building:', inspection_data.get('building_name', 'N/A')),
            ('Inspection Date:', inspection_data.get('inspection_date', 'N/A')),
            ('Inspector:', inspection_data.get('inspector_name', 'N/A')),
            ('Total Defects:', str(inspection_data.get('total_defects', 0))),
            ('Defects with Photos:', str(inspection_data.get('photo_count', 0))),
            ('Defects with Notes:', str(inspection_data.get('note_count', 0)))
        ]
        
        for idx, (label, value) in enumerate(details):
            row = table.rows[idx]
            row.cells[0].text = label
            row.cells[0].paragraphs[0].runs[0].font.bold = True
            row.cells[1].text = value
        
        doc.add_paragraph()
    
    def add_defect_section(
        self,
        doc: Document,
        defect: Dict[str, Any],
        defect_num: int,
        total_defects: int
    ):
        """
        Add a defect section to the document
        
        Args:
            doc: Document object
            defect: Defect data dictionary
            defect_num: Current defect number
            total_defects: Total number of defects
        """
        # Defect number heading
        heading = doc.add_heading(f'Defect {defect_num} of {total_defects}', level=2)
        heading.runs[0].font.color.rgb = RGBColor(54, 96, 146)
        
        # Defect details table
        table = doc.add_table(rows=7, cols=2)
        table.style = 'Light List Accent 1'
        
        # Set column widths
        table.columns[0].width = Inches(1.5)
        table.columns[1].width = Inches(5.0)
        
        # Fill in defect details
        details = [
            ('Room:', defect.get('room', 'N/A')),
            ('Component:', defect.get('component', 'N/A')),
            ('Issue:', defect.get('issue_description', 'N/A')),
            ('Trade:', defect.get('trade', 'N/A')),
            ('Priority:', defect.get('priority', 'N/A')),
            ('Status:', defect.get('status', 'Open')),
            ('Inspector Notes:', defect.get('inspector_notes', 'None'))
        ]
        
        for idx, (label, value) in enumerate(details):
            row = table.rows[idx]
            row.cells[0].text = label
            row.cells[0].paragraphs[0].runs[0].font.bold = True
            row.cells[1].text = value
            
            # Highlight priority
            if label == 'Priority:':
                cell = row.cells[1]
                if value == 'High':
                    cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(192, 0, 0)
                    cell.paragraphs[0].runs[0].font.bold = True
                elif value == 'Medium':
                    cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(255, 153, 0)
                    cell.paragraphs[0].runs[0].font.bold = True
                elif value == 'Low':
                    cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(0, 128, 0)
        
        # Add photo if available
        if defect.get('photo_url'):
            doc.add_paragraph()
            
            photo_url = defect['photo_url']
            img_bytes = self.download_photo(photo_url)
            
            if img_bytes:
                try:
                    # Calculate dimensions (5 inches wide, maintain aspect ratio)
                    width, height = self.calculate_image_dimensions(img_bytes, target_width=5.0)
                    
                    # Add the image
                    img_bytes.seek(0)
                    doc.add_picture(img_bytes, width=Inches(width), height=Inches(height))
                    
                    # Add caption
                    caption = doc.add_paragraph()
                    caption.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    caption_text = caption.add_run(
                        f"Photo {defect_num}: {defect.get('room', 'Unknown')} - {defect.get('component', 'Unknown')}"
                    )
                    caption_text.font.size = Pt(10)
                    caption_text.font.italic = True
                    caption_text.font.color.rgb = RGBColor(100, 100, 100)
                    
                except Exception as e:
                    print(f"Error adding photo to Word document: {str(e)}")
                    p = doc.add_paragraph("Photo could not be loaded")
                    p.runs[0].font.italic = True
                    p.runs[0].font.color.rgb = RGBColor(192, 0, 0)
            else:
                p = doc.add_paragraph("Photo unavailable")
                p.runs[0].font.italic = True
                p.runs[0].font.color.rgb = RGBColor(192, 0, 0)
        
        # Add separator
        doc.add_paragraph()
        doc.add_paragraph('─' * 80)
        doc.add_paragraph()
    
    def generate_single_inspection_report(
        self,
        inspection_data: Dict[str, Any],
        defects: List[Dict[str, Any]],
        output_path: str
    ) -> bool:
        """
        Generate Word report for a single inspection with photos
        
        Args:
            inspection_data: Dictionary containing inspection metadata
            defects: List of defect dictionaries with photo_url and inspector_notes
            output_path: Path where the Word file should be saved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            doc = Document()
            
            # Set document margins
            sections = doc.sections
            for section in sections:
                section.top_margin = Inches(1)
                section.bottom_margin = Inches(1)
                section.left_margin = Inches(1)
                section.right_margin = Inches(1)
            
            # Calculate photo and note counts
            photo_count = sum(1 for d in defects if d.get('photo_url'))
            note_count = sum(1 for d in defects if d.get('inspector_notes'))
            
            inspection_data['photo_count'] = photo_count
            inspection_data['note_count'] = note_count
            inspection_data['total_defects'] = len(defects)
            
            # Add header
            self.add_header(doc, inspection_data)
            
            # Add defects
            for idx, defect in enumerate(defects, 1):
                self.add_defect_section(doc, defect, idx, len(defects))
            
            # Add footer with generation timestamp
            doc.add_page_break()
            footer = doc.add_paragraph()
            footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
            footer_text = footer.add_run(f'Report generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            footer_text.font.size = Pt(9)
            footer_text.font.color.rgb = RGBColor(128, 128, 128)
            
            # Save document
            doc.save(output_path)
            return True
            
        except Exception as e:
            print(f"Error generating Word report: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def generate_multi_inspection_report(
        self,
        inspections: List[Dict[str, Any]],
        output_path: str
    ) -> bool:
        """
        Generate Word report for multiple inspections with photos
        
        Args:
            inspections: List of inspection dictionaries, each containing:
                - inspection_data: metadata
                - defects: list of defects with photos
            output_path: Path where the Word file should be saved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            doc = Document()
            
            # Set document margins
            sections = doc.sections
            for section in sections:
                section.top_margin = Inches(1)
                section.bottom_margin = Inches(1)
                section.left_margin = Inches(1)
                section.right_margin = Inches(1)
            
            # Title page
            title = doc.add_heading('MULTI-INSPECTION REPORT', level=0)
            title.alignment = WD_ALIGN_PARAGRAPH.CENTER
            
            doc.add_paragraph()
            
            # Summary table
            summary_heading = doc.add_heading('Summary of Inspections', level=1)
            summary_heading.runs[0].font.color.rgb = RGBColor(54, 96, 146)
            
            # Calculate totals
            total_defects = sum(len(i['defects']) for i in inspections)
            total_photos = sum(
                sum(1 for d in i['defects'] if d.get('photo_url'))
                for i in inspections
            )
            total_notes = sum(
                sum(1 for d in i['defects'] if d.get('inspector_notes'))
                for i in inspections
            )
            
            # Summary stats
            summary_table = doc.add_table(rows=4, cols=2)
            summary_table.style = 'Light Grid Accent 1'
            
            summary_stats = [
                ('Total Inspections:', str(len(inspections))),
                ('Total Defects:', str(total_defects)),
                ('Total Photos:', str(total_photos)),
                ('Total Notes:', str(total_notes))
            ]
            
            for idx, (label, value) in enumerate(summary_stats):
                row = summary_table.rows[idx]
                row.cells[0].text = label
                row.cells[0].paragraphs[0].runs[0].font.bold = True
                row.cells[1].text = value
            
            doc.add_paragraph()
            
            # Inspection list table
            list_heading = doc.add_heading('Inspection Details', level=2)
            
            inspection_table = doc.add_table(rows=len(inspections) + 1, cols=5)
            inspection_table.style = 'Light List Accent 1'
            
            # Headers
            headers = ['Building', 'Date', 'Inspector', 'Defects', 'Photos']
            header_row = inspection_table.rows[0]
            for idx, header in enumerate(headers):
                cell = header_row.cells[idx]
                cell.text = header
                cell.paragraphs[0].runs[0].font.bold = True
            
            # Data rows
            for idx, inspection in enumerate(inspections, 1):
                data = inspection['inspection_data']
                defects = inspection['defects']
                
                row = inspection_table.rows[idx]
                row.cells[0].text = data.get('building_name', 'N/A')
                row.cells[1].text = data.get('inspection_date', 'N/A')
                row.cells[2].text = data.get('inspector_name', 'N/A')
                row.cells[3].text = str(len(defects))
                row.cells[4].text = str(sum(1 for d in defects if d.get('photo_url')))
            
            # Page break before individual inspections
            doc.add_page_break()
            
            # Add each inspection in detail
            for idx, inspection in enumerate(inspections, 1):
                data = inspection['inspection_data']
                defects = inspection['defects']
                
                # Inspection heading
                insp_heading = doc.add_heading(f'Inspection {idx}: {data.get("building_name", "Unknown")}', level=1)
                insp_heading.runs[0].font.color.rgb = RGBColor(54, 96, 146)
                
                # Add inspection header
                photo_count = sum(1 for d in defects if d.get('photo_url'))
                note_count = sum(1 for d in defects if d.get('inspector_notes'))
                
                data['photo_count'] = photo_count
                data['note_count'] = note_count
                data['total_defects'] = len(defects)
                
                self.add_header(doc, data)
                
                # Add all defects for this inspection
                for defect_idx, defect in enumerate(defects, 1):
                    self.add_defect_section(doc, defect, defect_idx, len(defects))
                
                # Page break between inspections (except for last one)
                if idx < len(inspections):
                    doc.add_page_break()
            
            # Add footer
            doc.add_page_break()
            footer = doc.add_paragraph()
            footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
            footer_text = footer.add_run(f'Report generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            footer_text.font.size = Pt(9)
            footer_text.font.color.rgb = RGBColor(128, 128, 128)
            
            # Save document
            doc.save(output_path)
            return True
            
        except Exception as e:
            print(f"Error generating multi-inspection Word report: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def create_word_report_from_database(
    inspection_ids: List[int],
    db_connection,
    api_key: str,
    output_path: str,
    report_type: str = "single"
) -> bool:
    """
    Main function to create Word report from database data
    
    Args:
        inspection_ids: List of inspection IDs to include
        db_connection: Database connection object
        api_key: SafetyCulture API key
        output_path: Where to save the Word file
        report_type: "single" or "multi" inspection report
        
    Returns:
        True if successful, False otherwise
    """
    try:
        generator = WordGeneratorAPI(api_key)
        
        if report_type == "single" and len(inspection_ids) == 1:
            # Query single inspection
            inspection_data, defects = _query_inspection_data(db_connection, inspection_ids[0])
            return generator.generate_single_inspection_report(inspection_data, defects, output_path)
        
        elif report_type == "multi":
            # Query multiple inspections
            inspections = []
            for inspection_id in inspection_ids:
                inspection_data, defects = _query_inspection_data(db_connection, inspection_id)
                inspections.append({
                    'inspection_data': inspection_data,
                    'defects': defects
                })
            return generator.generate_multi_inspection_report(inspections, output_path)
        
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
               b.name as building_name
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
        'building_name': row[4]
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
            'description': row[2],  # notes → description
            'trade': row[3],
            'priority': row[4],  # urgency → priority
            'status': row[5],  # status_class → status
            'photo_url': row[6],
            'photo_media_id': row[7],
            'inspector_notes': row[8]
        })
    
    cursor.close()
    return inspection_data, defects