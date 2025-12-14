#!/usr/bin/env python3
"""
SPLICE OCR Pipeline
Extracts text from PDF papers using PyMuPDF
"""

import fitz  # PyMuPDF
import json
from pathlib import Path
from typing import Dict, List
import hashlib


class PDFExtractor:
    """Extract text from PDF files"""
    
    def __init__(self, input_dir: str = "data/papers", output_dir: str = "data/processed"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_text(self, pdf_path: Path) -> Dict:
        """Extract text from single PDF"""
        doc = fitz.open(pdf_path)
        
        # Extract metadata
        metadata = {
            "filename": pdf_path.name,
            "pages": doc.page_count,
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "subject": doc.metadata.get("subject", ""),
        }
        
        # Extract text per page
        pages = []
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text()
            if text.strip():  # Only non-empty pages
                pages.append({
                    "page": page_num,
                    "text": text.strip()
                })
        
        doc.close()
        
        # Generate doc ID
        doc_id = hashlib.md5(pdf_path.name.encode()).hexdigest()[:12]
        
        return {
            "doc_id": doc_id,
            "metadata": metadata,
            "pages": pages,
            "full_text": "\n\n".join(p["text"] for p in pages)
        }
    
    def process_directory(self) -> List[Dict]:
        """Process all PDFs in input directory"""
        results = []
        pdf_files = list(self.input_dir.glob("*.pdf"))
        
        print(f"Found {len(pdf_files)} PDF files")
        
        for pdf_path in pdf_files:
            print(f"Processing: {pdf_path.name}")
            try:
                extracted = self.extract_text(pdf_path)
                
                # Save to JSON
                output_file = self.output_dir / f"{extracted['doc_id']}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(extracted, f, indent=2, ensure_ascii=False)
                
                results.append(extracted)
                print(f"  ✅ Saved: {output_file.name}")
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
        
        return results
    
    def extract_single(self, pdf_path: str) -> Dict:
        """Extract text from single PDF file"""
        return self.extract_text(Path(pdf_path))


if __name__ == "__main__":
    # Quick test
    extractor = PDFExtractor()
    results = extractor.process_directory()
    print(f"\n✅ Processed {len(results)} papers!")