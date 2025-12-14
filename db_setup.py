#!/usr/bin/env python3
"""
SPLICE Database Setup
ChromaDB for vector storage and semantic search
"""

import chromadb
from chromadb.config import Settings
from pathlib import Path
import json
from typing import List, Dict


class SpliceDB:
    """ChromaDB wrapper for SPLICE papers"""
    
    def __init__(self, db_path: str = "data/chroma_db"):
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(
            path=str(self.db_path),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Create or get collection
        self.collection = self.client.get_or_create_collection(
            name="splice_papers",
            metadata={"description": "mRNA papers for SPLICE agent"}
        )
    
    def add_paper(self, doc_id: str, text: str, metadata: Dict):
        """Add single paper to database"""
        self.collection.add(
            ids=[doc_id],
            documents=[text],
            metadatas=[metadata]
        )
    
    def add_papers_from_json(self, json_dir: str = "data/processed"):
        """Add all papers from processed JSON files"""
        json_path = Path(json_dir)
        json_files = list(json_path.glob("*.json"))
        
        print(f"Found {len(json_files)} processed papers")
        
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Prepare metadata
            metadata = {
                "filename": data["metadata"]["filename"],
                "title": data["metadata"]["title"],
                "author": data["metadata"]["author"],
                "pages": data["metadata"]["pages"],
            }
            
            # Add to DB
            self.add_paper(
                doc_id=data["doc_id"],
                text=data["full_text"],
                metadata=metadata
            )
            
            print(f"  ✅ Added: {metadata['filename']}")
        
        print(f"\n✅ Total papers in DB: {self.collection.count()}")
    
    def search(self, query: str, n_results: int = 5) -> Dict:
        """Semantic search in papers"""
        results = self.collection.query(
            query_texts=[query],
            n_results=n_results
        )
        return results
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        return {
            "total_papers": self.collection.count(),
            "collection_name": self.collection.name,
            "db_path": str(self.db_path)
        }


def init_database():
    """Initialize database and load papers"""
    print("🚀 Initializing SPLICE Database...")
    
    db = SpliceDB()
    
    # Check if already populated
    stats = db.get_stats()
    print(f"Current papers in DB: {stats['total_papers']}")
    
    if stats['total_papers'] == 0:
        print("\n📚 Loading papers from processed JSONs...")
        db.add_papers_from_json()
    else:
        print("✅ Database already populated!")
    
    return db


if __name__ == "__main__":
    # Setup and test
    db = init_database()
    
    # Test search
    print("\n🔍 Testing search...")
    query = "spliceosome programming"
    results = db.search(query, n_results=3)
    
    print(f"\nTop 3 results for '{query}':")
    for i, (doc_id, metadata) in enumerate(zip(results['ids'][0], results['metadatas'][0]), 1):
        print(f"{i}. {metadata['filename']}")
    
    print("\n✅ Database ready!")