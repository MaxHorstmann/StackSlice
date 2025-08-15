#!/usr/bin/env python3
"""
StackSlice - Stack Exchange Data Explorer
Startup script that imports data and starts the web server
"""

import os
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_data_directory():
    """Check if data directory exists"""
    data_dir = Path("data/ai.stackexchange.com")
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        logger.error("Please ensure the Stack Exchange data dump is in the data/ai.stackexchange.com directory")
        return False
    
    required_files = [
        "Posts.xml", "Users.xml", "Comments.xml", 
        "Votes.xml", "Badges.xml", "Tags.xml"
    ]
    
    missing_files = []
    for file in required_files:
        if not (data_dir / file).exists():
            missing_files.append(file)
    
    if missing_files:
        logger.error(f"Missing required files: {missing_files}")
        return False
    
    logger.info("Data directory and files found")
    return True

def import_data():
    """Import Stack Exchange data into DuckDB"""
    try:
        from data_importer import StackExchangeDataImporter
        
        db_path = "stackslice.db"
        
        # Check if database already exists and has data
        if os.path.exists(db_path):
            import duckdb
            conn = duckdb.connect(db_path)
            try:
                result = conn.execute("SELECT COUNT(*) FROM posts").fetchone()
                if result and result[0] > 0:
                    logger.info(f"Database already exists with {result[0]} posts. Skipping import.")
                    conn.close()
                    return True
            except:
                # Table doesn't exist, need to import
                pass
            conn.close()
        
        logger.info("Starting data import...")
        importer = StackExchangeDataImporter("data/ai.stackexchange.com", db_path)
        importer.import_all_data()
        importer.close()
        logger.info("Data import completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during data import: {e}")
        return False

def start_server():
    """Start the FastAPI web server"""
    try:
        import uvicorn
        from main import app
        
        logger.info("Starting StackSlice web server...")
        logger.info("Access the application at: http://localhost:8000")
        
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=8000,
            log_level="info"
        )
        
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        return False

def main():
    """Main startup function"""
    logger.info("Starting StackSlice...")
    
    # Check if data directory exists
    if not check_data_directory():
        sys.exit(1)
    
    # Import data
    if not import_data():
        logger.error("Failed to import data. Exiting.")
        sys.exit(1)
    
    # Start web server
    start_server()

if __name__ == "__main__":
    main()
