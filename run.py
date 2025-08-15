#!/usr/bin/env python3
"""
StackSlice - Multi-Site Stack Exchange Data Explorer
Startup script that imports data for multiple sites and starts the web server
"""

import os
import sys
from pathlib import Path
import logging
import requests
import zipfile
import py7zr
from urllib.parse import urljoin

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
ARCHIVE_BASE_URL = "https://archive.org/download/stackexchange/"
DEFAULT_SITES = ["ai.meta.stackexchange.com"]
REQUIRED_FILES = [
    "Posts.xml", "Users.xml", "Comments.xml", 
    "Votes.xml", "Badges.xml", "Tags.xml"
]

def get_sites_to_import():
    """Get the list of sites to import from environment or use defaults"""
    sites_env = os.environ.get("STACKEXCHANGE_SITES")
    if sites_env:
        return [site.strip() for site in sites_env.split(",")]
    return DEFAULT_SITES

def download_site_data(site_name):
    """Download Stack Exchange data for a specific site"""
    logger.info(f"Downloading data for {site_name}...")
    
    # Create data directory
    data_dir = Path(f"data/{site_name}")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Download URL
    archive_filename = f"{site_name}.7z"
    download_url = urljoin(ARCHIVE_BASE_URL, archive_filename)
    local_archive_path = data_dir / archive_filename
    
    try:
        # Download the archive
        logger.info(f"Downloading from {download_url}...")
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        # Save to local file
        with open(local_archive_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded {local_archive_path}")
        
        # Extract the archive
        logger.info("Extracting archive...")
        with py7zr.SevenZipFile(local_archive_path, mode='r') as archive:
            archive.extractall(path=data_dir)
        
        logger.info("Extraction completed")
        
        # Clean up archive file
        local_archive_path.unlink()
        logger.info("Archive file cleaned up")
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download data: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to extract data: {e}")
        return False

def check_site_data(site_name):
    """Check if data directory exists for a site, download if necessary"""
    data_dir = Path(f"data/{site_name}")
    
    logger.info(f"Checking data for site: {site_name}")
    
    # Check if all required files exist
    missing_files = []
    if data_dir.exists():
        for file in REQUIRED_FILES:
            if not (data_dir / file).exists():
                missing_files.append(file)
    else:
        missing_files = REQUIRED_FILES.copy()
    
    if missing_files:
        logger.info(f"Missing files for {site_name}: {missing_files}")
        logger.info("Attempting to download data from archive.org...")
        
        if download_site_data(site_name):
            # Re-check after download
            missing_files = []
            for file in REQUIRED_FILES:
                if not (data_dir / file).exists():
                    missing_files.append(file)
            
            if missing_files:
                logger.error(f"Still missing files for {site_name} after download: {missing_files}")
                return False
        else:
            logger.error(f"Failed to download data for {site_name}")
            logger.error(f"Please manually download {site_name}.7z from {ARCHIVE_BASE_URL}")
            logger.error(f"Extract it to {data_dir}")
            return False
    
    logger.info(f"Data directory and files found for {site_name}")
    return True

def import_all_sites():
    """Import Stack Exchange data for all configured sites into DuckDB"""
    try:
        from data_importer import StackExchangeDataImporter
        
        db_path = "stackexchange.db"
        sites = get_sites_to_import()
        
        # Check if database already exists and has data
        if os.path.exists(db_path):
            import duckdb
            conn = duckdb.connect(db_path)
            try:
                sites_in_db = conn.execute("SELECT DISTINCT site FROM posts").fetchall()
                existing_sites = {site[0] for site in sites_in_db}
                
                # Check if all our sites are already imported
                sites_to_import = set(sites) - existing_sites
                if not sites_to_import:
                    logger.info(f"Database already contains data for all sites: {sites}")
                    conn.close()
                    return True
                else:
                    logger.info(f"Need to import data for sites: {sites_to_import}")
                    sites = list(sites_to_import)
            except:
                # Table doesn't exist, need to import all
                logger.info("Database exists but no posts table found, importing all sites")
            conn.close()
        
        # Import data for each site
        importer = StackExchangeDataImporter(db_path)
        
        try:
            for site_name in sites:
                logger.info(f"Starting import for {site_name}...")
                data_folder = f"data/{site_name}"
                importer.import_site_data(site_name, data_folder)
                
                # Show stats
                stats = importer.get_site_stats(site_name)
                logger.info(f"Import completed for {site_name}:")
                for table, count in stats.items():
                    logger.info(f"  {table}: {count:,}")
        
        finally:
            importer.close()
        
        logger.info("All sites imported successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during data import: {e}")
        return False

def start_server():
    """Start the FastAPI web server"""
    try:
        import uvicorn
        from main import app
        
        # Get port from environment (for cloud deployment)
        port = int(os.environ.get("PORT", 8000))
        
        logger.info("Starting StackSlice multi-site web server...")
        logger.info(f"Access the application at: http://localhost:{port}")
        
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=port,
            log_level="info"
        )
        
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        return False

def main():
    """Main startup function"""
    sites = get_sites_to_import()
    logger.info(f"Starting StackSlice for sites: {', '.join(sites)}")
    
    # Check data for all sites
    for site in sites:
        if not check_site_data(site):
            logger.error(f"Failed to get data for {site}. Exiting.")
            sys.exit(1)
    
    # Import data for all sites
    if not import_all_sites():
        logger.error("Failed to import data. Exiting.")
        sys.exit(1)
    
    # Start web server
    start_server()

if __name__ == "__main__":
    main()
