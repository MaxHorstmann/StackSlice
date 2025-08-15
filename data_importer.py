#!/usr/bin/env python3
"""
Multi-site Stack Exchange Data Importer
Imports data from multiple Stack Exchange sites into a single DuckDB database
"""

import os
import sys
from pathlib import Path
import logging
import duckdb
from lxml import etree
from typing import Dict, Any, Optional
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StackExchangeDataImporter:
    """Import data from multiple Stack Exchange sites into DuckDB"""
    
    def __init__(self, db_path: str = "stackexchange.db"):
        """Initialize the importer with database path"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.create_tables()
    
    def create_tables(self):
        """Create database tables with site column"""
        logger.info("Creating database tables...")
        
        # Posts table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                site VARCHAR NOT NULL,
                id INTEGER NOT NULL,
                post_type_id INTEGER,
                accepted_answer_id INTEGER,
                creation_date TIMESTAMP,
                score INTEGER,
                view_count INTEGER,
                body TEXT,
                owner_user_id INTEGER,
                last_editor_user_id INTEGER,
                last_edit_date TIMESTAMP,
                last_activity_date TIMESTAMP,
                title VARCHAR,
                tags VARCHAR,
                answer_count INTEGER,
                comment_count INTEGER,
                content_license VARCHAR,
                parent_id INTEGER,
                closed_date TIMESTAMP,
                PRIMARY KEY (site, id)
            )
        """)
        
        # Users table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                site VARCHAR NOT NULL,
                id INTEGER NOT NULL,
                reputation INTEGER,
                creation_date TIMESTAMP,
                display_name VARCHAR,
                last_access_date TIMESTAMP,
                website_url VARCHAR,
                location VARCHAR,
                about_me TEXT,
                views INTEGER,
                up_votes INTEGER,
                down_votes INTEGER,
                profile_image_url VARCHAR,
                email_hash VARCHAR,
                account_id INTEGER,
                PRIMARY KEY (site, id)
            )
        """)
        
        # Comments table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                site VARCHAR NOT NULL,
                id INTEGER NOT NULL,
                post_id INTEGER NOT NULL,
                score INTEGER,
                text TEXT,
                creation_date TIMESTAMP,
                user_display_name VARCHAR,
                user_id INTEGER,
                content_license VARCHAR,
                PRIMARY KEY (site, id)
            )
        """)
        
        # Votes table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                site VARCHAR NOT NULL,
                id INTEGER NOT NULL,
                post_id INTEGER NOT NULL,
                vote_type_id INTEGER,
                creation_date TIMESTAMP,
                user_id INTEGER,
                bounty_amount INTEGER,
                PRIMARY KEY (site, id)
            )
        """)
        
        # Tags table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                site VARCHAR NOT NULL,
                id INTEGER NOT NULL,
                tag_name VARCHAR,
                count INTEGER,
                excerpt_post_id INTEGER,
                wiki_post_id INTEGER,
                PRIMARY KEY (site, id)
            )
        """)
        
        # Badges table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS badges (
                site VARCHAR NOT NULL,
                id INTEGER NOT NULL,
                user_id INTEGER,
                name VARCHAR,
                date TIMESTAMP,
                class INTEGER,
                tag_based BOOLEAN,
                PRIMARY KEY (site, id)
            )
        """)
        
        logger.info("Database tables created successfully")
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to proper format"""
        if not date_str:
            return None
        try:
            # Stack Exchange dates are in ISO format
            return date_str
        except:
            return None
    
    def safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to int"""
        if not value:
            return None
        try:
            return int(value)
        except:
            return None
    
    def safe_bool(self, value: str) -> Optional[bool]:
        """Safely convert string to bool"""
        if not value:
            return None
        return value.lower() == 'true'
    
    def import_posts(self, site_name: str, data_folder: str):
        """Import posts from Posts.xml"""
        posts_file = Path(data_folder) / "Posts.xml"
        if not posts_file.exists():
            logger.warning(f"Posts.xml not found in {data_folder}")
            return
        
        logger.info(f"Importing posts for {site_name}...")
        
        # Clear existing posts for this site
        self.conn.execute("DELETE FROM posts WHERE site = ?", [site_name])
        
        # Parse and insert posts
        parser = etree.iterparse(str(posts_file), events=('start', 'end'))
        parser = iter(parser)
        event, root = next(parser)
        
        batch = []
        batch_size = 1000
        
        for event, elem in parser:
            if event == 'end' and elem.tag == 'row':
                post_data = (
                    site_name,
                    self.safe_int(elem.get('Id')),
                    self.safe_int(elem.get('PostTypeId')),
                    self.safe_int(elem.get('AcceptedAnswerId')),
                    self.parse_date(elem.get('CreationDate')),
                    self.safe_int(elem.get('Score')),
                    self.safe_int(elem.get('ViewCount')),
                    elem.get('Body'),
                    self.safe_int(elem.get('OwnerUserId')),
                    self.safe_int(elem.get('LastEditorUserId')),
                    self.parse_date(elem.get('LastEditDate')),
                    self.parse_date(elem.get('LastActivityDate')),
                    elem.get('Title'),
                    elem.get('Tags'),
                    self.safe_int(elem.get('AnswerCount')),
                    self.safe_int(elem.get('CommentCount')),
                    elem.get('ContentLicense'),
                    self.safe_int(elem.get('ParentId')),
                    self.parse_date(elem.get('ClosedDate'))
                )
                batch.append(post_data)
                
                if len(batch) >= batch_size:
                    self.conn.executemany("""
                        INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    batch = []
                
                elem.clear()
                root.clear()
        
        # Insert remaining records
        if batch:
            self.conn.executemany("""
                INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
        
        count = self.conn.execute("SELECT COUNT(*) FROM posts WHERE site = ?", [site_name]).fetchone()[0]
        logger.info(f"Imported {count} posts for {site_name}")
    
    def import_users(self, site_name: str, data_folder: str):
        """Import users from Users.xml"""
        users_file = Path(data_folder) / "Users.xml"
        if not users_file.exists():
            logger.warning(f"Users.xml not found in {data_folder}")
            return
        
        logger.info(f"Importing users for {site_name}...")
        
        # Clear existing users for this site
        self.conn.execute("DELETE FROM users WHERE site = ?", [site_name])
        
        parser = etree.iterparse(str(users_file), events=('start', 'end'))
        parser = iter(parser)
        event, root = next(parser)
        
        batch = []
        batch_size = 1000
        
        for event, elem in parser:
            if event == 'end' and elem.tag == 'row':
                user_data = (
                    site_name,
                    self.safe_int(elem.get('Id')),
                    self.safe_int(elem.get('Reputation')),
                    self.parse_date(elem.get('CreationDate')),
                    elem.get('DisplayName'),
                    self.parse_date(elem.get('LastAccessDate')),
                    elem.get('WebsiteUrl'),
                    elem.get('Location'),
                    elem.get('AboutMe'),
                    self.safe_int(elem.get('Views')),
                    self.safe_int(elem.get('UpVotes')),
                    self.safe_int(elem.get('DownVotes')),
                    elem.get('ProfileImageUrl'),
                    elem.get('EmailHash'),
                    self.safe_int(elem.get('AccountId'))
                )
                batch.append(user_data)
                
                if len(batch) >= batch_size:
                    self.conn.executemany("""
                        INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    batch = []
                
                elem.clear()
                root.clear()
        
        if batch:
            self.conn.executemany("""
                INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
        
        count = self.conn.execute("SELECT COUNT(*) FROM users WHERE site = ?", [site_name]).fetchone()[0]
        logger.info(f"Imported {count} users for {site_name}")
    
    def import_comments(self, site_name: str, data_folder: str):
        """Import comments from Comments.xml"""
        comments_file = Path(data_folder) / "Comments.xml"
        if not comments_file.exists():
            logger.warning(f"Comments.xml not found in {data_folder}")
            return
        
        logger.info(f"Importing comments for {site_name}...")
        
        # Clear existing comments for this site
        self.conn.execute("DELETE FROM comments WHERE site = ?", [site_name])
        
        parser = etree.iterparse(str(comments_file), events=('start', 'end'))
        parser = iter(parser)
        event, root = next(parser)
        
        batch = []
        batch_size = 1000
        
        for event, elem in parser:
            if event == 'end' and elem.tag == 'row':
                comment_data = (
                    site_name,
                    self.safe_int(elem.get('Id')),
                    self.safe_int(elem.get('PostId')),
                    self.safe_int(elem.get('Score')),
                    elem.get('Text'),
                    self.parse_date(elem.get('CreationDate')),
                    elem.get('UserDisplayName'),
                    self.safe_int(elem.get('UserId')),
                    elem.get('ContentLicense')
                )
                batch.append(comment_data)
                
                if len(batch) >= batch_size:
                    self.conn.executemany("""
                        INSERT INTO comments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    batch = []
                
                elem.clear()
                root.clear()
        
        if batch:
            self.conn.executemany("""
                INSERT INTO comments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
        
        count = self.conn.execute("SELECT COUNT(*) FROM comments WHERE site = ?", [site_name]).fetchone()[0]
        logger.info(f"Imported {count} comments for {site_name}")
    
    def import_other_tables(self, site_name: str, data_folder: str):
        """Import votes, tags, and badges"""
        data_folder = Path(data_folder)
        
        # Import votes
        votes_file = data_folder / "Votes.xml"
        if votes_file.exists():
            logger.info(f"Importing votes for {site_name}...")
            self.conn.execute("DELETE FROM votes WHERE site = ?", [site_name])
            
            parser = etree.iterparse(str(votes_file), events=('start', 'end'))
            parser = iter(parser)
            event, root = next(parser)
            
            batch = []
            for event, elem in parser:
                if event == 'end' and elem.tag == 'row':
                    vote_data = (
                        site_name,
                        self.safe_int(elem.get('Id')),
                        self.safe_int(elem.get('PostId')),
                        self.safe_int(elem.get('VoteTypeId')),
                        self.parse_date(elem.get('CreationDate')),
                        self.safe_int(elem.get('UserId')),
                        self.safe_int(elem.get('BountyAmount'))
                    )
                    batch.append(vote_data)
                    
                    if len(batch) >= 1000:
                        self.conn.executemany("INSERT INTO votes VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
                        batch = []
                    
                    elem.clear()
                    root.clear()
            
            if batch:
                self.conn.executemany("INSERT INTO votes VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
        
        # Import tags
        tags_file = data_folder / "Tags.xml"
        if tags_file.exists():
            logger.info(f"Importing tags for {site_name}...")
            self.conn.execute("DELETE FROM tags WHERE site = ?", [site_name])
            
            parser = etree.iterparse(str(tags_file), events=('start', 'end'))
            parser = iter(parser)
            event, root = next(parser)
            
            batch = []
            for event, elem in parser:
                if event == 'end' and elem.tag == 'row':
                    tag_data = (
                        site_name,
                        self.safe_int(elem.get('Id')),
                        elem.get('TagName'),
                        self.safe_int(elem.get('Count')),
                        self.safe_int(elem.get('ExcerptPostId')),
                        self.safe_int(elem.get('WikiPostId'))
                    )
                    batch.append(tag_data)
                    
                    if len(batch) >= 1000:
                        self.conn.executemany("INSERT INTO tags VALUES (?, ?, ?, ?, ?, ?)", batch)
                        batch = []
                    
                    elem.clear()
                    root.clear()
            
            if batch:
                self.conn.executemany("INSERT INTO tags VALUES (?, ?, ?, ?, ?, ?)", batch)
        
        # Import badges
        badges_file = data_folder / "Badges.xml"
        if badges_file.exists():
            logger.info(f"Importing badges for {site_name}...")
            self.conn.execute("DELETE FROM badges WHERE site = ?", [site_name])
            
            parser = etree.iterparse(str(badges_file), events=('start', 'end'))
            parser = iter(parser)
            event, root = next(parser)
            
            batch = []
            for event, elem in parser:
                if event == 'end' and elem.tag == 'row':
                    badge_data = (
                        site_name,
                        self.safe_int(elem.get('Id')),
                        self.safe_int(elem.get('UserId')),
                        elem.get('Name'),
                        self.parse_date(elem.get('Date')),
                        self.safe_int(elem.get('Class')),
                        self.safe_bool(elem.get('TagBased'))
                    )
                    batch.append(badge_data)
                    
                    if len(batch) >= 1000:
                        self.conn.executemany("INSERT INTO badges VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
                        batch = []
                    
                    elem.clear()
                    root.clear()
            
            if batch:
                self.conn.executemany("INSERT INTO badges VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
    
    def import_site_data(self, site_name: str, data_folder: str):
        """Import all data for a specific site"""
        logger.info(f"Starting import for site: {site_name}")
        
        self.import_posts(site_name, data_folder)
        self.import_users(site_name, data_folder)
        self.import_comments(site_name, data_folder)
        self.import_other_tables(site_name, data_folder)
        
        logger.info(f"Completed import for site: {site_name}")
    
    def get_site_stats(self, site_name: str):
        """Get statistics for a site"""
        stats = {}
        
        tables = ['posts', 'users', 'comments', 'votes', 'tags', 'badges']
        for table in tables:
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE site = ?", [site_name]).fetchone()[0]
                stats[table] = count
            except:
                stats[table] = 0
        
        return stats
    
    def list_sites(self):
        """List all sites in the database"""
        try:
            sites = self.conn.execute("SELECT DISTINCT site FROM posts ORDER BY site").fetchall()
            return [site[0] for site in sites]
        except:
            return []
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description="Import Stack Exchange data for multiple sites")
    parser.add_argument("--site", required=True, help="Site name (e.g., ai.stackexchange.com)")
    parser.add_argument("--data-folder", required=True, help="Path to data folder containing XML files")
    parser.add_argument("--db-path", default="stackexchange.db", help="Database file path")
    
    args = parser.parse_args()
    
    if not Path(args.data_folder).exists():
        logger.error(f"Data folder does not exist: {args.data_folder}")
        sys.exit(1)
    
    importer = StackExchangeDataImporter(args.db_path)
    try:
        importer.import_site_data(args.site, args.data_folder)
        
        # Show stats
        stats = importer.get_site_stats(args.site)
        logger.info(f"Import completed for {args.site}:")
        for table, count in stats.items():
            logger.info(f"  {table}: {count:,}")
            
    finally:
        importer.close()

if __name__ == "__main__":
    main()
