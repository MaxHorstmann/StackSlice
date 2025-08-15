import os
import xml.etree.ElementTree as ET
import duckdb
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StackExchangeDataImporter:
    def __init__(self, data_dir: str, db_path: str = "stackslice.db"):
        self.data_dir = Path(data_dir)
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        
    def create_tables(self):
        """Create the database tables for Stack Exchange data"""
        
        # Posts table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY,
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
                title TEXT,
                tags TEXT,
                answer_count INTEGER,
                comment_count INTEGER,
                content_license TEXT,
                parent_id INTEGER,
                closed_date TIMESTAMP
            )
        """)
        
        # Users table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                reputation INTEGER,
                creation_date TIMESTAMP,
                display_name TEXT,
                last_access_date TIMESTAMP,
                website_url TEXT,
                location TEXT,
                about_me TEXT,
                views INTEGER,
                up_votes INTEGER,
                down_votes INTEGER,
                account_id INTEGER
            )
        """)
        
        # Comments table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                score INTEGER,
                text TEXT,
                creation_date TIMESTAMP,
                user_id INTEGER
            )
        """)
        
        # Votes table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                vote_type_id INTEGER,
                creation_date TIMESTAMP,
                user_id INTEGER,
                bounty_amount INTEGER
            )
        """)
        
        # Badges table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS badges (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                name TEXT,
                date TIMESTAMP,
                class INTEGER,
                tag_based BOOLEAN
            )
        """)
        
        # Tags table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY,
                tag_name TEXT,
                count INTEGER,
                excerpt_post_id INTEGER,
                wiki_post_id INTEGER
            )
        """)
        
        logger.info("Database tables created successfully")
    
    def parse_date(self, date_str):
        """Parse Stack Exchange date format"""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('T', ' ').replace('Z', ''))
        except:
            return None
    
    def import_posts(self):
        """Import posts from Posts.xml"""
        posts_file = self.data_dir / "Posts.xml"
        if not posts_file.exists():
            logger.warning(f"Posts.xml not found in {self.data_dir}")
            return
            
        logger.info("Importing posts...")
        tree = ET.parse(posts_file)
        root = tree.getroot()
        
        posts_data = []
        for row in root.findall('row'):
            posts_data.append({
                'id': int(row.get('Id')),
                'post_type_id': int(row.get('PostTypeId', 0)),
                'accepted_answer_id': int(row.get('AcceptedAnswerId')) if row.get('AcceptedAnswerId') else None,
                'creation_date': self.parse_date(row.get('CreationDate')),
                'score': int(row.get('Score', 0)),
                'view_count': int(row.get('ViewCount', 0)),
                'body': row.get('Body', ''),
                'owner_user_id': int(row.get('OwnerUserId')) if row.get('OwnerUserId') else None,
                'last_editor_user_id': int(row.get('LastEditorUserId')) if row.get('LastEditorUserId') else None,
                'last_edit_date': self.parse_date(row.get('LastEditDate')),
                'last_activity_date': self.parse_date(row.get('LastActivityDate')),
                'title': row.get('Title', ''),
                'tags': row.get('Tags', ''),
                'answer_count': int(row.get('AnswerCount', 0)),
                'comment_count': int(row.get('CommentCount', 0)),
                'content_license': row.get('ContentLicense', ''),
                'parent_id': int(row.get('ParentId')) if row.get('ParentId') else None,
                'closed_date': self.parse_date(row.get('ClosedDate'))
            })
        
        # Batch insert
        self.conn.executemany("""
            INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [(
            p['id'], p['post_type_id'], p['accepted_answer_id'], p['creation_date'],
            p['score'], p['view_count'], p['body'], p['owner_user_id'],
            p['last_editor_user_id'], p['last_edit_date'], p['last_activity_date'],
            p['title'], p['tags'], p['answer_count'], p['comment_count'],
            p['content_license'], p['parent_id'], p['closed_date']
        ) for p in posts_data])
        
        logger.info(f"Imported {len(posts_data)} posts")
    
    def import_users(self):
        """Import users from Users.xml"""
        users_file = self.data_dir / "Users.xml"
        if not users_file.exists():
            logger.warning(f"Users.xml not found in {self.data_dir}")
            return
            
        logger.info("Importing users...")
        tree = ET.parse(users_file)
        root = tree.getroot()
        
        users_data = []
        for row in root.findall('row'):
            users_data.append({
                'id': int(row.get('Id')),
                'reputation': int(row.get('Reputation', 0)),
                'creation_date': self.parse_date(row.get('CreationDate')),
                'display_name': row.get('DisplayName', ''),
                'last_access_date': self.parse_date(row.get('LastAccessDate')),
                'website_url': row.get('WebsiteUrl', ''),
                'location': row.get('Location', ''),
                'about_me': row.get('AboutMe', ''),
                'views': int(row.get('Views', 0)),
                'up_votes': int(row.get('UpVotes', 0)),
                'down_votes': int(row.get('DownVotes', 0)),
                'account_id': int(row.get('AccountId')) if row.get('AccountId') else None
            })
        
        self.conn.executemany("""
            INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [(
            u['id'], u['reputation'], u['creation_date'], u['display_name'],
            u['last_access_date'], u['website_url'], u['location'], u['about_me'],
            u['views'], u['up_votes'], u['down_votes'], u['account_id']
        ) for u in users_data])
        
        logger.info(f"Imported {len(users_data)} users")
    
    def import_comments(self):
        """Import comments from Comments.xml"""
        comments_file = self.data_dir / "Comments.xml"
        if not comments_file.exists():
            logger.warning(f"Comments.xml not found in {self.data_dir}")
            return
            
        logger.info("Importing comments...")
        tree = ET.parse(comments_file)
        root = tree.getroot()
        
        comments_data = []
        for row in root.findall('row'):
            comments_data.append({
                'id': int(row.get('Id')),
                'post_id': int(row.get('PostId')),
                'score': int(row.get('Score', 0)),
                'text': row.get('Text', ''),
                'creation_date': self.parse_date(row.get('CreationDate')),
                'user_id': int(row.get('UserId')) if row.get('UserId') else None
            })
        
        self.conn.executemany("""
            INSERT INTO comments VALUES (?, ?, ?, ?, ?, ?)
        """, [(
            c['id'], c['post_id'], c['score'], c['text'], c['creation_date'], c['user_id']
        ) for c in comments_data])
        
        logger.info(f"Imported {len(comments_data)} comments")
    
    def import_votes(self):
        """Import votes from Votes.xml"""
        votes_file = self.data_dir / "Votes.xml"
        if not votes_file.exists():
            logger.warning(f"Votes.xml not found in {self.data_dir}")
            return
            
        logger.info("Importing votes...")
        tree = ET.parse(votes_file)
        root = tree.getroot()
        
        votes_data = []
        for row in root.findall('row'):
            votes_data.append({
                'id': int(row.get('Id')),
                'post_id': int(row.get('PostId')),
                'vote_type_id': int(row.get('VoteTypeId')),
                'creation_date': self.parse_date(row.get('CreationDate')),
                'user_id': int(row.get('UserId')) if row.get('UserId') else None,
                'bounty_amount': int(row.get('BountyAmount')) if row.get('BountyAmount') else None
            })
        
        self.conn.executemany("""
            INSERT INTO votes VALUES (?, ?, ?, ?, ?, ?)
        """, [(
            v['id'], v['post_id'], v['vote_type_id'], v['creation_date'], v['user_id'], v['bounty_amount']
        ) for v in votes_data])
        
        logger.info(f"Imported {len(votes_data)} votes")
    
    def import_badges(self):
        """Import badges from Badges.xml"""
        badges_file = self.data_dir / "Badges.xml"
        if not badges_file.exists():
            logger.warning(f"Badges.xml not found in {self.data_dir}")
            return
            
        logger.info("Importing badges...")
        tree = ET.parse(badges_file)
        root = tree.getroot()
        
        badges_data = []
        for row in root.findall('row'):
            badges_data.append({
                'id': int(row.get('Id')),
                'user_id': int(row.get('UserId')),
                'name': row.get('Name', ''),
                'date': self.parse_date(row.get('Date')),
                'class': int(row.get('Class', 0)),
                'tag_based': row.get('TagBased') == 'True'
            })
        
        self.conn.executemany("""
            INSERT INTO badges VALUES (?, ?, ?, ?, ?, ?)
        """, [(
            b['id'], b['user_id'], b['name'], b['date'], b['class'], b['tag_based']
        ) for b in badges_data])
        
        logger.info(f"Imported {len(badges_data)} badges")
    
    def import_tags(self):
        """Import tags from Tags.xml"""
        tags_file = self.data_dir / "Tags.xml"
        if not tags_file.exists():
            logger.warning(f"Tags.xml not found in {self.data_dir}")
            return
            
        logger.info("Importing tags...")
        tree = ET.parse(tags_file)
        root = tree.getroot()
        
        tags_data = []
        for row in root.findall('row'):
            tags_data.append({
                'id': int(row.get('Id')),
                'tag_name': row.get('TagName', ''),
                'count': int(row.get('Count', 0)),
                'excerpt_post_id': int(row.get('ExcerptPostId')) if row.get('ExcerptPostId') else None,
                'wiki_post_id': int(row.get('WikiPostId')) if row.get('WikiPostId') else None
            })
        
        self.conn.executemany("""
            INSERT INTO tags VALUES (?, ?, ?, ?, ?)
        """, [(
            t['id'], t['tag_name'], t['count'], t['excerpt_post_id'], t['wiki_post_id']
        ) for t in tags_data])
        
        logger.info(f"Imported {len(tags_data)} tags")
    
    def import_all_data(self):
        """Import all Stack Exchange data"""
        logger.info("Starting data import...")
        
        # Clear existing data
        self.conn.execute("DROP TABLE IF EXISTS posts")
        self.conn.execute("DROP TABLE IF EXISTS users") 
        self.conn.execute("DROP TABLE IF EXISTS comments")
        self.conn.execute("DROP TABLE IF EXISTS votes")
        self.conn.execute("DROP TABLE IF EXISTS badges")
        self.conn.execute("DROP TABLE IF EXISTS tags")
        
        # Create tables
        self.create_tables()
        
        # Import data
        self.import_users()
        self.import_posts()
        self.import_comments()
        self.import_votes()
        self.import_badges()
        self.import_tags()
        
        logger.info("Data import completed successfully!")
    
    def close(self):
        """Close database connection"""
        self.conn.close()

if __name__ == "__main__":
    # Import the AI Stack Exchange data
    importer = StackExchangeDataImporter("data/ai.stackexchange.com")
    importer.import_all_data()
    importer.close()
