from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
import duckdb
from typing import Optional, List, Dict, Any
import html
import re
from datetime import datetime
from pathlib import Path

app = FastAPI(title="StackSlice", description="Slice and dice Stack Overflow data")

# Setup templates and static files
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Database connection
DB_PATH = "stackexchange.db"

def get_db():
    """Get database connection"""
    return duckdb.connect(DB_PATH)

def clean_html(text: str) -> str:
    """Clean HTML tags from text and decode entities"""
    if not text:
        return ""
    # Remove HTML tags
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    # Decode HTML entities
    text = html.unescape(text)
    return text

def format_date(date_obj) -> str:
    """Format datetime object for display"""
    if not date_obj:
        return ""
    if isinstance(date_obj, str):
        try:
            date_obj = datetime.fromisoformat(date_obj)
        except:
            return date_obj
    return date_obj.strftime("%Y-%m-%d %H:%M")

def extract_tags(tags_str: str) -> List[str]:
    """Extract individual tags from tag string"""
    if not tags_str:
        return []
    # Tags are in format |tag1|tag2|tag3|
    return [tag for tag in tags_str.split('|') if tag]

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page with overview statistics"""
    conn = get_db()
    
    try:
        # Get basic statistics
        stats = {}
        
        # Total posts
        result = conn.execute("SELECT COUNT(*) as total FROM posts").fetchone()
        stats['total_posts'] = result[0] if result else 0
        
        # Total questions (PostTypeId = 1)
        result = conn.execute("SELECT COUNT(*) as total FROM posts WHERE post_type_id = 1").fetchone()
        stats['total_questions'] = result[0] if result else 0
        
        # Total answers (PostTypeId = 2)
        result = conn.execute("SELECT COUNT(*) as total FROM posts WHERE post_type_id = 2").fetchone()
        stats['total_answers'] = result[0] if result else 0
        
        # Total users
        result = conn.execute("SELECT COUNT(*) as total FROM users").fetchone()
        stats['total_users'] = result[0] if result else 0
        
        # Total comments
        result = conn.execute("SELECT COUNT(*) as total FROM comments").fetchone()
        stats['total_comments'] = result[0] if result else 0
        
        # Most recent activity
        result = conn.execute("""
            SELECT creation_date 
            FROM posts 
            ORDER BY creation_date DESC 
            LIMIT 1
        """).fetchone()
        stats['latest_activity'] = format_date(result[0]) if result else "N/A"
        
        # Top tags
        top_tags = conn.execute("""
            SELECT tag_name, count 
            FROM tags 
            ORDER BY count DESC 
            LIMIT 10
        """).fetchall()
        
        conn.close()
        
        return templates.TemplateResponse("home.html", {
            "request": request,
            "stats": stats,
            "top_tags": top_tags
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/posts", response_class=HTMLResponse)
async def posts_page(
    request: Request,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    post_type: Optional[str] = None,
    sort: str = Query("recent")
):
    """Browse posts with pagination and filtering"""
    conn = get_db()
    
    try:
        offset = (page - 1) * limit
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if search:
            where_conditions.append("(title ILIKE ? OR body ILIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        if post_type == "questions":
            where_conditions.append("post_type_id = ?")
            params.append(1)
        elif post_type == "answers":
            where_conditions.append("post_type_id = ?")
            params.append(2)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Build ORDER BY clause
        if sort == "recent":
            order_clause = "creation_date DESC"
        elif sort == "score":
            order_clause = "score DESC"
        elif sort == "views":
            order_clause = "view_count DESC"
        else:
            order_clause = "creation_date DESC"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM posts WHERE {where_clause}"
        total = conn.execute(count_query, params).fetchone()[0]
        
        # Get posts
        query = f"""
            SELECT 
                id, post_type_id, title, score, view_count, answer_count,
                creation_date, owner_user_id, tags, body
            FROM posts 
            WHERE {where_clause}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        posts_data = conn.execute(query, params).fetchall()
        
        # Format posts data
        posts = []
        for row in posts_data:
            post = {
                'id': row[0],
                'post_type_id': row[1],
                'title': row[2] or "No title",
                'score': row[3],
                'view_count': row[4],
                'answer_count': row[5],
                'creation_date': format_date(row[6]),
                'owner_user_id': row[7],
                'tags': extract_tags(row[8]),
                'body_preview': clean_html(row[9])[:200] + "..." if len(clean_html(row[9])) > 200 else clean_html(row[9])
            }
            posts.append(post)
        
        conn.close()
        
        # Calculate pagination
        total_pages = (total + limit - 1) // limit
        
        return templates.TemplateResponse("posts.html", {
            "request": request,
            "posts": posts,
            "current_page": page,
            "total_pages": total_pages,
            "total_posts": total,
            "search": search or "",
            "post_type": post_type or "",
            "sort": sort,
            "has_prev": page > 1,
            "has_next": page < total_pages,
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if page < total_pages else None
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/posts/{post_id}", response_class=HTMLResponse)
async def post_detail(request: Request, post_id: int):
    """View a specific post with its answers and comments"""
    conn = get_db()
    
    try:
        # Get the main post
        post_query = """
            SELECT p.*, u.display_name as owner_name
            FROM posts p
            LEFT JOIN users u ON p.owner_user_id = u.id
            WHERE p.id = ?
        """
        post_data = conn.execute(post_query, [post_id]).fetchone()
        
        if not post_data:
            raise HTTPException(status_code=404, detail="Post not found")
        
        # Format main post
        post = {
            'id': post_data[0],
            'post_type_id': post_data[1],
            'title': post_data[11] or "No title",
            'body': post_data[6],
            'score': post_data[4],
            'view_count': post_data[5],
            'creation_date': format_date(post_data[3]),
            'owner_user_id': post_data[7],
            'owner_name': post_data[-1] or "Unknown User",
            'tags': extract_tags(post_data[12]),
            'answer_count': post_data[13],
            'comment_count': post_data[14]
        }
        
        # Get answers if this is a question
        answers = []
        if post['post_type_id'] == 1:
            answers_query = """
                SELECT p.*, u.display_name as owner_name
                FROM posts p
                LEFT JOIN users u ON p.owner_user_id = u.id
                WHERE p.parent_id = ? AND p.post_type_id = 2
                ORDER BY p.score DESC, p.creation_date ASC
            """
            answers_data = conn.execute(answers_query, [post_id]).fetchall()
            
            for answer_data in answers_data:
                answer = {
                    'id': answer_data[0],
                    'body': answer_data[6],
                    'score': answer_data[4],
                    'creation_date': format_date(answer_data[3]),
                    'owner_user_id': answer_data[7],
                    'owner_name': answer_data[-1] or "Unknown User",
                    'is_accepted': answer_data[0] == post_data[2]  # accepted_answer_id
                }
                answers.append(answer)
        
        # Get comments for the post
        comments_query = """
            SELECT c.*, u.display_name as user_name
            FROM comments c
            LEFT JOIN users u ON c.user_id = u.id
            WHERE c.post_id = ?
            ORDER BY c.creation_date ASC
        """
        comments_data = conn.execute(comments_query, [post_id]).fetchall()
        
        comments = []
        for comment_data in comments_data:
            comment = {
                'id': comment_data[0],
                'text': comment_data[3],
                'score': comment_data[2],
                'creation_date': format_date(comment_data[4]),
                'user_id': comment_data[5],
                'user_name': comment_data[-1] or "Unknown User"
            }
            comments.append(comment)
        
        conn.close()
        
        return templates.TemplateResponse("post_detail.html", {
            "request": request,
            "post": post,
            "answers": answers,
            "comments": comments
        })
        
    except Exception as e:
        conn.close()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/users", response_class=HTMLResponse)
async def users_page(
    request: Request,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    sort: str = Query("reputation")
):
    """Browse users with pagination and filtering"""
    conn = get_db()
    
    try:
        offset = (page - 1) * limit
        
        # Build WHERE clause
        where_clause = "1=1"
        params = []
        
        if search:
            where_clause = "display_name ILIKE ?"
            params.append(f"%{search}%")
        
        # Build ORDER BY clause
        if sort == "reputation":
            order_clause = "reputation DESC"
        elif sort == "recent":
            order_clause = "creation_date DESC"
        elif sort == "name":
            order_clause = "display_name ASC"
        else:
            order_clause = "reputation DESC"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM users WHERE {where_clause}"
        total = conn.execute(count_query, params).fetchone()[0]
        
        # Get users
        query = f"""
            SELECT 
                id, display_name, reputation, creation_date, 
                location, views, up_votes, down_votes
            FROM users 
            WHERE {where_clause}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        users_data = conn.execute(query, params).fetchall()
        
        # Format users data
        users = []
        for row in users_data:
            user = {
                'id': row[0],
                'display_name': row[1],
                'reputation': row[2],
                'creation_date': format_date(row[3]),
                'location': row[4] or "",
                'views': row[5],
                'up_votes': row[6],
                'down_votes': row[7]
            }
            users.append(user)
        
        conn.close()
        
        # Calculate pagination
        total_pages = (total + limit - 1) // limit
        
        return templates.TemplateResponse("users.html", {
            "request": request,
            "users": users,
            "current_page": page,
            "total_pages": total_pages,
            "total_users": total,
            "search": search or "",
            "sort": sort,
            "has_prev": page > 1,
            "has_next": page < total_pages,
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if page < total_pages else None
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request):
    """Analytics dashboard with charts and insights"""
    conn = get_db()
    
    try:
        # Posts over time
        posts_over_time = conn.execute("""
            SELECT 
                DATE_TRUNC('month', creation_date) as month,
                COUNT(*) as count
            FROM posts 
            WHERE post_type_id = 1
            GROUP BY DATE_TRUNC('month', creation_date)
            ORDER BY month
        """).fetchall()
        
        # Top users by reputation
        top_users = conn.execute("""
            SELECT display_name, reputation, up_votes, down_votes
            FROM users
            ORDER BY reputation DESC
            LIMIT 10
        """).fetchall()
        
        # Popular tags
        popular_tags = conn.execute("""
            SELECT tag_name, count
            FROM tags
            ORDER BY count DESC
            LIMIT 15
        """).fetchall()
        
        # Post score distribution
        score_distribution = conn.execute("""
            SELECT 
                score_range,
                COUNT(*) as count
            FROM (
                SELECT 
                    CASE 
                        WHEN score < 0 THEN 'Negative'
                        WHEN score = 0 THEN 'Zero'
                        WHEN score BETWEEN 1 AND 5 THEN '1-5'
                        WHEN score BETWEEN 6 AND 10 THEN '6-10'
                        WHEN score BETWEEN 11 AND 20 THEN '11-20'
                        ELSE '20+'
                    END as score_range,
                    CASE 
                        WHEN score < 0 THEN 1
                        WHEN score = 0 THEN 2
                        WHEN score BETWEEN 1 AND 5 THEN 3
                        WHEN score BETWEEN 6 AND 10 THEN 4
                        WHEN score BETWEEN 11 AND 20 THEN 5
                        ELSE 6
                    END as sort_order
                FROM posts
                WHERE post_type_id = 1
            ) sub
            GROUP BY score_range, sort_order
            ORDER BY sort_order
        """).fetchall()
        
        conn.close()
        
        return templates.TemplateResponse("analytics.html", {
            "request": request,
            "posts_over_time": posts_over_time,
            "top_users": top_users,
            "popular_tags": popular_tags,
            "score_distribution": score_distribution
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/tags", response_class=JSONResponse)
async def api_tags(search: Optional[str] = None):
    """API endpoint for tag search (for autocomplete)"""
    conn = get_db()
    
    try:
        if search:
            query = """
                SELECT tag_name, count 
                FROM tags 
                WHERE tag_name ILIKE ? 
                ORDER BY count DESC 
                LIMIT 10
            """
            tags = conn.execute(query, [f"%{search}%"]).fetchall()
        else:
            query = """
                SELECT tag_name, count 
                FROM tags 
                ORDER BY count DESC 
                LIMIT 20
            """
            tags = conn.execute(query).fetchall()
        
        conn.close()
        
        return {"tags": [{"name": tag[0], "count": tag[1]} for tag in tags]}
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
