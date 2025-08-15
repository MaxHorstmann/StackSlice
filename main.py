from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
import duckdb
from typing import Optional, List, Dict, Any
import html
import re
from datetime import datetime
from pathlib import Path

app = FastAPI(title="StackSlice", description="Slice and dice Stack Exchange data")

# Setup templates and static files
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Database connection
DB_PATH = "stackexchange.db"

def get_db():
    """Get database connection"""
    return duckdb.connect(DB_PATH)

def get_available_sites():
    """Get list of available sites in the database, with main sites first, then meta sites"""
    try:
        conn = get_db()
        sites = conn.execute("SELECT DISTINCT site FROM posts ORDER BY site").fetchall()
        conn.close()
        site_list = [site[0] for site in sites]
        
        # Sort to put main sites first, then meta sites
        main_sites = [s for s in site_list if not s.endswith('.meta.stackexchange.com')]
        meta_sites = [s for s in site_list if s.endswith('.meta.stackexchange.com')]
        
        return main_sites + meta_sites
    except:
        return []

def get_default_site():
    """Get the default site (first available site)"""
    sites = get_available_sites()
    return sites[0] if sites else "ai.stackexchange.com"

def validate_site(site: str) -> str:
    """Validate and return a valid site name"""
    available_sites = get_available_sites()
    if site and site in available_sites:
        return site
    return get_default_site()

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
async def home(request: Request, site: Optional[str] = Query(None)):
    """Home page with overview statistics"""
    site = validate_site(site)
    conn = get_db()
    
    try:
        # Get basic statistics for the selected site
        stats = {}
        
        # Total posts
        result = conn.execute("SELECT COUNT(*) as total FROM posts WHERE site = ?", [site]).fetchone()
        stats['total_posts'] = result[0] if result else 0
        
        # Total questions (PostTypeId = 1)
        result = conn.execute("SELECT COUNT(*) as total FROM posts WHERE site = ? AND post_type_id = 1", [site]).fetchone()
        stats['total_questions'] = result[0] if result else 0
        
        # Total answers (PostTypeId = 2)
        result = conn.execute("SELECT COUNT(*) as total FROM posts WHERE site = ? AND post_type_id = 2", [site]).fetchone()
        stats['total_answers'] = result[0] if result else 0
        
        # Total users
        result = conn.execute("SELECT COUNT(*) as total FROM users WHERE site = ?", [site]).fetchone()
        stats['total_users'] = result[0] if result else 0
        
        # Total comments
        result = conn.execute("SELECT COUNT(*) as total FROM comments WHERE site = ?", [site]).fetchone()
        stats['total_comments'] = result[0] if result else 0
        
        # Most recent activity
        result = conn.execute("""
            SELECT creation_date 
            FROM posts 
            WHERE site = ?
            ORDER BY creation_date DESC 
            LIMIT 1
        """, [site]).fetchone()
        stats['latest_activity'] = format_date(result[0]) if result else "N/A"
        
        # Top tags
        top_tags = conn.execute("""
            SELECT tag_name, count 
            FROM tags 
            WHERE site = ?
            ORDER BY count DESC 
            LIMIT 10
        """, [site]).fetchall()
        
        conn.close()
        
        return templates.TemplateResponse("home.html", {
            "request": request,
            "stats": stats,
            "top_tags": top_tags,
            "current_site": site,
            "available_sites": get_available_sites()
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
    sort: str = Query("recent"),
    site: Optional[str] = Query(None)
):
    """Browse posts with pagination and filtering"""
    site = validate_site(site)
    conn = get_db()
    
    try:
        offset = (page - 1) * limit
        
        # Build WHERE clause
        where_conditions = ["site = ?"]
        params = [site]
        
        if search:
            where_conditions.append("(title ILIKE ? OR body ILIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        if post_type == "questions":
            where_conditions.append("post_type_id = ?")
            params.append(1)
        elif post_type == "answers":
            where_conditions.append("post_type_id = ?")
            params.append(2)
        
        where_clause = " AND ".join(where_conditions)
        
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
            "next_page": page + 1 if page < total_pages else None,
            "current_site": site,
            "available_sites": get_available_sites()
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/posts/{post_id}", response_class=HTMLResponse)
async def post_detail(request: Request, post_id: int, site: Optional[str] = Query(None)):
    """View a specific post with its answers and comments"""
    site = validate_site(site)
    conn = get_db()
    
    try:
        # Get the main post
        post_query = """
            SELECT p.*, u.display_name as owner_name
            FROM posts p
            LEFT JOIN users u ON p.owner_user_id = u.id AND u.site = p.site
            WHERE p.id = ? AND p.site = ?
        """
        post_data = conn.execute(post_query, [post_id, site]).fetchone()
        
        if not post_data:
            raise HTTPException(status_code=404, detail="Post not found")
        
        # Format main post
        post = {
            'id': post_data[1],  # Updated index after adding site column
            'post_type_id': post_data[2],
            'title': post_data[12] or "No title",
            'body': post_data[7],
            'score': post_data[5],
            'view_count': post_data[6],
            'creation_date': format_date(post_data[4]),
            'owner_user_id': post_data[8],
            'owner_name': post_data[-1] or "Unknown User",
            'tags': extract_tags(post_data[13]),
            'answer_count': post_data[14],
            'comment_count': post_data[15]
        }
        
        # Get answers if this is a question
        answers = []
        if post['post_type_id'] == 1:
            answers_query = """
                SELECT p.*, u.display_name as owner_name
                FROM posts p
                LEFT JOIN users u ON p.owner_user_id = u.id AND u.site = p.site
                WHERE p.parent_id = ? AND p.post_type_id = 2 AND p.site = ?
                ORDER BY p.score DESC, p.creation_date ASC
            """
            answers_data = conn.execute(answers_query, [post_id, site]).fetchall()
            
            for answer_data in answers_data:
                answer = {
                    'id': answer_data[1],  # Updated index after adding site column
                    'body': answer_data[7],
                    'score': answer_data[5],
                    'creation_date': format_date(answer_data[4]),
                    'owner_user_id': answer_data[8],
                    'owner_name': answer_data[-1] or "Unknown User",
                    'is_accepted': answer_data[1] == post_data[3]  # accepted_answer_id
                }
                answers.append(answer)
        
        # Get comments for the post
        comments_query = """
            SELECT c.*, u.display_name as user_name
            FROM comments c
            LEFT JOIN users u ON c.user_id = u.id AND u.site = c.site
            WHERE c.post_id = ? AND c.site = ?
            ORDER BY c.creation_date ASC
        """
        comments_data = conn.execute(comments_query, [post_id, site]).fetchall()
        
        comments = []
        for comment_data in comments_data:
            comment = {
                'id': comment_data[1],  # Updated index after adding site column
                'text': comment_data[4],
                'score': comment_data[3],
                'creation_date': format_date(comment_data[5]),
                'user_id': comment_data[7],
                'user_name': comment_data[-1] or "Unknown User"
            }
            comments.append(comment)
        
        conn.close()
        
        return templates.TemplateResponse("post_detail.html", {
            "request": request,
            "post": post,
            "answers": answers,
            "comments": comments,
            "current_site": site,
            "available_sites": get_available_sites()
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
    sort: str = Query("reputation"),
    site: Optional[str] = Query(None)
):
    """Browse users with pagination and filtering"""
    site = validate_site(site)
    conn = get_db()
    
    try:
        offset = (page - 1) * limit
        
        # Build WHERE clause
        where_conditions = ["site = ?"]
        params = [site]
        
        if search:
            where_conditions.append("display_name ILIKE ?")
            params.append(f"%{search}%")
        
        where_clause = " AND ".join(where_conditions)
        
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
            "next_page": page + 1 if page < total_pages else None,
            "current_site": site,
            "available_sites": get_available_sites()
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request, site: Optional[str] = Query(None)):
    """Analytics dashboard with charts and insights"""
    site = validate_site(site)
    conn = get_db()
    
    try:
        # Posts over time
        posts_over_time = conn.execute("""
            SELECT 
                DATE_TRUNC('month', creation_date) as month,
                COUNT(*) as count
            FROM posts 
            WHERE post_type_id = 1 AND site = ?
            GROUP BY DATE_TRUNC('month', creation_date)
            ORDER BY month
        """, [site]).fetchall()
        
        # Top users by reputation
        top_users = conn.execute("""
            SELECT display_name, reputation, up_votes, down_votes
            FROM users
            WHERE site = ?
            ORDER BY reputation DESC
            LIMIT 10
        """, [site]).fetchall()
        
        # Popular tags
        popular_tags = conn.execute("""
            SELECT tag_name, count
            FROM tags
            WHERE site = ?
            ORDER BY count DESC
            LIMIT 15
        """, [site]).fetchall()
        
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
                WHERE post_type_id = 1 AND site = ?
            ) sub
            GROUP BY score_range, sort_order
            ORDER BY sort_order
        """, [site]).fetchall()
        
        conn.close()
        
        return templates.TemplateResponse("analytics.html", {
            "request": request,
            "posts_over_time": posts_over_time,
            "top_users": top_users,
            "popular_tags": popular_tags,
            "score_distribution": score_distribution,
            "current_site": site,
            "available_sites": get_available_sites()
        })
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/tags", response_class=JSONResponse)
async def api_tags(search: Optional[str] = None, site: Optional[str] = Query(None)):
    """API endpoint for tag search (for autocomplete)"""
    site = validate_site(site)
    conn = get_db()
    
    try:
        if search:
            query = """
                SELECT tag_name, count 
                FROM tags 
                WHERE tag_name ILIKE ? AND site = ?
                ORDER BY count DESC 
                LIMIT 10
            """
            tags = conn.execute(query, [f"%{search}%", site]).fetchall()
        else:
            query = """
                SELECT tag_name, count 
                FROM tags 
                WHERE site = ?
                ORDER BY count DESC 
                LIMIT 20
            """
            tags = conn.execute(query, [site]).fetchall()
        
        conn.close()
        
        return {"tags": [{"name": tag[0], "count": tag[1]} for tag in tags]}
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/switch-site/{new_site}")
async def switch_site(new_site: str, request: Request):
    """Switch to a different site"""
    # Validate the site exists
    available_sites = get_available_sites()
    if new_site not in available_sites:
        raise HTTPException(status_code=404, detail="Site not found")
    
    # Get the referring page to redirect back to
    referer = request.headers.get("referer", "/")
    
    # Parse the referer URL to maintain the current page but switch site
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    
    parsed = urlparse(referer)
    query_params = parse_qs(parsed.query)
    
    # Update or add the site parameter
    query_params["site"] = [new_site]
    
    # Rebuild the URL
    new_query = urlencode(query_params, doseq=True)
    new_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))
    
    return RedirectResponse(url=new_url, status_code=302)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
