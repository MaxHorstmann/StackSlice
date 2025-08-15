# StackSlice 🔧

> **Slice and dice data from Stack Exchange network sites**

A modern web application for exploring and analyzing Stack Exchange data dumps with powerful multi-site support, automatic data downloading, and interactive analytics.

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/your-template)

## ✨ Features

### 🌐 **Multi-Site Support**
- Browse multiple Stack Exchange sites in one application
- Site switcher in navigation for seamless switching
- Currently supports AI Stack Exchange and AI Meta Stack Exchange
- Easy to add more sites via environment configuration

### 📊 **Comprehensive Analytics**
- Interactive charts and visualizations
- Community insights and trends
- Post engagement metrics
- User reputation analysis

### 🔍 **Advanced Data Explorer**
- **Posts Browser**: Search, filter, and sort questions/answers
- **User Directory**: Explore user profiles and statistics  
- **Real-time Search**: Fast full-text search across posts
- **Responsive Design**: Works perfectly on desktop and mobile

### ⚡ **Automated Data Management**
- **Auto-Download**: Automatically downloads Stack Exchange data from archive.org
- **Smart Import**: Detects existing data and only imports what's needed
- **Multi-Site Database**: Unified schema supporting multiple Stack Exchange sites
- **Fast Performance**: Powered by DuckDB for lightning-fast analytical queries

## 🚀 **Live Demo**

🌍 **[View Live Application](https://your-railway-app.railway.app)** *(Deploy to see your URL)*

## 🛠️ **Tech Stack**

- **Backend**: FastAPI + Python 3.13
- **Database**: DuckDB 1.3.2 (embedded analytical database)
- **Frontend**: Bootstrap 5 + Chart.js + Jinja2 templates
- **Data Processing**: XML parsing with automatic download from archive.org
- **Deployment**: Railway.app ready with Procfile and railway.json

## ⚡ **Quick Start**

### **Option 1: Deploy to Railway (Recommended)**

1. **Click the Railway button above** or visit [Railway.app](https://railway.app)
2. **Connect your GitHub** and select this repository
3. **Deploy**: Railway automatically handles everything!
4. **Done**: Your app will be live in ~5-10 minutes

### **Option 2: Run Locally**

```bash
# Clone the repository
git clone https://github.com/MaxHorstmann/StackSlice.git
cd StackSlice

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the application
python run.py
```

The application will:
- ✅ **Auto-download** Stack Exchange data from archive.org
- ✅ **Import data** into DuckDB (first run takes ~5-10 minutes)
- ✅ **Start web server** at http://localhost:8000

## 🗂️ **Project Structure**

```
StackSlice/
├── 📁 templates/          # Jinja2 HTML templates
│   ├── base.html         # Base template with navigation
│   ├── home.html         # Site overview with stats
│   ├── posts.html        # Posts browser with search
│   ├── users.html        # User directory
│   └── analytics.html    # Charts and insights
├── 📁 static/
│   └── style.css         # Custom styles
├── 📁 data/              # Auto-downloaded Stack Exchange data
│   ├── ai.stackexchange.com/
│   └── ai.meta.stackexchange.com/
├── 🐍 main.py            # FastAPI web application
├── 🐍 data_importer.py   # Multi-site data import logic
├── 🐍 run.py             # Startup script with auto-download
├── 📋 requirements.txt   # Python dependencies
├── 🚂 Procfile          # Railway deployment config
├── ⚙️  railway.json      # Railway app configuration
└── 📖 README.md         # This file
```

## 🌐 **Multi-Site Configuration**

Add more Stack Exchange sites by setting the environment variable:

```bash
export STACKEXCHANGE_SITES="ai.stackexchange.com,ai.meta.stackexchange.com,datascience.stackexchange.com"
```

Or modify the `DEFAULT_SITES` list in `run.py`.

## 🔧 **Key Features**

### **Automatic Data Management**
- Downloads Stack Exchange 7z archives from archive.org
- Extracts and imports XML data automatically
- Supports incremental imports (won't re-import existing data)
- Multi-site database schema with site-specific data

### **Smart Navigation**
- Site switcher in top navigation
- Main sites listed first, meta sites second
- Full site names displayed for clarity
- Contextual navigation that maintains current site

### **Analytics Dashboard**
- Questions posted over time
- Score distribution analysis  
- Popular tags visualization
- Top users by reputation
- Community engagement metrics

### **Advanced Search & Filtering**
- Full-text search across post titles and content
- Filter by post type (questions/answers)
- Sort by date, score, views, or relevance
- Pagination for large datasets
- Real-time search suggestions

## 🗄️ **Database Schema**

Multi-site enabled schema with `site` column in all tables:

```sql
-- All tables include 'site' column for multi-site support
posts(id, site, post_type_id, title, body, score, view_count, creation_date, ...)
users(id, site, display_name, reputation, creation_date, ...)
comments(id, site, post_id, user_id, text, creation_date, ...)
votes(id, site, post_id, vote_type_id, creation_date, ...)
badges(id, site, user_id, name, date, class, ...)
tags(id, site, tag_name, count, excerpt_post_id, ...)
```

## 🚀 **Deployment**

### **Railway (Recommended)**
- ✅ One-click deployment
- ✅ Automatic builds from GitHub
- ✅ Persistent file storage for DuckDB
- ✅ Environment variable configuration
- ✅ Custom domain support

### **Other Platforms**
The app includes standard deployment files for various platforms:
- `Procfile` for Heroku-style platforms
- `requirements.txt` with pinned versions
- `railway.json` for Railway-specific configuration
- Environment-based configuration (PORT, etc.)

## 🤝 **Contributing**

Contributions welcome! Ideas for enhancement:

- 📈 **More Analytics**: Additional charts and insights
- 🔍 **Advanced Search**: Elasticsearch integration
- 🏷️ **Tag Analysis**: Tag relationship mapping
- 👥 **User Insights**: User behavior analytics
- 🌍 **More Sites**: Support for additional Stack Exchange sites
- 🎨 **UI/UX**: Enhanced design and user experience

## 📄 **License**

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 **Acknowledgments**

- **Stack Exchange** for providing public data dumps
- **Archive.org** for hosting accessible data archives
- **FastAPI**, **DuckDB**, and **Bootstrap** communities
- All Stack Exchange contributors whose knowledge makes this possible

---

**Built with ❤️ by [MaxHorstmann](https://github.com/MaxHorstmann)**
