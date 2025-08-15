# StackSlice

A web application to slice and dice data from Stack Overflow and other Stack Exchange network sites. This project provides an intuitive interface for exploring and analyzing Stack Exchange data dumps.

## Features

- **Data Import**: Automatically imports Stack Exchange XML data dumps into a DuckDB database
- **Post Browser**: Browse questions and answers with search, filtering, and sorting
- **User Explorer**: View user profiles, reputation, and statistics
- **Analytics Dashboard**: Interactive charts and insights about the community
- **Responsive Design**: Modern, mobile-friendly interface built with Bootstrap
- **Fast Performance**: Powered by DuckDB for lightning-fast queries

## Tech Stack

- **Backend**: FastAPI (Python web framework)
- **Database**: DuckDB (in-process analytical database)
- **Frontend**: HTML/CSS/JavaScript with Bootstrap 5
- **Charts**: Chart.js for data visualization
- **Templates**: Jinja2 templating engine

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Stack Exchange data dump (XML files)

### Installation

1. **Clone or download this repository**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Prepare data**: 
   - Download a Stack Exchange data dump from [Stack Exchange Data Dump](https://archive.org/details/stackexchange)
   - Extract the files to `data/ai.stackexchange.com/` (or modify the path in the code)
   - Ensure these files are present:
     - Posts.xml
     - Users.xml
     - Comments.xml
     - Votes.xml
     - Badges.xml
     - Tags.xml

4. **Start the application**:
   ```bash
   python run.py
   ```

The application will:
- Check for required data files
- Import data into DuckDB (first run only)
- Start the web server at http://localhost:8000

## Project Structure

```
StackSlice/
├── templates/                    # HTML templates
│   ├── base.html
│   ├── home.html
│   ├── posts.html
│   ├── post_detail.html
│   ├── users.html
│   └── analytics.html
├── static/
│   └── style.css                # Custom CSS styles
├── data_importer.py             # Data import logic
├── main.py                      # FastAPI application
├── run.py                       # Startup script
├── requirements.txt             # Python dependencies
└── README.md
```

## Usage

### Home Page
- Overview statistics of the Stack Exchange site
- Quick navigation to different sections
- Search functionality

### Posts Browser
- Browse all posts (questions and answers)
- Filter by post type (questions/answers)
- Search in titles and content
- Sort by date, score, or views
- Pagination for large datasets

### Post Details
- View complete post content with formatting
- See all answers (for questions)
- View comments and metadata
- Score and voting information

### Users Directory
- Browse all users with their statistics
- Sort by reputation, join date, or name
- Search users by display name
- View user reputation and voting history

### Analytics Dashboard
- Interactive charts showing:
  - Questions posted over time
  - Score distribution
  - Popular tags
  - Top contributing users
- Community insights and statistics

## Data Import

The application automatically handles data import on first run. The import process:

1. **Parses XML files** using Python's ElementTree
2. **Cleans and normalizes data** (dates, HTML content, etc.)
3. **Creates optimized database schema** in DuckDB
4. **Batch inserts data** for performance
5. **Creates indexes** for fast querying

## Database Schema

The application creates the following tables:

- **posts**: Questions and answers with metadata
- **users**: User profiles and statistics  
- **comments**: Post comments
- **votes**: Voting records
- **badges**: User badges and achievements
- **tags**: Tag definitions and usage counts

## API Endpoints

- `GET /`: Home page with statistics
- `GET /posts`: Browse posts with filtering/pagination
- `GET /posts/{id}`: View specific post details
- `GET /users`: Browse users with search/sort
- `GET /analytics`: Analytics dashboard
- `GET /api/tags`: JSON API for tag search

## Customization

### Adding New Sites
To analyze different Stack Exchange sites:

1. Download the data dump for your target site
2. Update the data directory path in `run.py` and `data_importer.py`
3. Modify branding/titles in templates as needed

### Extending Analytics
Add new analytics by:

1. Adding SQL queries in the `/analytics` route in `main.py`
2. Creating new chart components in `analytics.html`
3. Styling with Chart.js

### Custom Styling
- Edit `static/style.css` for visual customizations
- Templates use Bootstrap 5 classes for responsive design
- Icons from Bootstrap Icons

## Performance

- **DuckDB** provides excellent performance for analytical queries
- **Pagination** limits memory usage for large datasets  
- **Indexes** on commonly queried fields
- **Batch processing** during data import
- **Responsive design** works well on mobile devices

## Contributing

This is a demonstration project, but feel free to:

- Add support for additional Stack Exchange sites
- Improve the analytics with more charts and insights
- Enhance the search functionality
- Add user authentication and personalization
- Implement caching for better performance

## License

This project is provided under the MIT License. See the LICENSE file for details.

## Acknowledgments

- Stack Exchange for providing public data dumps
- The FastAPI, DuckDB, and Bootstrap communities
- All Stack Exchange contributors whose data makes this possible
