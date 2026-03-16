"""
Distributed CLI Runner for News Intelligence Scraper.
Synchronous version for gevent/distributed systems.
"""

import sys
import argparse
from datetime import datetime, date
from scraper.engine import run_scrape_job, discover_articles, SECTOR_KEYWORDS, SEARCH_MODIFIERS, REGION_MAP, scrape_only
from sqlalchemy import text
from db.database import get_db_sync, init_db_sync, Article, ScrapeJob

def run_discovery(sector: str, region: str, day_str: str):
    init_db_sync()
    day = date.fromisoformat(day_str)
    
    keywords = SECTOR_KEYWORDS.get(sector.lower(), [sector])
    geo = REGION_MAP.get(region.lower(), {"geo": "IN"})["geo"]
    job_id = f"dist-{int(datetime.now().timestamp())}"
    cumulative = set()
    
    discovered = discover_articles(keywords, day, geo, region, job_id, cumulative)
    print(f"DISCOVERY_COUNT={len(discovered)}")
    
    with get_db_sync() as db:
        db.execute(text("""
            INSERT INTO scrape_jobs (id, sector, region, date_from, date_to, status, total_found, started_at) 
            VALUES (:id, :sector, :region, :date_from, :date_to, :status, :total, :started)
        """), {
            "id": job_id, "sector": sector, "region": region, 
            "date_from": day_str, "date_to": day_str, 
            "status": "discovery_complete", "total": len(discovered),
            "started": datetime.now()
        })
        
        for article in discovered:
            val_dict = {
                "title": article["title"], "url": article["url"], "agency": article.get("agency"),
                "published_at": datetime.fromisoformat(article["published_at"]) if isinstance(article["published_at"], str) else article["published_at"],
                "sector": sector, "region": region, 
                "scrape_job_id": job_id
            }
            db.execute(text("""
                INSERT INTO articles (title, url, agency, published_at, sector, region, scrape_job_id)
                VALUES (:title, :url, :agency, :published_at, :sector, :region, :scrape_job_id)
                ON CONFLICT (url) DO NOTHING
            """), val_dict)
            
        db.commit()
    print(f"JOB_ID={job_id}")

def run_worker(job_id: str, chunk_index: int, total_chunks: int):
    init_db_sync()
    with get_db_sync() as db:
        res = db.execute(text("SELECT * FROM articles WHERE scrape_job_id=:job_id AND full_body IS NULL"), {"job_id": job_id})
        all_pending = res.mappings().all()
        
        if not all_pending:
            print("No articles to process.")
            return

        chunk_size = len(all_pending) // total_chunks
        start = chunk_index * chunk_size
        end = start + chunk_size if chunk_index < total_chunks - 1 else len(all_pending)
        
        my_chunk = all_pending[start:end]
        print(f"Worker {chunk_index}/{total_chunks} picking up {len(my_chunk)} articles.")
        
        for art in my_chunk:
            print(f"Scraping: {art['title']}")
            try:
                art_dict = dict(art)
                scrape_only(art_dict, job_id, art_dict["sector"], art_dict["region"], "admin")
            except Exception as e:
                print(f"Error scraping {art['title']}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["discovery", "worker"], required=True)
    parser.add_argument("--sector", default="artificial intelligence")
    parser.add_argument("--region", default="india")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--job_id", help="Job ID for workers")
    parser.add_argument("--index", type=int, help="Matrix chunk index")
    parser.add_argument("--total", type=int, help="Total Matrix chunks")
    
    args = parser.parse_args()
    
    try:
        if args.mode == "discovery":
            run_discovery(args.sector, args.region, args.date)
        elif args.mode == "worker":
            if not args.job_id: 
                print("Error: job_id required for workers")
                sys.exit(1)
            run_worker(args.job_id, args.index, args.total)
    except KeyboardInterrupt:
        print("\n[!] Shutdown requested by user.")
        sys.exit(0)
