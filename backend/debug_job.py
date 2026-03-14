import asyncio
from sqlalchemy import select, func
from db.database import get_db, ScrapeJob, Article

async def debug():
    async with get_db() as db:
        res = await db.execute(select(ScrapeJob).order_by(ScrapeJob.started_at.desc()).limit(5))
        jobs = res.scalars().all()
        for job in jobs:
            print(f"JOB {job.id}:")
            print(f"  Status: {job.status}")
            print(f"  Phase: {job.current_phase}")
            print(f"  Found: {job.total_found}")
            print(f"  Scraped: {job.total_scraped}")
            print("-" * 20)
            
            # Check articles for this job
            res_art = await db.execute(select(func.count(Article.id)).where(Article.scrape_job_id == job.id))
            print(f"  Articles in DB: {res_art.scalar()}")
            print("\n")

if __name__ == "__main__":
    asyncio.run(debug())
