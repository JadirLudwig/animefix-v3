from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from fastapi.responses import PlainTextResponse, RedirectResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager
import logging
from typing import List

from .database import engine, Base, get_db
from .models import Anime, Episode
from .schemas import AnimeCreate, AnimeResponse
from .worker import start_background_jobs, sync_anime_updates, auto_refresh_episode
import httpx
from fake_useragent import UserAgent
from .validator import is_link_alive

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create tables
Base.metadata.create_all(bind=engine)

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing APScheduler...")
    start_background_jobs(scheduler)
    scheduler.start()
    
    # Lifespan: Start scheduler
    yield
    
    logger.info("Shutting down APScheduler...")
    scheduler.shutdown()

app = FastAPI(title="Anime IPTV System", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/")
async def root():
    return FileResponse("app/static/index.html")

@app.get("/manifest.json")
async def manifest_pwa():
    return FileResponse("app/static/manifest.json")

@app.get("/service-worker.js")
async def service_worker_pwa():
    return FileResponse("app/static/service-worker.js")

@app.get("/admin")
async def admin_page():
    return FileResponse("app/static/admin.html")

@app.post("/api/animes", response_model=AnimeResponse)
async def create_anime(anime: AnimeCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    db_anime = db.query(Anime).filter(Anime.base_url == anime.base_url).first()
    if not db_anime:
        db_anime = Anime(name="Pending Anime...", base_url=anime.base_url)
        db.add(db_anime)
        db.commit()
        db.refresh(db_anime)
        
    # Trigger episode extraction in background
    background_tasks.add_task(sync_anime_updates, db_anime.id)
    return db_anime

@app.get("/api/animes", response_model=List[AnimeResponse])
async def read_animes(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    animes = db.query(Anime).offset(skip).limit(limit).all()
    return animes

@app.post("/api/animes/{anime_id}/sync")
async def sync_anime_endpoint(anime_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    anime = db.query(Anime).filter(Anime.id == anime_id).first()
    if not anime:
        raise HTTPException(status_code=404, detail="Anime not found")
    
    background_tasks.add_task(sync_anime_updates, anime_id)
    return {"message": "Sync started"}

@app.delete("/api/animes/{anime_id}")
async def delete_anime(anime_id: int, db: Session = Depends(get_db)):
    anime = db.query(Anime).filter(Anime.id == anime_id).first()
    if not anime:
        raise HTTPException(status_code=404, detail="Anime not found")
    
    # Delete all episodes first, then the anime
    db.query(Episode).filter(Episode.anime_id == anime_id).delete()
    db.delete(anime)
    db.commit()
    return {"message": f"Anime '{anime.name}' and all its episodes deleted."}


@app.get("/stream/{episode_id}")
async def get_stream(episode_id: int, request: Request, db: Session = Depends(get_db)):
    ep = db.query(Episode).filter(Episode.id == episode_id).first()
    if not ep:
        raise HTTPException(status_code=404, detail="Episode not found")

    # YouTube embeds: redirect directly, no validation needed
    if ep.media_type == 'youtube' and ep.stream_url:
        return RedirectResponse(url=ep.stream_url)
        
    # Check if stream URL is provided and valid
    is_alive = False
    if ep.stream_url:
        is_alive = await is_link_alive(ep.stream_url)
        
    if not is_alive or ep.status == "Pending":
        logger.info(f"Stream is missing or dead for Episode {ep.number} of Anime {ep.anime_id}. Scraping now...")
        success = await auto_refresh_episode(episode_id)
        if not success:
            raise HTTPException(status_code=503, detail="Stream is currently unavailable and could not be scraped.")
            
        db.refresh(ep)
        
    if not ep.stream_url:
        raise HTTPException(status_code=404, detail="Could not capture stream link from site.")
        
    # Only proxy direct video links (mp4/m3u8), not YouTube iframes
    if ep.media_type == 'youtube':
        return RedirectResponse(url=ep.stream_url)

    # STREAM PROXY: Masquerade as a browser to avoid Referer/UA blocks
    ua = UserAgent(os='linux', browsers=['chrome'])
    headers = {
        "User-Agent": ua.random,
        "Referer": ep.page_url, # Pretend we are coming from the anime site
        "Accept": "*/*"
    }
    
    # Handle Range requests from the player (important for mp4 seeking)
    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header

    # Set status code (206 for partial, 200 for full)
    status_code = 206 if range_header else 200

    # HLS REWRITER: If it's an m3u8, we fetch it, rewrite relative links, and return it
    if ep.media_type == '.m3u8':
        try:
            async with httpx.AsyncClient(follow_redirects=True, verify=False) as client:
                resp = await client.get(ep.stream_url, headers=headers, timeout=20)
                if resp.status_code == 200:
                    content = resp.text
                    from urllib.parse import urljoin
                    base_url_stream = ep.stream_url
                    new_lines = []
                    for line in content.splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            line = urljoin(base_url_stream, line)
                        new_lines.append(line)
                    
                    return PlainTextResponse(
                        "\n".join(new_lines), 
                        media_type="application/x-mpegURL"
                    )
        except Exception as e:
            logger.error(f"HLS Rewriter error: {e}")

    # For everything else (mp4), use the streaming proxy
    async def generate():
        async with httpx.AsyncClient(follow_redirects=True, verify=False) as client:
            try:
                async with client.stream("GET", ep.stream_url, headers=headers, timeout=30) as resp:
                    if resp.status_code >= 400:
                        logger.error(f"Proxy failed with status {resp.status_code} for {ep.stream_url}")
                        return
                    
                    async for chunk in resp.aiter_bytes():
                        yield chunk
            except Exception as e:
                logger.error(f"Stream Proxy connection error: {e}")

    return StreamingResponse(
        generate(), 
        status_code=status_code, 
        media_type="video/mp4" if ep.media_type == '.mp4' else "application/x-mpegURL",
        headers={"Accept-Ranges": "bytes"}
    )

