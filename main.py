# main.py (condensed version with imports)
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Query, Depends, HTTPException, status
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import uuid
from redis import Redis
from rq import Queue
from worker import process_file
from db import get_db, File, Chunk, FileChunk, init_db
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, text
from datetime import datetime
import logging
from dotenv import load_dotenv
import mimetypes
from PIL import Image
import io
from collections import defaultdict

# Import centralized documentation
from docs import API_TITLE, API_VERSION, API_DESCRIPTION, API_TAGS, ENDPOINT_DOCS, RESPONSES

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app with centralized docs
app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description=API_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "API Support",
        "url": "http://localhost:8000",
        "email": "support@example.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    swagger_ui_parameters={
        "defaultModelsExpandDepth": -1,
        "defaultModelExpandDepth": 2,
        "docExpansion": "list",
        "filter": True,
        "tryItOutEnabled": True,
        "syntaxHighlight": {"theme": "monokai"}
    }
)

app.openapi_tags = API_TAGS

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "temp")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1024 * 1024))
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_conn = Redis.from_url(REDIS_URL)
small_queue = Queue('uploads', connection=redis_conn)
large_queue = Queue('large-files', connection=redis_conn)
cleanup_queue = Queue('cleanup', connection=redis_conn)

@app.on_event("startup")
async def startup_event():
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    redis_conn.close()
    logger.info("Shutdown complete")

# =========================
# File Endpoints
# =========================

@app.post("/upload", status_code=status.HTTP_202_ACCEPTED, tags=["Files"], 
          summary=ENDPOINT_DOCS["upload"]["summary"],
          description=ENDPOINT_DOCS["upload"]["description"],
          response_description=ENDPOINT_DOCS["upload"]["response_description"],
          responses=RESPONSES["upload"])
async def upload(file: UploadFile = FastAPIFile(...)):
    """Upload a file for deduplicated storage"""
    try:
        file_id = str(uuid.uuid4())
        temp_path = os.path.join(UPLOAD_DIR, file_id)
        
        with open(temp_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        
        file_size = os.path.getsize(temp_path)
        
        if file_size > 50 * 1024 * 1024:
            queue = large_queue
            job_timeout = 7200
        else:
            queue = small_queue
            job_timeout = 3600
        
        job = queue.enqueue(process_file, temp_path, file.filename, file_id,
                           job_timeout=job_timeout, result_ttl=5000)
        
        return {
            "file_id": file_id, "job_id": job.id, "queue": queue.name,
            "filename": file.filename, "size": file_size, "status": "queued"
        }
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files", tags=["Files"],
         summary=ENDPOINT_DOCS["list_files"]["summary"],
         description=ENDPOINT_DOCS["list_files"]["description"],
         response_description=ENDPOINT_DOCS["list_files"]["response_description"])
def list_files(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort_by: str = Query("created_at", regex="^(created_at|filename|size)$"),
    order: str = Query("desc", regex="^(asc|desc)$"),
    db: Session = Depends(get_db)
):
    try:
        query = db.query(File, func.count(FileChunk.id).label('chunk_count')).outerjoin(
            FileChunk, File.id == FileChunk.file_id).group_by(File.id)
        
        sort_column = File.created_at if sort_by == "created_at" else File.size if sort_by == "size" else func.lower(File.filename)
        query = query.order_by(desc(sort_column) if order == "desc" else sort_column)
        
        total = db.query(File).count()
        results = query.offset(skip).limit(limit).all()
        
        return {
            "total": total, "skip": skip, "limit": limit,
            "files": [{"file_id": f.id, "filename": f.filename, "size": f.size,
                      "created_at": f.created_at.isoformat(), "chunk_count": cnt} 
                      for f, cnt in results]
        }
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/search", tags=["Files"],
         summary=ENDPOINT_DOCS["search_files"]["summary"],
         description=ENDPOINT_DOCS["search_files"]["description"],
         response_description=ENDPOINT_DOCS["search_files"]["response_description"])
def search_files(
    q: str = Query(""), min_size: int = Query(None, ge=0), max_size: int = Query(None, ge=0),
    skip: int = Query(0), limit: int = Query(100, ge=1, le=1000), db: Session = Depends(get_db)
):
    try:
        query = db.query(File, func.count(FileChunk.id).label('chunk_count')).outerjoin(
            FileChunk, File.id == FileChunk.file_id).group_by(File.id)
        if q: query = query.filter(File.filename.ilike(f"%{q}%"))
        if min_size: query = query.filter(File.size >= min_size)
        if max_size: query = query.filter(File.size <= max_size)
        query = query.order_by(desc(File.created_at))
        total = query.count()
        results = query.offset(skip).limit(limit).all()
        return {
            "total": total, "skip": skip, "limit": limit,
            "files": [{"file_id": f.id, "filename": f.filename, "size": f.size,
                      "created_at": f.created_at.isoformat(), "chunk_count": cnt}
                      for f, cnt in results]
        }
    except Exception as e:
        logger.error(f"Error searching files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{file_id}", tags=["Files"],
         summary=ENDPOINT_DOCS["download"]["summary"],
         description=ENDPOINT_DOCS["download"]["description"],
         response_description=ENDPOINT_DOCS["download"]["response_description"],
         responses=RESPONSES["download"])
def download_file(file_id: str, db: Session = Depends(get_db)):
    try:
        file = db.query(File).filter(File.id == file_id).first()
        if not file: raise HTTPException(status_code=404, detail="File not found")
        
        file_chunks = db.query(FileChunk.order_index, Chunk.path, Chunk.hash).join(
            Chunk, FileChunk.chunk_hash == Chunk.hash).filter(
            FileChunk.file_id == file_id).order_by(FileChunk.order_index).all()
        
        if not file_chunks: raise HTTPException(status_code=404, detail="No chunks found")
        
        def file_stream():
            for _, chunk_path, chunk_hash in file_chunks:
                try:
                    if os.path.exists(chunk_path):
                        with open(chunk_path, "rb") as f: yield f.read()
                    else: yield b""
                except: yield b""
        
        return StreamingResponse(file_stream(), media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file.filename}",
                    "Content-Length": str(file.size), "X-File-ID": file_id})
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# Statistics & Health
# =========================

@app.get("/stats", tags=["Statistics"],
         summary=ENDPOINT_DOCS["stats"]["summary"],
         description=ENDPOINT_DOCS["stats"]["description"],
         response_description=ENDPOINT_DOCS["stats"]["response_description"])
def get_stats(db: Session = Depends(get_db)):
    try:
        total_files = db.query(File).count()
        total_size = db.query(func.sum(File.size)).scalar() or 0
        chunk_stats = db.query(
            func.count(Chunk.hash).label('total_chunks'),
            func.sum(Chunk.size).label('total_size'),
            func.sum(Chunk.ref_count).label('total_refs'),
            func.avg(Chunk.ref_count).label('avg_refs'),
            func.max(Chunk.ref_count).label('max_refs'),
            func.count(func.nullif(Chunk.ref_count, 1)).label('shared_chunks')
        ).first()
        top_chunks = db.query(Chunk.hash, Chunk.ref_count, Chunk.size).order_by(desc(Chunk.ref_count)).limit(10).all()
        physical_size = chunk_stats.total_size or 0
        space_saved = total_size - physical_size
        return {
            "files": {"total": total_files, "total_size_bytes": total_size,
                     "total_size_mb": round(total_size / (1024*1024), 2),
                     "total_size_gb": round(total_size / (1024**3), 2)},
            "chunks": {"total_unique_chunks": chunk_stats.total_chunks or 0,
                      "total_references": chunk_stats.total_refs or 0,
                      "shared_chunks": chunk_stats.shared_chunks or 0,
                      "max_ref_count": chunk_stats.max_refs or 0,
                      "avg_ref_count": round(chunk_stats.avg_refs or 0, 2),
                      "top_chunks": [{"hash": h[:16]+"...", "ref_count": rc, "size_bytes": s} 
                                    for h, rc, s in top_chunks]},
            "storage": {"logical_size_bytes": total_size, "physical_size_bytes": physical_size,
                       "space_saved_bytes": space_saved, "space_saved_mb": round(space_saved/(1024*1024), 2),
                       "space_saved_gb": round(space_saved/(1024**3), 2),
                       "deduplication_ratio": round(physical_size/total_size if total_size>0 else 1, 3),
                       "efficiency_percent": round((1-physical_size/total_size)*100 if total_size>0 else 0, 2)}
        }
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health", tags=["System"],
         summary=ENDPOINT_DOCS["health"]["summary"],
         description=ENDPOINT_DOCS["health"]["description"],
         response_description=ENDPOINT_DOCS["health"]["response_description"])
def health_check(db: Session = Depends(get_db)):
    status = {"status": "healthy", "timestamp": datetime.utcnow().isoformat(), "services": {}}
    try:
        redis_conn.ping()
        status["services"]["redis"] = "healthy"
    except Exception as e:
        status["services"]["redis"] = f"unhealthy: {e}"
        status["status"] = "degraded"
    try:
        db.execute(text("SELECT 1"))
        status["services"]["database"] = "healthy"
    except Exception as e:
        status["services"]["database"] = f"unhealthy: {e}"
        status["status"] = "degraded"
    return status

# =========================
# Image Endpoints
# =========================

@app.get("/image/{file_id}", tags=["Images"],
         summary=ENDPOINT_DOCS["image_display"]["summary"],
         description=ENDPOINT_DOCS["image_display"]["description"],
         response_description=ENDPOINT_DOCS["image_display"]["response_description"],
         responses=RESPONSES["image"])
async def display_image(file_id: str, db: Session = Depends(get_db)):
    try:
        file = db.query(File).filter(File.id == file_id).first()
        if not file: raise HTTPException(status_code=404, detail="File not found")
        if not file.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
            raise HTTPException(status_code=400, detail="File is not an image")
        file_chunks = db.query(FileChunk).filter_by(file_id=file_id).order_by(FileChunk.order_index).all()
        if not file_chunks: raise HTTPException(status_code=404, detail="No chunks found")
        def image_stream():
            for fc in file_chunks:
                chunk = db.query(Chunk).filter_by(hash=fc.chunk_hash).first()
                if chunk and os.path.exists(chunk.path):
                    with open(chunk.path, "rb") as f: yield f.read()
        content_type = "image/png" if file.filename.lower().endswith('.png') else "image/jpeg"
        return StreamingResponse(image_stream(), media_type=content_type,
            headers={"Content-Disposition": f"inline; filename={file.filename}", "Cache-Control": "public, max-age=3600"})
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Error displaying image: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/gallery", response_class=HTMLResponse, tags=["Images"],
         summary=ENDPOINT_DOCS["gallery"]["summary"],
         description=ENDPOINT_DOCS["gallery"]["description"],
         response_description=ENDPOINT_DOCS["gallery"]["response_description"])
def image_gallery(skip: int = Query(0), limit: int = Query(20, ge=1, le=100), db: Session = Depends(get_db)):
    files = db.query(File).filter(func.lower(File.filename).like('%.png')).order_by(desc(File.created_at)).offset(skip).limit(limit).all()
    total = db.query(File).filter(func.lower(File.filename).like('%.png')).count()
    html = f"""<!DOCTYPE html><html><head><title>Gallery - {total} Images</title>
    <style>body{{font-family:Arial;margin:20px;background:#f0f0f0}}.gallery{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;padding:20px}}
    .image-card{{background:white;border-radius:8px;overflow:hidden;box-shadow:0 2px 4px rgba(0,0,0,0.1);cursor:pointer;transition:transform 0.2s}}
    .image-card:hover{{transform:translateY(-5px)}}.image-card img{{width:100%;height:200px;object-fit:cover}}.image-info{{padding:10px}}
    .filename{{font-weight:bold;word-break:break-all;font-size:12px}}.size{{color:#666;font-size:11px}}.date{{color:#999;font-size:10px}}
    .pagination{{text-align:center;margin:20px}}.pagination a{{margin:0 5px;padding:8px 12px;background:#007bff;color:white;text-decoration:none;border-radius:4px}}
    .modal{{display:none;position:fixed;z-index:1000;left:0;top:0;width:100%;height:100%;background:rgba(0,0,0,0.9)}}
    .modal-content{{max-width:90%;max-height:90%;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%)}}
    .close{{position:absolute;top:15px;right:35px;color:#fff;font-size:40px;cursor:pointer}}</style></head>
    <body><div style="background:white;padding:20px;border-radius:8px;margin-bottom:20px"><h1>📸 Image Gallery</h1><p>Total: {total}</p></div>
    <div class="gallery">"""
    for f in files:
        size_mb = f.size/(1024*1024)
        html += f'<div class="image-card" onclick="openModal(`/image/{f.id}`)"><img src="/image/{f.id}" loading="lazy"><div class="image-info"><div class="filename">{f.filename[:40]}</div><div class="size">{size_mb:.2f} MB</div><div class="date">{f.created_at.strftime("%Y-%m-%d")}</div></div></div>'
    total_pages = (total+limit-1)//limit
    current = skip//limit+1
    html += '</div><div class="pagination">'
    if current>1: html += f'<a href="/gallery?skip={skip-limit}&limit={limit}">← Previous</a>'
    for p in range(max(1,current-3), min(total_pages,current+3)+1):
        if p==current: html += f'<strong style="margin:0 5px;padding:8px 12px;background:#007bff;color:white;border-radius:4px">{p}</strong>'
        else: html += f'<a href="/gallery?skip={(p-1)*limit}&limit={limit}">{p}</a>'
    if current<total_pages: html += f'<a href="/gallery?skip={skip+limit}&limit={limit}">Next →</a>'
    html += f"""</div><div id="modal" class="modal" onclick="closeModal()"><span class="close">&times;</span><img class="modal-content" id="modal-img"></div>
    <script>function openModal(src){{document.getElementById('modal').style.display='block';document.getElementById('modal-img').src=src;}}
    function closeModal(){{document.getElementById('modal').style.display='none';document.getElementById('modal-img').src='';}}
    document.addEventListener('keydown',e=>{{if(e.key==='Escape')closeModal();}});</script></body></html>"""
    return HTMLResponse(content=html)

@app.get("/images", tags=["Images"],
         summary=ENDPOINT_DOCS["list_images"]["summary"],
         description=ENDPOINT_DOCS["list_images"]["description"],
         response_description=ENDPOINT_DOCS["list_images"]["response_description"])
def list_images(skip: int = Query(0), limit: int = Query(100), db: Session = Depends(get_db)):
    files = db.query(File).filter(func.lower(File.filename).like('%.png')).order_by(desc(File.created_at)).offset(skip).limit(limit).all()
    total = db.query(File).filter(func.lower(File.filename).like('%.png')).count()
    return {"total": total, "skip": skip, "limit": limit,
            "images": [{"file_id": f.id, "filename": f.filename, "size": f.size,
                       "size_mb": round(f.size/(1024*1024),2), "created_at": f.created_at.isoformat(),
                       "url": f"/image/{f.id}"} for f in files]}

@app.get("/image/{file_id}/thumbnail", tags=["Images"],
         summary=ENDPOINT_DOCS["thumbnail"]["summary"],
         description=ENDPOINT_DOCS["thumbnail"]["description"],
         response_description=ENDPOINT_DOCS["thumbnail"]["response_description"])
async def get_thumbnail(file_id: str, size: int = 200, db: Session = Depends(get_db)):
    try:
        file = db.query(File).filter(File.id == file_id).first()
        if not file: raise HTTPException(404, "File not found")
        file_chunks = db.query(FileChunk).filter_by(file_id=file_id).order_by(FileChunk.order_index).all()
        image_data = b''.join([open(db.query(Chunk).filter_by(hash=fc.chunk_hash).first().path, 'rb').read() 
                               for fc in file_chunks if db.query(Chunk).filter_by(hash=fc.chunk_hash).first()])
        img = Image.open(io.BytesIO(image_data))
        img.thumbnail((size, size), Image.Resampling.LANCZOS)
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format=img.format or 'PNG')
        return StreamingResponse(io.BytesIO(img_byte_arr.getvalue()),
            media_type=f"image/{img.format.lower() if img.format else 'png'}",
            headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        logger.error(f"Error generating thumbnail: {e}")
        raise HTTPException(500, detail=str(e))

# =========================
# Deletion Endpoints
# =========================

def delete_file(file: File, db: Session):
    file_id, filename = file.id, file.filename
    file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
    if not file_chunks:
        db.delete(file); db.commit()
        return {"message": f"File '{filename}' deleted", "file_id": file_id, "chunks_removed": 0}
    chunks_to_delete, chunks_updated = [], []
    for fc in file_chunks:
        chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
        if chunk:
            chunk.ref_count -= 1
            if chunk.ref_count == 0: chunks_to_delete.append(chunk)
            else: chunks_updated.append(chunk)
        db.delete(fc)
    db.delete(file)
    db.commit()
    deleted = []
    for chunk in chunks_to_delete:
        if os.path.exists(chunk.path): os.remove(chunk.path)
        db.delete(chunk)
        deleted.append(chunk.hash[:16])
        chunk_dir = os.path.dirname(chunk.path)
        try:
            if os.path.exists(chunk_dir) and not os.listdir(chunk_dir): os.rmdir(chunk_dir)
        except: pass
    db.commit()
    return {"message": f"File '{filename}' deleted", "file_id": file_id,
            "total_chunks": len(file_chunks), "chunks_updated": len(chunks_updated),
            "chunks_removed": len(deleted), "deleted_chunks": deleted[:10]}

@app.delete("/files/{file_id}", tags=["Files"],
            summary=ENDPOINT_DOCS["delete_file"]["summary"],
            description=ENDPOINT_DOCS["delete_file"]["description"],
            response_description=ENDPOINT_DOCS["delete_file"]["response_description"],
            responses=RESPONSES["delete"])
def delete_file_by_id(file_id: str, force: bool = Query(False), db: Session = Depends(get_db)):
    file = db.query(File).filter(File.id == file_id).first()
    if not file: raise HTTPException(404, "File not found")
    file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
    shared_count = sum(1 for fc in file_chunks 
                      if db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first() and 
                      db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first().ref_count > 1)
    if shared_count > 0 and not force:
        return {"warning": f"Shares {shared_count} chunks with other files",
                "preview": f"/files/{file_id}/delete-preview",
                "force_delete": f"DELETE /files/{file_id}?force=true"}
    return delete_file(file, db)

@app.delete("/files/batch", tags=["Files"],
            summary=ENDPOINT_DOCS["delete_batch"]["summary"],
            description=ENDPOINT_DOCS["delete_batch"]["description"],
            response_description=ENDPOINT_DOCS["delete_batch"]["response_description"])
def delete_files_batch(file_ids: list[str] = Query(...), db: Session = Depends(get_db)):
    results, errors = [], []
    for file_id in file_ids:
        file = db.query(File).filter(File.id == file_id).first()
        if file:
            results.append({"file_id": file_id, "filename": file.filename, "status": "deleted"})
            delete_file(file, db)
        else: errors.append({"file_id": file_id, "status": "not_found"})
    return {"message": f"Deleted {len(results)} files, {len(errors)} errors", "deleted": results, "errors": errors}

@app.post("/files/{file_id}/delete-preview", tags=["Files"],
          summary=ENDPOINT_DOCS["delete_preview"]["summary"],
          description=ENDPOINT_DOCS["delete_preview"]["description"],
          response_description=ENDPOINT_DOCS["delete_preview"]["response_description"])
def preview_deletion(file_id: str, db: Session = Depends(get_db)):
    file = db.query(File).filter(File.id == file_id).first()
    if not file: raise HTTPException(404, "File not found")
    file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
    if not file_chunks: return {"file_id": file_id, "filename": file.filename, "message": "No chunks found"}
    chunks_to_delete, shared_files = [], set()
    for fc in file_chunks:
        chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
        if chunk:
            if chunk.ref_count - 1 == 0: chunks_to_delete.append(chunk)
            else: shared_files.update([f.id for f in db.query(File).join(FileChunk).filter(FileChunk.chunk_hash == chunk.hash, File.id != file_id).all()])
    space_to_free = sum(c.size for c in chunks_to_delete)
    return {"file": {"file_id": file_id, "filename": file.filename, "size_mb": round(file.size/(1024*1024),2)},
            "impact": {"chunks_to_delete": len(chunks_to_delete), "chunks_to_keep": len(file_chunks)-len(chunks_to_delete),
                      "space_to_free_mb": round(space_to_free/(1024*1024),2), "files_affected": len(shared_files)},
            "warning": "Preview only - no data deleted"}

@app.get("/files/{file_id}/shared-chunks", tags=["Files"],
         summary=ENDPOINT_DOCS["shared_chunks"]["summary"],
         description=ENDPOINT_DOCS["shared_chunks"]["description"],
         response_description=ENDPOINT_DOCS["shared_chunks"]["response_description"])
def get_shared_chunks(file_id: str, db: Session = Depends(get_db)):
    file = db.query(File).filter(File.id == file_id).first()
    if not file: raise HTTPException(404, "File not found")
    file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
    shared = []
    for fc in file_chunks:
        chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
        if chunk and chunk.ref_count > 1:
            other = db.query(File).join(FileChunk).filter(FileChunk.chunk_hash == chunk.hash, File.id != file_id).all()
            if other: shared.append({"hash": chunk.hash[:16]+"...", "size_kb": round(chunk.size/1024,1),
                                     "ref_count": chunk.ref_count, "other_files": [f.filename for f in other]})
    return {"file": file.filename, "total_shared_chunks": len(shared), "shared_chunks": shared[:50]}

# =========================
# Chunk Management
# =========================

@app.get("/chunks/unused", tags=["Chunks"],
         summary=ENDPOINT_DOCS["unused_chunks"]["summary"],
         description=ENDPOINT_DOCS["unused_chunks"]["description"],
         response_description=ENDPOINT_DOCS["unused_chunks"]["response_description"])
def list_unused_chunks(db: Session = Depends(get_db)):
    unused = db.query(Chunk).filter(Chunk.ref_count == 0).all()
    return {"total_unused_chunks": len(unused), "total_size_mb": round(sum(c.size for c in unused)/(1024*1024),2),
            "unused_chunks": [{"hash": c.hash[:16]+"...", "size_bytes": c.size, "path": c.path} for c in unused[:50]]}

@app.delete("/chunks/unused", tags=["Chunks"],
            summary=ENDPOINT_DOCS["cleanup_chunks"]["summary"],
            description=ENDPOINT_DOCS["cleanup_chunks"]["description"],
            response_description=ENDPOINT_DOCS["cleanup_chunks"]["response_description"])
def cleanup_unused_chunks(db: Session = Depends(get_db)):
    unused = db.query(Chunk).filter(Chunk.ref_count == 0).all()
    if not unused: return {"message": "No unused chunks"}
    total_size = 0
    for chunk in unused:
        if os.path.exists(chunk.path): os.remove(chunk.path); total_size += chunk.size
        db.delete(chunk)
    db.commit()
    return {"message": f"Cleaned up {len(unused)} chunks", "space_freed_mb": round(total_size/(1024*1024),2)}