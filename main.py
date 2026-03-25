# main.py
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Query, Depends, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="File Storage with Deduplication API", 
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration from environment
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "temp")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1024 * 1024))
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Redis connection for RQ
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_conn = Redis.from_url(REDIS_URL)

upload_queue = Queue('uploads', connection=redis_conn)      # File uploads
dedup_queue = Queue('dedup', connection=redis_conn)         # Deduplication processing
cleanup_queue = Queue('cleanup', connection=redis_conn)     # Cleanup tasks

q = Queue(connection=redis_conn)

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database and create tables"""
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources"""
    redis_conn.close()
    logger.info("Shutdown complete")

# =========================
# File Upload Endpoints
# =========================

small_queue = Queue('uploads', connection=redis_conn)  # For < 50MB
large_queue = Queue('large-files', connection=redis_conn)  # For > 50MB
cleanup_queue = Queue('cleanup', connection=redis_conn)

@app.post("/upload")
async def upload(file: UploadFile = FastAPIFile(...)):
    try:
        file_id = str(uuid.uuid4())
        temp_path = os.path.join(UPLOAD_DIR, file_id)
        
        # Save file
        with open(temp_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        
        # Get file size
        file_size = os.path.getsize(temp_path)
        
        # Route to appropriate queue
        if file_size > 50 * 1024 * 1024:  # > 50MB
            queue = large_queue
            job_timeout = 7200  # 2 hours for large files
            logger.info(f"Large file {file_size/1024/1024:.1f}MB routed to large-files queue")
        else:
            queue = small_queue
            job_timeout = 3600  # 1 hour for small files
        
        # Enqueue job with appropriate timeout
        job = queue.enqueue(
            process_file,
            temp_path,
            file.filename,
            file_id,
            job_timeout=job_timeout,
            result_ttl=5000
        )
        
        return {
            "file_id": file_id,
            "job_id": job.id,
            "queue": queue.name,
            "filename": file.filename,
            "size": file_size,
            "status": "queued"
        }
        
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
# =========================
# File Listing Endpoints (Optimized for PostgreSQL)
# =========================

@app.get("/files")
def list_files(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort_by: str = Query("created_at", regex="^(created_at|filename|size)$"),
    order: str = Query("desc", regex="^(asc|desc)$"),
    db: Session = Depends(get_db)
):
    """List all uploaded files with pagination"""
    
    try:
        # Optimized query for PostgreSQL
        query = db.query(
            File,
            func.count(FileChunk.id).label('chunk_count')
        ).outerjoin(
            FileChunk, File.id == FileChunk.file_id
        ).group_by(File.id)
        
        # Apply sorting with case-insensitive filename sorting
        if sort_by == "created_at":
            sort_column = File.created_at
        elif sort_by == "size":
            sort_column = File.size
        else:
            # Case-insensitive filename sorting for PostgreSQL
            sort_column = func.lower(File.filename)
        
        if order == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(sort_column)
        
        # Get total count efficiently
        total = db.query(File).count()
        
        # Apply pagination
        results = query.offset(skip).limit(limit).all()
        
        # Format response
        files = []
        for file, chunk_count in results:
            files.append({
                "file_id": file.id,
                "filename": file.filename,
                "size": file.size,
                "created_at": file.created_at.isoformat(),
                "chunk_count": chunk_count
            })
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "files": files
        }
    
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/search")
def search_files(
    q: str = Query("", description="Search query for filename"),
    min_size: int = Query(None, ge=0),
    max_size: int = Query(None, ge=0),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Search files by filename (case-insensitive) and size range"""
    
    try:
        query = db.query(
            File,
            func.count(FileChunk.id).label('chunk_count')
        ).outerjoin(
            FileChunk, File.id == FileChunk.file_id
        ).group_by(File.id)
        
        # Case-insensitive search using ILIKE (PostgreSQL)
        if q:
            query = query.filter(File.filename.ilike(f"%{q}%"))
        
        if min_size is not None:
            query = query.filter(File.size >= min_size)
        
        if max_size is not None:
            query = query.filter(File.size <= max_size)
        
        # Order by created_at desc by default
        query = query.order_by(desc(File.created_at))
        
        total = query.count()
        results = query.offset(skip).limit(limit).all()
        
        files = []
        for file, chunk_count in results:
            files.append({
                "file_id": file.id,
                "filename": file.filename,
                "size": file.size,
                "created_at": file.created_at.isoformat(),
                "chunk_count": chunk_count
            })
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "files": files
        }
    
    except Exception as e:
        logger.error(f"Error searching files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# File Download Endpoint (Optimized)
# =========================

@app.get("/download/{file_id}")
def download_file(file_id: str, db: Session = Depends(get_db)):
    """Download a file by reconstructing from chunks"""
    
    try:
        # Check if file exists with a single query
        file = db.query(File).filter(File.id == file_id).first()
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Get all chunks in order with a single optimized query
        file_chunks = db.query(
            FileChunk.order_index,
            Chunk.path,
            Chunk.hash
        ).join(
            Chunk, FileChunk.chunk_hash == Chunk.hash
        ).filter(
            FileChunk.file_id == file_id
        ).order_by(
            FileChunk.order_index
        ).all()
        
        if not file_chunks:
            raise HTTPException(status_code=404, detail="No chunks found for this file")
        
        def file_stream():
            """Stream file chunks in order with buffering"""
            for order_index, chunk_path, chunk_hash in file_chunks:
                try:
                    if os.path.exists(chunk_path):
                        with open(chunk_path, "rb") as f:
                            yield f.read()
                    else:
                        logger.error(f"Missing chunk: {chunk_hash} for file {file_id}")
                        # Yield empty bytes or raise error
                        yield b""
                except Exception as e:
                    logger.error(f"Error reading chunk {chunk_hash}: {e}")
                    yield b""
        
        # Return streaming response with filename
        return StreamingResponse(
            file_stream(), 
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={file.filename}",
                "Content-Length": str(file.size),  # Optional: helps with progress bars
                "X-File-ID": file_id
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# Statistics Endpoint (Optimized)
# =========================

@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get system statistics with PostgreSQL-optimized queries"""
    
    try:
        # File statistics
        total_files = db.query(File).count()
        total_size = db.query(func.sum(File.size)).scalar() or 0
        
        # Chunk statistics with single query
        chunk_stats = db.query(
            func.count(Chunk.hash).label('total_chunks'),
            func.sum(Chunk.size).label('total_size'),
            func.sum(Chunk.ref_count).label('total_refs'),
            func.avg(Chunk.ref_count).label('avg_refs'),
            func.max(Chunk.ref_count).label('max_refs'),
            func.count(func.nullif(Chunk.ref_count, 1)).label('shared_chunks')
        ).first()
        
        # Get top shared chunks
        top_chunks = db.query(
            Chunk.hash,
            Chunk.ref_count,
            Chunk.size
        ).order_by(
            desc(Chunk.ref_count)
        ).limit(10).all()
        
        physical_size = chunk_stats.total_size or 0
        logical_size = total_size
        space_saved = logical_size - physical_size
        
        return {
            "files": {
                "total": total_files,
                "total_size_bytes": logical_size,
                "total_size_mb": round(logical_size / (1024 * 1024), 2),
                "total_size_gb": round(logical_size / (1024 * 1024 * 1024), 2)
            },
            "chunks": {
                "total_unique_chunks": chunk_stats.total_chunks or 0,
                "total_references": chunk_stats.total_refs or 0,
                "shared_chunks": chunk_stats.shared_chunks or 0,
                "max_ref_count": chunk_stats.max_refs or 0,
                "avg_ref_count": round(chunk_stats.avg_refs or 0, 2),
                "top_chunks": [
                    {
                        "hash": chunk.hash[:16] + "...",
                        "ref_count": chunk.ref_count,
                        "size_bytes": chunk.size
                    }
                    for chunk in top_chunks
                ]
            },
            "storage": {
                "logical_size_bytes": logical_size,
                "physical_size_bytes": physical_size,
                "space_saved_bytes": space_saved,
                "space_saved_mb": round(space_saved / (1024 * 1024), 2),
                "space_saved_gb": round(space_saved / (1024 * 1024 * 1024), 2),
                "deduplication_ratio": round(physical_size / logical_size if logical_size > 0 else 1, 3),
                "efficiency_percent": round((1 - (physical_size / logical_size)) * 100 if logical_size > 0 else 0, 2)
            }
        }
    
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# Health Check
# =========================

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    """Comprehensive health check endpoint"""
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {}
    }
    
    # Check Redis
    try:
        redis_conn.ping()
        health_status["services"]["redis"] = "healthy"
    except Exception as e:
        health_status["services"]["redis"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Database
    try:
        db.execute(text("SELECT 1"))
        health_status["services"]["database"] = "healthy"
    except Exception as e:
        health_status["services"]["database"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Disk Space (optional)
    try:
        import shutil
        disk_usage = shutil.disk_usage("/")
        free_gb = disk_usage.free / (1024**3)
        health_status["services"]["disk"] = {
            "status": "healthy" if free_gb > 1 else "warning",
            "free_gb": round(free_gb, 2)
        }
        if free_gb < 1:
            health_status["status"] = "degraded"
    except:
        pass
    
    return health_status

# main.py - Add these endpoints

from fastapi.responses import StreamingResponse, HTMLResponse
import mimetypes

@app.get("/image/{file_id}")
async def display_image(file_id: str, db: Session = Depends(get_db)):
    """Display image directly in browser using file ID"""
    try:
        # Get file from database
        file = db.query(File).filter(File.id == file_id).first()
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Check if it's an image (support PNG, JPG, etc.)
        if not file.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
            raise HTTPException(status_code=400, detail="File is not an image")
        
        # Get all chunks in order
        file_chunks = db.query(FileChunk).filter_by(
            file_id=file_id
        ).order_by(
            FileChunk.order_index
        ).all()
        
        if not file_chunks:
            raise HTTPException(status_code=404, detail="No chunks found")
        
        def image_stream():
            """Stream image chunks"""
            for fc in file_chunks:
                chunk = db.query(Chunk).filter_by(hash=fc.chunk_hash).first()
                if chunk and os.path.exists(chunk.path):
                    with open(chunk.path, "rb") as f:
                        yield f.read()
        
        # Get MIME type
        content_type = "image/png" if file.filename.lower().endswith('.png') else "image/jpeg"
        
        return StreamingResponse(
            image_stream(),
            media_type=content_type,
            headers={
                "Content-Disposition": f"inline; filename={file.filename}",
                "Cache-Control": "public, max-age=3600"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error displaying image: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/image/filename/{filename}")
async def display_image_by_filename(filename: str, db: Session = Depends(get_db)):
    """
    Display image using filename (returns first match)
    Usage: /image/filename/heatmap_baseline_vs_11469.png
    """
    try:
        # Find file by filename (case-insensitive)
        file = db.query(File).filter(
            func.lower(File.filename) == func.lower(filename)
        ).first()
        
        if not file:
            # Try partial match
            files = db.query(File).filter(
                File.filename.ilike(f"%{filename}%")
            ).limit(5).all()
            
            if files:
                # Return HTML with multiple matches
                html = f"""
                <html>
                <head><title>Multiple Matches</title></head>
                <body>
                    <h2>Multiple files match "{filename}"</h2>
                    <ul>
                """
                for f in files:
                    html += f'<li><a href="/image/{f.id}">{f.filename}</a> ({f.size:,} bytes)</li>'
                html += """
                    </ul>
                </body>
                </html>
                """
                return HTMLResponse(content=html)
            
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found")
        
        # Redirect to the ID-based endpoint
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=f"/image/{file.id}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error displaying image: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/gallery")
def image_gallery(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """HTML gallery of all images"""
    # Get all PNG files
    files = db.query(File).filter(
        func.lower(File.filename).like('%.png')
    ).order_by(desc(File.created_at)).offset(skip).limit(limit).all()
    
    total = db.query(File).filter(
        func.lower(File.filename).like('%.png')
    ).count()
    
    # Generate HTML gallery
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Image Gallery - {total} Images</title>
        <style>
            body {{ 
                font-family: Arial, sans-serif; 
                margin: 20px; 
                background: #f0f0f0;
            }}
            .header {{
                background: white;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .gallery {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
                gap: 20px;
                padding: 20px;
            }}
            .image-card {{
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                transition: transform 0.2s;
                cursor: pointer;
            }}
            .image-card:hover {{
                transform: translateY(-5px);
                box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            }}
            .image-card img {{
                width: 100%;
                height: 200px;
                object-fit: cover;
            }}
            .image-info {{
                padding: 10px;
            }}
            .filename {{
                font-weight: bold;
                word-break: break-all;
                font-size: 12px;
            }}
            .size {{
                color: #666;
                font-size: 11px;
                margin-top: 5px;
            }}
            .date {{
                color: #999;
                font-size: 10px;
                margin-top: 5px;
            }}
            .pagination {{
                text-align: center;
                margin: 20px;
            }}
            .pagination a {{
                margin: 0 5px;
                padding: 8px 12px;
                background: #007bff;
                color: white;
                text-decoration: none;
                border-radius: 4px;
            }}
            .pagination a:hover {{
                background: #0056b3;
            }}
            .modal {{
                display: none;
                position: fixed;
                z-index: 1000;
                left: 0;
                top: 0;
                width: 100%;
                height: 100%;
                background-color: rgba(0,0,0,0.9);
            }}
            .modal-content {{
                margin: auto;
                display: block;
                max-width: 90%;
                max-height: 90%;
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
            }}
            .close {{
                position: absolute;
                top: 15px;
                right: 35px;
                color: #f1f1f1;
                font-size: 40px;
                font-weight: bold;
                cursor: pointer;
            }}
            .info {{
                text-align: center;
                color: white;
                position: absolute;
                bottom: 20px;
                left: 0;
                right: 0;
                font-size: 14px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📸 Image Gallery</h1>
            <p>Total images: {total} | Showing {len(files)} images</p>
        </div>
        
        <div class="gallery">
    """
    
    for file in files:
        size_mb = file.size / (1024 * 1024)
        date_str = file.created_at.strftime('%Y-%m-%d %H:%M:%S')
        # Truncate filename if too long
        display_name = file.filename[:40] + "..." if len(file.filename) > 40 else file.filename
        
        html += f"""
            <div class="image-card" onclick="openModal('/image/{file.id}')">
                <img src="/image/{file.id}" alt="{display_name}" loading="lazy">
                <div class="image-info">
                    <div class="filename" title="{file.filename}">{display_name}</div>
                    <div class="size">{size_mb:.2f} MB</div>
                    <div class="date">{date_str}</div>
                </div>
            </div>
        """
    
    # Pagination
    total_pages = (total + limit - 1) // limit
    current_page = skip // limit + 1
    
    html += f"""
        </div>
        
        <div class="pagination">
    """
    
    if current_page > 1:
        html += f'<a href="/gallery?skip={skip-limit}&limit={limit}">← Previous</a>'
    
    # Show page numbers
    start_page = max(1, current_page - 3)
    end_page = min(total_pages, current_page + 3)
    
    for page in range(start_page, end_page + 1):
        new_skip = (page - 1) * limit
        if page == current_page:
            html += f'<strong style="margin:0 5px;padding:8px 12px;background:#007bff;color:white;border-radius:4px;">{page}</strong>'
        else:
            html += f'<a href="/gallery?skip={new_skip}&limit={limit}">{page}</a>'
    
    if current_page < total_pages:
        html += f'<a href="/gallery?skip={skip+limit}&limit={limit}">Next →</a>'
    
    html += """
        </div>
        
        <div id="modal" class="modal" onclick="closeModal()">
            <span class="close">&times;</span>
            <img class="modal-content" id="modal-img">
            <div class="info" id="modal-info"></div>
        </div>
        
        <script>
            function openModal(src) {
                document.getElementById('modal').style.display = 'block';
                document.getElementById('modal-img').src = src;
                // Extract filename from URL
                const filename = src.split('/').pop();
                document.getElementById('modal-info').innerHTML = filename;
            }
            
            function closeModal() {
                document.getElementById('modal').style.display = 'none';
                document.getElementById('modal-img').src = '';
                document.getElementById('modal-info').innerHTML = '';
            }
            
            document.addEventListener('keydown', function(e) {
                if (e.key === 'Escape') {
                    closeModal();
                }
            });
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html)

# Add to main.py - Thumbnail support
from PIL import Image
import io

@app.get("/image/{file_id}/thumbnail")
async def get_thumbnail(file_id: str, size: int = 200, db: Session = Depends(get_db)):
    """
    Generate and serve thumbnail of image
    Usage: /image/5de5f5c9-9efa-4083-9548-92b7d7564530/thumbnail?size=200
    """
    try:
        file = db.query(File).filter(File.id == file_id).first()
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Get all chunks
        file_chunks = db.query(FileChunk).filter_by(
            file_id=file_id
        ).order_by(
            FileChunk.order_index
        ).all()
        
        # Reconstruct image in memory
        image_data = b''
        for fc in file_chunks:
            chunk = db.query(Chunk).filter_by(hash=fc.chunk_hash).first()
            if chunk and os.path.exists(chunk.path):
                with open(chunk.path, "rb") as f:
                    image_data += f.read()
        
        # Generate thumbnail
        img = Image.open(io.BytesIO(image_data))
        img.thumbnail((size, size), Image.Resampling.LANCZOS)
        
        # Save to bytes
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format=img.format or 'PNG')
        img_byte_arr = img_byte_arr.getvalue()
        
        return StreamingResponse(
            io.BytesIO(img_byte_arr),
            media_type=f"image/{img.format.lower() if img.format else 'png'}",
            headers={"Cache-Control": "public, max-age=86400"}  # Cache for 24 hours
        )
        
    except Exception as e:
        logger.error(f"Error generating thumbnail: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/images")
def list_images(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """List all image files with their details"""
    # Get all files with .png extension (case-insensitive)
    files = db.query(File).filter(
        func.lower(File.filename).like('%.png')
    ).order_by(desc(File.created_at)).offset(skip).limit(limit).all()
    
    total = db.query(File).filter(
        func.lower(File.filename).like('%.png')
    ).count()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "images": [
            {
                "file_id": f.id,
                "filename": f.filename,
                "size": f.size,
                "size_mb": round(f.size / (1024 * 1024), 2),
                "created_at": f.created_at.isoformat(),
                "url": f"/image/{f.id}"
            }
            for f in files
        ]
    }


@app.delete("/files/{file_id}")
def delete_file_by_id(
    file_id: str, 
    force: bool = Query(False, description="Force delete even if file has shared chunks"),
    db: Session = Depends(get_db)
):
    """
    Delete a file by its ID.
    If file has shared chunks, requires force=True to proceed.
    """
    try:
        # Get the file
        file = db.query(File).filter(File.id == file_id).first()
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Check if file has shared chunks
        file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
        has_shared = False
        shared_count = 0
        
        for fc in file_chunks:
            chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
            if chunk and chunk.ref_count > 1:
                has_shared = True
                shared_count += 1
        
        # If has shared chunks and not force, return warning
        if has_shared and not force:
            return {
                "warning": f"This file shares {shared_count} chunks with other files.",
                "message": "Deleting this file will only remove the references, not the actual chunk data.",
                "preview": f"/files/{file_id}/delete-preview",
                "force_delete": f"DELETE /files/{file_id}?force=true"
            }
        
        # Proceed with deletion
        return delete_file(file, db)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/files/name/{filename:path}")
def delete_file_by_filename(filename: str, db: Session = Depends(get_db)):
    """
    Delete a file by its filename.
    If multiple files with same name exist, returns list of matches.
    Only removes chunks that are no longer referenced by any other file.
    """
    try:
        from urllib.parse import unquote
        filename = unquote(filename)
        
        # Find files with this filename
        files = db.query(File).filter(File.filename == filename).all()
        
        if not files:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found")
        
        # If multiple files with same name, return list for user to choose
        if len(files) > 1:
            return {
                "message": f"Multiple files found with name '{filename}'",
                "files": [
                    {
                        "file_id": f.id,
                        "filename": f.filename,
                        "size": f.size,
                        "created_at": f.created_at.isoformat()
                    }
                    for f in files
                ],
                "action": "Please use /files/{file_id} to delete specific file"
            }
        
        # Single file found
        return delete_file(files[0], db)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def delete_file(file: File, db: Session):
    """
    Helper function to delete a file and clean up unused chunks
    """
    file_id = file.id
    filename = file.filename
    
    logger.info(f"Deleting file: {filename} (ID: {file_id})")
    
    # Get all chunks for this file
    file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
    
    if not file_chunks:
        # No chunks found, just delete the file record
        db.delete(file)
        db.commit()
        return {
            "message": f"File '{filename}' deleted successfully",
            "file_id": file_id,
            "chunks_removed": 0,
            "total_chunks": 0
        }
    
    # Track chunks that become unused
    chunks_to_delete = []
    chunks_updated = []
    
    # Process each chunk
    for fc in file_chunks:
        chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
        
        if chunk:
            # Decrement reference count
            old_ref_count = chunk.ref_count
            chunk.ref_count -= 1
            
            logger.debug(f"Chunk {chunk.hash[:16]}... ref_count: {old_ref_count} -> {chunk.ref_count}")
            
            # If ref_count becomes 0, mark for deletion
            if chunk.ref_count == 0:
                chunks_to_delete.append(chunk)
                logger.debug(f"Chunk {chunk.hash[:16]}... marked for deletion (no more references)")
            else:
                chunks_updated.append(chunk)
    
    # Remove the file-chunk mappings
    for fc in file_chunks:
        db.delete(fc)
    
    # Delete the file record
    db.delete(file)
    
    # Commit to save ref_count changes and file deletion
    db.commit()
    
    # Now delete physical chunk files and database records
    deleted_chunks = []
    for chunk in chunks_to_delete:
        try:
            # Delete physical file
            if os.path.exists(chunk.path):
                os.remove(chunk.path)
                logger.debug(f"Deleted chunk file: {chunk.path}")
            
            # Delete database record
            db.delete(chunk)
            deleted_chunks.append(chunk.hash[:16])
            
            # Try to clean up empty directories
            chunk_dir = os.path.dirname(chunk.path)
            try:
                if os.path.exists(chunk_dir) and not os.listdir(chunk_dir):
                    os.rmdir(chunk_dir)
                    logger.debug(f"Removed empty directory: {chunk_dir}")
            except Exception as e:
                logger.debug(f"Could not remove directory {chunk_dir}: {e}")
                
        except Exception as e:
            logger.error(f"Error deleting chunk {chunk.hash}: {e}")
    
    # Final commit for chunk deletions
    db.commit()
    
    logger.info(
        f"File deleted: {filename}\n"
        f"  - Total chunks: {len(file_chunks)}\n"
        f"  - Updated chunks (still referenced): {len(chunks_updated)}\n"
        f"  - Deleted chunks (no longer referenced): {len(deleted_chunks)}"
    )
    
    return {
        "message": f"File '{filename}' deleted successfully",
        "file_id": file_id,
        "total_chunks": len(file_chunks),
        "chunks_updated": len(chunks_updated),
        "chunks_removed": len(deleted_chunks),
        "deleted_chunks": deleted_chunks[:10]  # Show first 10 for reference
    }

@app.delete("/files/batch")
def delete_files_batch(
    file_ids: list[str] = Query(..., description="List of file IDs to delete"),
    db: Session = Depends(get_db)
):
    """
    Delete multiple files at once.
    Example: /files/batch?file_ids=id1&file_ids=id2&file_ids=id3
    """
    results = []
    errors = []
    
    for file_id in file_ids:
        try:
            file = db.query(File).filter(File.id == file_id).first()
            if file:
                result = delete_file(file, db)
                results.append({
                    "file_id": file_id,
                    "filename": file.filename,
                    "status": "deleted",
                    "chunks_removed": result["chunks_removed"]
                })
            else:
                errors.append({
                    "file_id": file_id,
                    "status": "not_found"
                })
        except Exception as e:
            errors.append({
                "file_id": file_id,
                "status": "error",
                "error": str(e)
            })
    
    return {
        "message": f"Deleted {len(results)} files, {len(errors)} errors",
        "deleted": results,
        "errors": errors
    }




@app.get("/chunks/unused")
def list_unused_chunks(db: Session = Depends(get_db)):
    """
    List all chunks that have ref_count = 0 (not used by any file)
    """
    try:
        unused_chunks = db.query(Chunk).filter(Chunk.ref_count == 0).all()
        
        return {
            "total_unused_chunks": len(unused_chunks),
            "total_size_bytes": sum(c.size for c in unused_chunks),
            "total_size_mb": round(sum(c.size for c in unused_chunks) / (1024 * 1024), 2),
            "unused_chunks": [
                {
                    "hash": c.hash[:16] + "...",
                    "full_hash": c.hash,
                    "size_bytes": c.size,
                    "path": c.path,
                    "created_at": c.created_at.isoformat()
                }
                for c in unused_chunks[:50]  # Show first 50
            ]
        }
        
    except Exception as e:
        logger.error(f"Error listing unused chunks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/chunks/unused")
def cleanup_unused_chunks(db: Session = Depends(get_db)):
    """
    Delete all chunks that have ref_count = 0
    """
    try:
        unused_chunks = db.query(Chunk).filter(Chunk.ref_count == 0).all()
        
        if not unused_chunks:
            return {
                "message": "No unused chunks found",
                "chunks_deleted": 0,
                "space_freed_bytes": 0
            }
        
        total_size = 0
        deleted_hashes = []
        
        for chunk in unused_chunks:
            try:
                # Delete physical file
                if os.path.exists(chunk.path):
                    os.remove(chunk.path)
                    total_size += chunk.size
                    deleted_hashes.append(chunk.hash[:16])
                
                # Delete database record
                db.delete(chunk)
                
                # Try to remove empty directories
                chunk_dir = os.path.dirname(chunk.path)
                try:
                    if os.path.exists(chunk_dir) and not os.listdir(chunk_dir):
                        os.rmdir(chunk_dir)
                except:
                    pass
                    
            except Exception as e:
                logger.error(f"Error deleting chunk {chunk.hash}: {e}")
        
        db.commit()
        
        return {
            "message": f"Cleaned up {len(deleted_hashes)} unused chunks",
            "chunks_deleted": len(deleted_hashes),
            "space_freed_bytes": total_size,
            "space_freed_mb": round(total_size / (1024 * 1024), 2),
            "deleted_chunks_preview": deleted_hashes[:20]
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error cleaning up chunks: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

from collections import defaultdict

@app.post("/files/{file_id}/delete-preview")
def preview_deletion(file_id: str, db: Session = Depends(get_db)):
    """
    Preview what would be deleted and show which other files share chunks.
    """
    try:
        file = db.query(File).filter(File.id == file_id).first()
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Get all chunks for this file
        file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
        
        if not file_chunks:
            return {
                "file_id": file_id,
                "filename": file.filename,
                "message": "No chunks found for this file",
                "total_chunks": 0
            }
        
        # Analyze each chunk
        chunks_info = []
        chunks_to_delete = []
        chunks_to_keep = []
        shared_chunks = []  # Chunks that are shared with other files
        
        # Track which files share chunks with this file
        shared_files_map = defaultdict(set)  # chunk_hash -> set of file_ids
        all_shared_files = set()  # Set of all files that share any chunk
        
        for fc in file_chunks:
            chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
            if not chunk:
                continue
            
            # Find other files that use this chunk
            other_files = db.query(File).join(
                FileChunk, File.id == FileChunk.file_id
            ).filter(
                FileChunk.chunk_hash == chunk.hash,
                File.id != file_id  # Exclude current file
            ).all()
            
            new_ref_count = chunk.ref_count - 1
            is_shared = len(other_files) > 0
            will_be_deleted = new_ref_count == 0
            
            chunk_info = {
                "hash": chunk.hash[:16] + "...",
                "full_hash": chunk.hash,
                "size_bytes": chunk.size,
                "size_kb": round(chunk.size / 1024, 2),
                "current_ref_count": chunk.ref_count,
                "new_ref_count": new_ref_count,
                "will_be_deleted": will_be_deleted,
                "is_shared": is_shared,
                "shared_with_files": [
                    {
                        "file_id": f.id,
                        "filename": f.filename,
                        "size": f.size,
                        "created_at": f.created_at.isoformat()
                    }
                    for f in other_files
                ]
            }
            
            chunks_info.append(chunk_info)
            
            if will_be_deleted:
                chunks_to_delete.append(chunk_info)
            else:
                chunks_to_keep.append(chunk_info)
            
            if is_shared:
                shared_chunks.append(chunk_info)
                for f in other_files:
                    shared_files_map[chunk.hash[:16]].add(f.filename)
                    all_shared_files.add(f.id)
        
        # Get details of all files that share chunks with this file
        shared_files_details = []
        for shared_file_id in all_shared_files:
            shared_file = db.query(File).filter(File.id == shared_file_id).first()
            if shared_file:
                # Count how many chunks are shared with the file being deleted
                shared_chunks_count = db.query(FileChunk).filter(
                    FileChunk.file_id == shared_file_id,
                    FileChunk.chunk_hash.in_([fc.chunk_hash for fc in file_chunks])
                ).count()
                
                shared_files_details.append({
                    "file_id": shared_file.id,
                    "filename": shared_file.filename,
                    "size_bytes": shared_file.size,
                    "size_mb": round(shared_file.size / (1024 * 1024), 2),
                    "created_at": shared_file.created_at.isoformat(),
                    "shared_chunks_count": shared_chunks_count,
                    "shared_chunks_preview": [
                        chunk["hash"] for chunk in chunks_info 
                        if chunk["is_shared"] and any(
                            f["file_id"] == shared_file_id 
                            for f in chunk["shared_with_files"]
                        )
                    ][:5]  # Show first 5 shared chunks
                })
        
        # Calculate total space impact
        total_size = file.size
        space_to_free = sum(c["size_bytes"] for c in chunks_to_delete)
        space_to_keep = total_size - space_to_free
        
        # Group by sharing pattern
        unique_shares = len(shared_chunks)
        total_shares = sum(len(c["shared_with_files"]) for c in shared_chunks)
        
        return {
            "file": {
                "file_id": file_id,
                "filename": file.filename,
                "size_bytes": file.size,
                "size_mb": round(file.size / (1024 * 1024), 2),
                "size_gb": round(file.size / (1024 * 1024 * 1024), 2),
                "created_at": file.created_at.isoformat(),
                "total_chunks": len(file_chunks)
            },
            "impact_analysis": {
                "total_chunks": len(file_chunks),
                "chunks_that_will_be_deleted": len(chunks_to_delete),
                "chunks_that_will_be_kept": len(chunks_to_keep),
                "space_that_will_be_freed_bytes": space_to_free,
                "space_that_will_be_freed_mb": round(space_to_free / (1024 * 1024), 2),
                "space_that_will_remain_allocated_bytes": space_to_keep,
                "space_that_will_remain_allocated_mb": round(space_to_keep / (1024 * 1024), 2),
                "deduplication_impact": {
                    "shared_chunks_count": len(shared_chunks),
                    "total_shares": total_shares,
                    "unique_other_files_affected": len(all_shared_files),
                    "sharing_percentage": round((len(shared_chunks) / len(file_chunks)) * 100, 2) if file_chunks else 0
                }
            },
            "chunks_breakdown": {
                "will_be_deleted": [
                    {
                        "hash": c["hash"],
                        "size_kb": c["size_kb"],
                        "current_ref_count": c["current_ref_count"],
                        "would_be_ref_count": 0
                    }
                    for c in chunks_to_delete[:20]  # Show first 20
                ],
                "will_be_kept": [
                    {
                        "hash": c["hash"],
                        "size_kb": c["size_kb"],
                        "current_ref_count": c["current_ref_count"],
                        "would_be_ref_count": c["new_ref_count"],
                        "shared_with": len(c["shared_with_files"]),
                        "shared_with_preview": [
                            f["filename"] for f in c["shared_with_files"][:3]
                        ]
                    }
                    for c in chunks_to_keep[:20]  # Show first 20
                ]
            },
            "shared_files": [
                {
                    "file_id": f["file_id"],
                    "filename": f["filename"],
                    "size_mb": f["size_mb"],
                    "shared_chunks_count": f["shared_chunks_count"],
                    "shared_chunks_preview": f["shared_chunks_preview"],
                    "created_at": f["created_at"]
                }
                for f in shared_files_details
            ],
            "warning": "This is a preview only. No data has been deleted.",
            "recommendation": (
                f"⚠️ This file shares {len(shared_chunks)} chunks with {len(all_shared_files)} other file(s). "
                f"Deleting it will free {round(space_to_free / (1024 * 1024), 2)} MB, "
                f"but will NOT affect the other files that use these shared chunks."
            )
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error previewing deletion: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    


@app.get("/files/{file_id}/shared-chunks")
def get_shared_chunks(file_id: str, db: Session = Depends(get_db)):
    """
    Show all chunks that this file shares with other files.
    """
    try:
        # First, check if file exists
        file = db.query(File).filter(File.id == file_id).first()
        if not file:
            # Try to see if it's a filename instead
            file_by_name = db.query(File).filter(File.filename == file_id).first()
            if file_by_name:
                # Redirect or return with correct ID
                return {
                    "message": f"File with filename '{file_id}' found. Use ID: {file_by_name.id}",
                    "file_id": file_by_name.id,
                    "filename": file_by_name.filename
                }
            
            # List available files to help debug
            available_files = db.query(File).limit(5).all()
            available_ids = [f.id for f in available_files]
            available_names = [f.filename for f in available_files]
            
            raise HTTPException(
                status_code=404, 
                detail={
                    "error": f"File with ID '{file_id}' not found",
                    "available_files": available_ids[:5],
                    "available_filenames": available_names[:5],
                    "hint": "Use /debug/all-files to see all files"
                }
            )
        
        # Get all chunks for this file
        file_chunks = db.query(FileChunk).filter_by(file_id=file_id).all()
        
        if not file_chunks:
            return {
                "file_id": file_id,
                "filename": file.filename,
                "total_shared_chunks": 0,
                "message": "This file has no chunks (possibly still processing)"
            }
        
        shared_chunks = []
        
        for fc in file_chunks:
            chunk = db.query(Chunk).filter(Chunk.hash == fc.chunk_hash).first()
            if chunk and chunk.ref_count > 1:
                # Find other files using this chunk
                other_files = db.query(File).join(
                    FileChunk, File.id == FileChunk.file_id
                ).filter(
                    FileChunk.chunk_hash == chunk.hash,
                    File.id != file_id
                ).distinct().all()
                
                if other_files:  # Only include if there are other files
                    shared_chunks.append({
                        "hash": chunk.hash[:16] + "...",
                        "full_hash": chunk.hash,
                        "size_bytes": chunk.size,
                        "size_kb": round(chunk.size / 1024, 2),
                        "ref_count": chunk.ref_count,
                        "other_files_count": len(other_files),
                        "other_files": [
                            {
                                "file_id": f.id,
                                "filename": f.filename,
                                "size_mb": round(f.size / (1024 * 1024), 2),
                                "created_at": f.created_at.isoformat() if f.created_at else None
                            }
                            for f in other_files[:10]  # Limit to 10 files per chunk
                        ]
                    })
        
        # Calculate summary statistics
        total_shared_size = sum(c["size_bytes"] for c in shared_chunks)
        
        return {
            "file_id": file_id,
            "filename": file.filename,
            "file_size_mb": round(file.size / (1024 * 1024), 2),
            "total_chunks": len(file_chunks),
            "total_shared_chunks": len(shared_chunks),
            "shared_percentage": round((len(shared_chunks) / len(file_chunks)) * 100, 2) if file_chunks else 0,
            "shared_space_mb": round(total_shared_size / (1024 * 1024), 2),
            "unique_files_affected": len(set(
                f["file_id"] for c in shared_chunks for f in c["other_files"]
            )),
            "shared_chunks": shared_chunks[:50]  # Limit to 50 chunks for performance
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting shared chunks: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))