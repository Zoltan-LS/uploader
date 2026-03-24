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

@app.post("/upload", status_code=202)
async def upload(file: UploadFile = FastAPIFile(...)):
    """Upload a file - processed asynchronously with deduplication"""
    try:
        file_id = str(uuid.uuid4())
        temp_path = os.path.join(UPLOAD_DIR, file_id)
        
        # Save uploaded file temporarily
        with open(temp_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        
        # Enqueue background processing job
        job = q.enqueue(
            process_file, 
            temp_path, 
            file.filename, 
            file_id,
            job_timeout=3600,  # 1 hour timeout for large files
            result_ttl=5000
        )
        
        logger.info(f"File uploaded: {file.filename} (ID: {file_id}, Job: {job.id})")
        
        return {
            "file_id": file_id, 
            "job_id": job.id,
            "filename": file.filename,
            "status": "processing"
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

