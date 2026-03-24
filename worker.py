# worker.py
import os
import hashlib
import logging
from sqlalchemy.orm import Session
from db import SessionLocal, File, Chunk, FileChunk
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1024 * 1024))  # 1MB
CHUNK_DIR = os.getenv("CHUNK_DIR", "chunks")
os.makedirs(CHUNK_DIR, exist_ok=True)

def get_chunk_path(hash_value):
    """Convert hash to nested directory structure: chunks/ab/cd/abcdef123456..."""
    if len(hash_value) < 4:
        return os.path.join(CHUNK_DIR, hash_value)
    
    # Use first 2 chars as first level, next 2 chars as second level
    first_level = hash_value[:2]
    second_level = hash_value[2:4]
    
    # Create directories if they don't exist
    chunk_dir = os.path.join(CHUNK_DIR, first_level, second_level)
    os.makedirs(chunk_dir, exist_ok=True)
    
    return os.path.join(chunk_dir, hash_value)

def sha256(data):
    """Calculate SHA256 hash of data"""
    return hashlib.sha256(data).hexdigest()

def process_file(path, filename, file_id):
    """
    Process uploaded file:
    - Split into chunks
    - Deduplicate chunks
    - Store file structure in database
    """
    db = SessionLocal()
    
    try:
        logger.info(f"Processing file: {filename} (ID: {file_id})")
        
        # Create file record
        db_file = File(
            id=file_id,
            filename=filename,
            size=0,  # Will update after processing
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(db_file)
        db.commit()
        
        total_size = 0
        chunk_index = 0
        chunks_processed = 0
        new_chunks = 0
        existing_chunks = 0
        
        # Process file in chunks
        with open(path, "rb") as f:
            while True:
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break
                
                chunk_size = len(chunk_data)
                total_size += chunk_size
                
                # Calculate hash
                chunk_hash = sha256(chunk_data)
                
                # Get sharded path
                chunk_path = get_chunk_path(chunk_hash)
                
                # Check if chunk already exists (using PostgreSQL transaction)
                db_chunk = db.query(Chunk).filter(Chunk.hash == chunk_hash).first()
                
                if not db_chunk:
                    # Save new chunk to disk
                    with open(chunk_path, "wb") as cf:
                        cf.write(chunk_data)
                    
                    # Create chunk record
                    db_chunk = Chunk(
                        hash=chunk_hash,
                        path=chunk_path,
                        ref_count=1,
                        size=chunk_size,
                        created_at=datetime.utcnow()
                    )
                    db.add(db_chunk)
                    new_chunks += 1
                    logger.debug(f"New chunk created: {chunk_hash[:16]}... ({chunk_size} bytes)")
                else:
                    # Increment reference count for existing chunk
                    db_chunk.ref_count += 1
                    existing_chunks += 1
                    logger.debug(f"Existing chunk reused: {chunk_hash[:16]}... (ref_count: {db_chunk.ref_count})")
                
                # Create file-chunk mapping
                file_chunk = FileChunk(
                    file_id=file_id,
                    chunk_hash=chunk_hash,
                    order_index=chunk_index
                )
                db.add(file_chunk)
                
                chunk_index += 1
                chunks_processed += 1
                
                # Commit every 100 chunks to avoid huge transactions
                if chunks_processed % 100 == 0:
                    db.commit()
                    logger.info(f"Processed {chunks_processed} chunks for {filename}")
        
        # Update file with total size
        db_file.size = total_size
        db.commit()
        
        logger.info(
            f"File processed successfully: {filename}\n"
            f"  - File ID: {file_id}\n"
            f"  - Size: {total_size} bytes ({total_size / (1024*1024):.2f} MB)\n"
            f"  - Chunks: {chunks_processed} total ({new_chunks} new, {existing_chunks} existing)\n"
            f"  - Deduplication saved: {(existing_chunks * CHUNK_SIZE) / (1024*1024):.2f} MB"
        )
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing file {filename}: {e}")
        logger.error(traceback.format_exc())
        raise
    
    finally:
        db.close()
        
        # Clean up temporary file
        if os.path.exists(path):
            try:
                os.remove(path)
                logger.debug(f"Removed temporary file: {path}")
            except Exception as e:
                logger.error(f"Error removing temporary file {path}: {e}")

# Optional: Add a cleanup function for chunks with ref_count=0
def cleanup_unused_chunks(db: Session = None):
    """Remove chunks with ref_count = 0 (should be run periodically)"""
    if db is None:
        db = SessionLocal()
    
    try:
        # Find chunks with no references
        unused_chunks = db.query(Chunk).filter(Chunk.ref_count == 0).all()
        
        if not unused_chunks:
            logger.info("No unused chunks to clean up")
            return 0
        
        logger.info(f"Found {len(unused_chunks)} unused chunks to clean up")
        
        cleaned = 0
        for chunk in unused_chunks:
            try:
                # Delete physical file
                if os.path.exists(chunk.path):
                    os.remove(chunk.path)
                    logger.debug(f"Deleted chunk file: {chunk.path}")
                
                # Delete database record
                db.delete(chunk)
                
                # Try to remove empty directories
                chunk_dir = os.path.dirname(chunk.path)
                try:
                    os.rmdir(chunk_dir)
                    logger.debug(f"Removed empty directory: {chunk_dir}")
                except OSError:
                    # Directory not empty, ignore
                    pass
                
                cleaned += 1
                
                # Commit every 100 deletions
                if cleaned % 100 == 0:
                    db.commit()
                    logger.info(f"Cleaned up {cleaned}/{len(unused_chunks)} chunks")
                    
            except Exception as e:
                logger.error(f"Error cleaning up chunk {chunk.hash}: {e}")
                continue
        
        db.commit()
        logger.info(f"Cleanup complete: removed {cleaned} unused chunks")
        return cleaned
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error during cleanup: {e}")
        raise
    finally:
        if db:
            db.close()

# Optional: Add a health check for worker
def worker_health_check():
    """Check if worker can connect to services"""
    issues = []
    
    # Check database
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        logger.info("Database connection: OK")
    except Exception as e:
        issues.append(f"Database: {e}")
        logger.error(f"Database connection failed: {e}")
    
    # Check chunk directory
    try:
        os.makedirs(CHUNK_DIR, exist_ok=True)
        logger.info(f"Chunk directory: {CHUNK_DIR} (OK)")
    except Exception as e:
        issues.append(f"Chunk directory: {e}")
    
    return issues

if __name__ == "__main__":
    # Run health check when worker starts
    issues = worker_health_check()
    if issues:
        logger.warning(f"Health check issues: {issues}")
    else:
        logger.info("Worker health check passed")