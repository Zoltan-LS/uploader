# worker.py - Optimized for large files
import os
import hashlib
import logging
from pathlib import Path
from dotenv import load_dotenv
import sys

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CHUNK_DIR = os.getenv("CHUNK_DIR", "chunks")
if not os.path.isabs(CHUNK_DIR):
    CHUNK_DIR = os.path.abspath(CHUNK_DIR)

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "temp")
if not os.path.isabs(UPLOAD_DIR):
    UPLOAD_DIR = os.path.abspath(UPLOAD_DIR)

# Reduce chunk size for large files to use less memory
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 512 * 1024))  # 512KB instead of 1MB

# Create directories
os.makedirs(CHUNK_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

logger.info(f"CHUNK_DIR: {CHUNK_DIR}")
logger.info(f"CHUNK_SIZE: {CHUNK_SIZE / 1024:.0f}KB")

def get_chunk_path(hash_value):
    """Convert hash to nested directory structure"""
    if len(hash_value) < 4:
        chunk_path = os.path.join(CHUNK_DIR, hash_value)
    else:
        first_level = hash_value[:2]
        second_level = hash_value[2:4]
        chunk_path = os.path.join(CHUNK_DIR, first_level, second_level, hash_value)
    
    os.makedirs(os.path.dirname(chunk_path), exist_ok=True)
    return chunk_path

def sha256(data):
    return hashlib.sha256(data).hexdigest()

def process_file(path, filename, file_id):
    """Process uploaded file with memory-efficient streaming"""
    from db import SessionLocal, File, Chunk, FileChunk
    from datetime import datetime
    
    db = SessionLocal()
    
    try:
        file_size = os.path.getsize(path)
        logger.info(f"Processing: {filename} ({file_size:,} bytes)")
        
        # Create file record
        db_file = File(
            id=file_id,
            filename=filename,
            size=file_size,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(db_file)
        db.commit()
        
        chunks_processed = 0
        new_chunks = 0
        existing_chunks = 0
        
        # Process file in chunks without loading entire file into memory
        with open(path, "rb") as f:
            index = 0
            while True:
                # Read chunk
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break
                
                chunk_hash = sha256(chunk_data)
                chunk_path = get_chunk_path(chunk_hash)
                
                # Check for existing chunk
                db_chunk = db.query(Chunk).filter(Chunk.hash == chunk_hash).first()
                
                if not db_chunk:
                    # Save new chunk
                    with open(chunk_path, "wb") as cf:
                        cf.write(chunk_data)
                    
                    db_chunk = Chunk(
                        hash=chunk_hash,
                        path=chunk_path,
                        ref_count=1,
                        size=len(chunk_data),
                        created_at=datetime.utcnow()
                    )
                    db.add(db_chunk)
                    new_chunks += 1
                else:
                    db_chunk.ref_count += 1
                    existing_chunks += 1
                
                # Create mapping
                db.add(FileChunk(
                    file_id=file_id,
                    chunk_hash=chunk_hash,
                    order_index=index
                ))
                
                index += 1
                chunks_processed += 1
                
                # Commit more frequently for large files
                if chunks_processed % 50 == 0:
                    db.commit()
                    logger.info(f"Processed {chunks_processed} chunks ({chunks_processed * CHUNK_SIZE / 1024 / 1024:.1f} MB)")
                
                # Clear chunk_data to free memory
                chunk_data = None
        
        db.commit()
        
        dedup_rate = (existing_chunks / chunks_processed * 100) if chunks_processed else 0
        
        logger.info(
            f"✓ File processed: {filename}\n"
            f"  - Size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)\n"
            f"  - Chunks: {chunks_processed} ({new_chunks} new, {existing_chunks} existing)\n"
            f"  - Dedup rate: {dedup_rate:.1f}%"
        )
        
    except MemoryError:
        logger.error(f"Memory error processing {filename}. Consider reducing CHUNK_SIZE")
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        db.close()
        if os.path.exists(path):
            os.remove(path)