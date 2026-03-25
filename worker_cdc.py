# worker_cdc.py - Advanced Content-Defined Chunking
import os
import hashlib
import logging
import struct
from db import SessionLocal, File, Chunk, FileChunk
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

# Configuration
MIN_CHUNK_SIZE = int(os.getenv("MIN_CHUNK_SIZE", 256 * 1024))  # 256KB
MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", 4 * 1024 * 1024))  # 4MB
AVG_CHUNK_SIZE = int(os.getenv("AVG_CHUNK_SIZE", 1024 * 1024))  # 1MB target

class RabinKarpChunker:
    """
    Content-defined chunking using Rabin-Karp rolling hash
    This is the same algorithm used by LBFS, rsync, and many dedup systems
    """
    
    def __init__(self, min_size=MIN_CHUNK_SIZE, max_size=MAX_CHUNK_SIZE, avg_size=AVG_CHUNK_SIZE):
        self.min_size = min_size
        self.max_size = max_size
        self.avg_size = avg_size
        
        # Rabin-Karp parameters
        self.base = 257  # Prime base
        self.mod = 2**64  # Use 64-bit overflow (natural)
        self.window_size = 48  # Sliding window size
        
        # Precompute powers
        self.powers = [1]
        for i in range(1, self.window_size + 1):
            self.powers.append((self.powers[-1] * self.base) & 0xFFFFFFFFFFFFFFFF)
    
    def _rolling_hash(self, data, start, end):
        """Compute rolling hash for window"""
        h = 0
        for i in range(start, end):
            h = (h * self.base + data[i]) & 0xFFFFFFFFFFFFFFFF
        return h
    
    def _is_boundary(self, hash_value):
        """Check if this hash marks a chunk boundary"""
        # Use mask to get 1/N probability where N = avg_size / min_size
        mask = (1 << 20) - 1  # 1MB average = 2^20
        return (hash_value & mask) == 0
    
    def chunk_file(self, file_path):
        """Split file into variable-sized chunks based on content"""
        chunks = []
        
        with open(file_path, 'rb') as f:
            data = f.read()
        
        if len(data) == 0:
            return chunks
        
        chunk_start = 0
        pos = self.min_size
        
        # Initialize rolling hash for first window
        current_hash = 0
        for i in range(min(self.window_size, len(data))):
            current_hash = (current_hash * self.base + data[i]) & 0xFFFFFFFFFFFFFFFF
        
        while pos < len(data):
            # Update rolling hash
            if pos >= self.window_size:
                # Remove oldest byte
                oldest = data[pos - self.window_size]
                current_hash = (current_hash - (oldest * self.powers[self.window_size - 1])) & 0xFFFFFFFFFFFFFFFF
                # Add new byte
                current_hash = (current_hash * self.base + data[pos]) & 0xFFFFFFFFFFFFFFFF
            
            # Check if this is a chunk boundary
            if self._is_boundary(current_hash) and (pos - chunk_start) >= self.min_size:
                chunk = data[chunk_start:pos]
                chunks.append(chunk)
                chunk_start = pos
                pos += self.min_size
            elif (pos - chunk_start) >= self.max_size:
                # Force chunk at max size
                chunk = data[chunk_start:pos]
                chunks.append(chunk)
                chunk_start = pos
                pos += self.min_size
            else:
                pos += 1
        
        # Add final chunk
        if chunk_start < len(data):
            chunks.append(data[chunk_start:])
        
        # Merge very small chunks
        chunks = self._merge_small_chunks(chunks)
        
        return chunks
    
    def _merge_small_chunks(self, chunks):
        """Merge chunks that are too small"""
        if not chunks:
            return chunks
        
        merged = []
        current = chunks[0]
        
        for i in range(1, len(chunks)):
            if len(current) < MIN_CHUNK_SIZE // 2:
                current += chunks[i]
            else:
                merged.append(current)
                current = chunks[i]
        
        merged.append(current)
        return merged

def get_chunk_path(hash_value):
    """Get sharded path for chunk"""
    if len(hash_value) < 4:
        return os.path.join("chunks", hash_value)
    
    first_level = hash_value[:2]
    second_level = hash_value[2:4]
    chunk_dir = os.path.join("chunks", first_level, second_level)
    os.makedirs(chunk_dir, exist_ok=True)
    
    return os.path.join(chunk_dir, hash_value)

def sha256(data):
    return hashlib.sha256(data).hexdigest()

def process_file_cdc(path, filename, file_id):
    """Process file with content-defined chunking for better deduplication"""
    db = SessionLocal()
    
    try:
        file_size = os.path.getsize(path)
        logger.info(f"Processing with CDC: {filename} ({file_size:,} bytes)")
        
        # Create file record
        db_file = File(
            id=file_id,
            filename=filename,
            size=file_size,
            created_at=datetime.utcnow()
        )
        db.add(db_file)
        db.commit()
        
        # Create chunker
        chunker = RabinKarpChunker()
        chunks = chunker.chunk_file(path)
        
        total_size = 0
        chunks_processed = 0
        new_chunks = 0
        existing_chunks = 0
        
        for i, chunk_data in enumerate(chunks):
            chunk_size = len(chunk_data)
            total_size += chunk_size
            
            # Calculate hash
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
                    size=chunk_size,
                    created_at=datetime.utcnow()
                )
                db.add(db_chunk)
                new_chunks += 1
                logger.debug(f"New CDC chunk: {chunk_hash[:16]}... ({chunk_size:,} bytes)")
            else:
                db_chunk.ref_count += 1
                existing_chunks += 1
                logger.debug(f"Existing CDC chunk reused: {chunk_hash[:16]}...")
            
            # Create mapping
            db.add(FileChunk(
                file_id=file_id,
                chunk_hash=chunk_hash,
                order_index=i
            ))
            
            chunks_processed += 1
            
            # Commit periodically
            if chunks_processed % 50 == 0:
                db.commit()
        
        db.commit()
        
        # Calculate deduplication ratio
        total_unique = new_chunks + existing_chunks
        dedup_ratio = (existing_chunks / total_unique * 100) if total_unique > 0 else 0
        
        logger.info(
            f"CDC Processing complete: {filename}\n"
            f"  - Size: {total_size:,} bytes ({total_size/(1024*1024):.2f} MB)\n"
            f"  - Chunks: {chunks_processed} ({new_chunks} new, {existing_chunks} existing)\n"
            f"  - Avg chunk size: {total_size/chunks_processed/1024:.1f} KB\n"
            f"  - Deduplication rate: {dedup_ratio:.1f}%"
        )
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing file: {e}")
        raise
    finally:
        db.close()
        if os.path.exists(path):
            os.remove(path)