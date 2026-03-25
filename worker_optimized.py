# worker_optimized.py - Complete deduplication worker
import os
import hashlib
import logging
import xxhash
from db import SessionLocal, File, Chunk, FileChunk
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class OptimizedDeduplicator:
    def __init__(self):
        self.min_chunk = 256 * 1024
        self.max_chunk = 4 * 1024 * 1024
        self.avg_chunk = 1 * 1024 * 1024
        
    def get_chunk_path(self, hash_value):
        if len(hash_value) < 4:
            return os.path.join("chunks", hash_value)
        first_level = hash_value[:2]
        second_level = hash_value[2:4]
        chunk_dir = os.path.join("chunks", first_level, second_level)
        os.makedirs(chunk_dir, exist_ok=True)
        return os.path.join(chunk_dir, hash_value)
    
    def get_fast_hash(self, data):
        """Fast hash for quick matching"""
        return xxhash.xxh64(data).hexdigest()
    
    def get_medium_hash(self, data):
        """Medium hash for filtering"""
        return hashlib.blake2b(data, digest_size=16).hexdigest()
    
    def get_strong_hash(self, data):
        """Strong hash for final verification"""
        return hashlib.sha256(data).hexdigest()
    
    def chunk_with_cdc(self, data):
        """Content-defined chunking"""
        chunks = []
        chunk_start = 0
        pos = self.min_chunk
        window_size = 64
        
        # Rolling hash state
        hash_val = 0
        base = 257
        
        # Precompute powers
        power = 1
        for i in range(window_size):
            power = (power * base) & 0xFFFFFFFFFFFFFFFF
        
        # Initialize rolling hash
        for i in range(min(window_size, len(data))):
            hash_val = (hash_val * base + data[i]) & 0xFFFFFFFFFFFFFFFF
        
        while pos < len(data):
            # Update rolling hash
            if pos >= window_size:
                hash_val = (hash_val - (data[pos - window_size] * power)) & 0xFFFFFFFFFFFFFFFF
                hash_val = (hash_val * base + data[pos]) & 0xFFFFFFFFFFFFFFFF
            
            # Check boundary
            if (hash_val & 0xFFFFF) == 0 and (pos - chunk_start) >= self.min_chunk:
                chunks.append(data[chunk_start:pos])
                chunk_start = pos
                pos += self.min_chunk
            elif (pos - chunk_start) >= self.max_chunk:
                chunks.append(data[chunk_start:pos])
                chunk_start = pos
                pos += self.min_chunk
            else:
                pos += 1
        
        if chunk_start < len(data):
            chunks.append(data[chunk_start:])
        
        return chunks
    
    def process_file(self, path, filename, file_id):
        """Process file with advanced deduplication"""
        db = SessionLocal()
        
        try:
            # Read file
            with open(path, 'rb') as f:
                data = f.read()
            
            file_size = len(data)
            logger.info(f"Processing: {filename} ({file_size:,} bytes)")
            
            # Create file record
            db_file = File(
                id=file_id,
                filename=filename,
                size=file_size,
                created_at=datetime.utcnow()
            )
            db.add(db_file)
            db.commit()
            
            # Chunk with CDC
            chunks = self.chunk_with_cdc(data)
            
            new_chunks = 0
            existing_chunks = 0
            
            for i, chunk_data in enumerate(chunks):
                chunk_size = len(chunk_data)
                
                # Multi-level hashing
                fast_hash = self.get_fast_hash(chunk_data)
                medium_hash = self.get_medium_hash(chunk_data)
                strong_hash = self.get_strong_hash(chunk_data)
                
                # Try to find existing chunk
                # First check by strong hash (exact match)
                db_chunk = db.query(Chunk).filter(Chunk.hash == strong_hash).first()
                
                if not db_chunk:
                    # No exact match, try similarity matching (optional)
                    # Could implement similarity search here
                    
                    # Save new chunk
                    chunk_path = self.get_chunk_path(strong_hash)
                    with open(chunk_path, 'wb') as cf:
                        cf.write(chunk_data)
                    
                    db_chunk = Chunk(
                        hash=strong_hash,
                        fast_hash=fast_hash,
                        medium_hash=medium_hash,
                        path=chunk_path,
                        ref_count=1,
                        size=chunk_size,
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
                    chunk_hash=strong_hash,
                    order_index=i
                ))
                
                # Commit every 50 chunks
                if (i + 1) % 50 == 0:
                    db.commit()
            
            db.commit()
            
            dedup_rate = (existing_chunks / len(chunks) * 100) if chunks else 0
            
            logger.info(
                f"Complete: {filename}\n"
                f"  - Total chunks: {len(chunks)}\n"
                f"  - New chunks: {new_chunks}\n"
                f"  - Reused chunks: {existing_chunks}\n"
                f"  - Dedup rate: {dedup_rate:.1f}%\n"
                f"  - Avg chunk size: {file_size/len(chunks)/1024:.1f} KB"
            )
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error: {e}")
            raise
        finally:
            db.close()
            if os.path.exists(path):
                os.remove(path)

# Use in worker
def process_file(path, filename, file_id):
    deduplicator = OptimizedDeduplicator()
    deduplicator.process_file(path, filename, file_id)