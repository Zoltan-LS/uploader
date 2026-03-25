# multi_level_dedup.py - Two-level deduplication
import hashlib
import xxhash  # pip install xxhash

def compute_signatures(chunk_data):
    """
    Compute multiple hash signatures for better matching
    Use fast hash for candidate matching, strong hash for verification
    """
    signatures = {
        # Fast hash for quick candidate matching
        "fast": xxhash.xxh64(chunk_data).hexdigest(),
        
        # Medium hash for filtering
        "medium": hashlib.blake2b(chunk_data, digest_size=16).hexdigest(),
        
        # Strong hash for final verification
        "strong": hashlib.sha256(chunk_data).hexdigest()
    }
    
    return signatures

def find_similar_chunks(db, fast_hash):
    """
    Find potential matches using fast hash first
    Reduces full SHA256 comparisons
    """
    # Use fast hash index to find candidates
    candidates = db.query(Chunk).filter(
        Chunk.fast_hash == fast_hash
    ).all()
    
    return candidates