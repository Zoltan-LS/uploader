# similarity_dedup.py - Find similar chunks for delta encoding
import difflib

def find_similar_chunks(db, chunk_data, threshold=0.8):
    """
    Find chunks that are similar but not identical
    Can store only the delta between them
    """
    # Get all chunks of similar size
    chunk_size = len(chunk_data)
    size_range = int(chunk_size * 0.2)  # 20% size variance
    
    similar_size_chunks = db.query(Chunk).filter(
        Chunk.size.between(chunk_size - size_range, chunk_size + size_range)
    ).limit(100).all()
    
    best_match = None
    best_ratio = 0
    
    for chunk in similar_size_chunks:
        # Read chunk data
        with open(chunk.path, 'rb') as f:
            existing_data = f.read()
        
        # Calculate similarity ratio
        ratio = difflib.SequenceMatcher(None, chunk_data, existing_data).ratio()
        
        if ratio > threshold and ratio > best_ratio:
            best_ratio = ratio
            best_match = chunk
    
    return best_match, best_ratio