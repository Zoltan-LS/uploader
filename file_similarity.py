# file_similarity.py - Analyze file similarity for better dedup
from collections import defaultdict
import numpy as np

def compute_file_signature(file_path, num_samples=100):
    """
    Compute file signature using sampling
    Useful for quickly identifying similar files
    """
    with open(file_path, 'rb') as f:
        data = f.read()
    
    file_size = len(data)
    
    if file_size < 1024 * 1024:  # < 1MB
        # For small files, use whole file
        return hashlib.sha256(data).hexdigest()
    
    # For large files, use sampling
    samples = []
    step = file_size // num_samples
    
    for i in range(0, file_size, step):
        sample = data[i:i+1024]
        samples.append(hashlib.md5(sample).hexdigest())
    
    # Create a combined signature
    combined = hashlib.sha256(''.join(samples).encode()).hexdigest()
    
    return combined

def group_similar_files(db):
    """
    Group similar files to improve deduplication
    """
    # Get all files
    files = db.query(File).all()
    
    # Compute signatures
    signatures = {}
    for file in files:
        # This would need file data - implement as needed
        signatures[file.id] = file.signature or "unknown"
    
    # Group by signature
    groups = defaultdict(list)
    for file_id, sig in signatures.items():
        groups[sig].append(file_id)
    
    return groups