# entropy_optimization.py - Smart chunking based on entropy
import math

def calculate_entropy(data):
    """Calculate Shannon entropy of chunk data"""
    if not data:
        return 0
    
    entropy = 0
    for x in range(256):
        p_x = data.count(x) / len(data)
        if p_x > 0:
            entropy += - p_x * math.log2(p_x)
    
    return entropy

def get_chunk_type(entropy):
    """Determine chunk type based on entropy"""
    if entropy < 3:
        return "highly_compressible"  # Text, zeros
    elif entropy < 6:
        return "mixed"  # Source code, structured data
    elif entropy < 7.5:
        return "already_compressed"  # Images, compressed files
    else:
        return "random"  # Encrypted, random data

def optimize_chunk_for_dedup(chunk_data):
    """
    Decide whether to split, merge, or transform chunk for better dedup
    """
    entropy = calculate_entropy(chunk_data)
    chunk_type = get_chunk_type(entropy)
    
    if chunk_type == "highly_compressible":
        # Text files - can use smaller chunks
        return "use_small_chunks"
    elif chunk_type == "already_compressed":
        # Already compressed - use larger chunks
        return "use_large_chunks"
    elif chunk_type == "mixed":
        # Balanced approach
        return "use_balanced_chunks"
    else:
        # Random data - dedup unlikely, use larger chunks
        return "use_large_chunks"